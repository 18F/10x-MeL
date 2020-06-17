from __future__ import annotations

from pathlib import Path
from typing import Dict, List, Set, Tuple, Optional, Callable, DefaultDict
from collections import Counter, defaultdict, OrderedDict
from functools import lru_cache
from time import time

import pandas as pd
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.feature_extraction import text

from analyzer.data_view.handler import DataViewHandler
from analyzer.dataset.handler import DatasetHandler
from analyzer.users.users_lib import UserHandler

from analyzer.data_view.data_view_lib import Label, LabelSequence, DataViewId
from analyzer.data_view.rich_data_view import RichDataView
from analyzer.dataset.dataset_lib import Dataset, DatasetId
from analyzer.constraint_lib import (
    TransformResourceHandler, Transform, FilterTransform, EnrichmentTransform,
)

from analyzer.text_processing import WordHistoryProcessor, WordHistoryResult

import logging

log = logging.getLogger(__name__)


DataFrame = pd.DataFrame
TransformLookup = DefaultDict[DatasetId, Dict[DataViewId, Set[Transform]]]


TAB = "\t"
COMMA = ","

stop_words = text.ENGLISH_STOP_WORDS.union(["book"])


class DataFrameCache:
    def __init__(self):
        self._cache: Dict[DataViewId, DataFrame] = {}


class Analyzer:
    DEFAULT_LIMIT = 250

    def __init__(
        self,
        data_dir: Path,
        data_view_handler: DataViewHandler,
        dataset_handler: DatasetHandler,
        user_handler: UserHandler,
        transform_resource_handler: TransformResourceHandler,
    ):
        self.data_dir = Path(data_dir)
        assert self.data_dir.exists()

        self._data_view_handler = data_view_handler
        self._dataset_handler = dataset_handler
        self._user_handler = user_handler
        self.transform_resource_handler = transform_resource_handler

        self._active_dataframe_by_data_view: Dict[RichDataView, DataFrame] = {}
        self._df_cache_by_dataset = defaultdict(OrderedDict)
        self._data_view_transforms_by_dataset_id: TransformLookup = defaultdict(dict)

    @lru_cache(maxsize=128)
    def _get_df(self, data_view: RichDataView) -> DataFrame:
        data_view_id = data_view.id
        dataset_id = data_view.dataset_id

        df_cache = self._df_cache_by_dataset[dataset_id]
        transforms_by_data_view_id = self._data_view_transforms_by_dataset_id[dataset_id]

        if data_view_id not in df_cache:
            log.info("data_view_id %s not in cache", data_view_id)
            # find best starting point
            cached_data_view_id, remaining_transforms = self.get_id_of_best_base_df(
                data_view.transforms, transforms_by_data_view_id,
            )

            log.info("best base_df: %s", cached_data_view_id)

            if cached_data_view_id:
                df = self._get_df(self.rich_data_view(cached_data_view_id))
                log.info(f"generating DataView {data_view_id} from {cached_data_view_id}")
                transforms = remaining_transforms
            else:
                log.info(f"generating DataView {data_view_id} from base")
                df = self.active_dataframe(data_view)
                transforms = data_view.transforms

            for transform in transforms:
                if isinstance(transform, FilterTransform):
                    df = transform.filter(df, self.transform_resource_handler.instance(data_view))

                elif isinstance(transform, EnrichmentTransform):
                    result = transform.enrich(df, self.transform_resource_handler.instance(data_view))

                    """
                    column_label, is_sort_ascending = result.sort
                    if column_label:
                        df = df.sort_values(by=[column_label], ascending=is_sort_ascending)
                    """

            df_cache[data_view_id] = df
            transforms_by_data_view_id[data_view_id] = data_view.transforms
        else:
            log.info(f"loading cached DataView {data_view_id}")

        return df_cache[data_view_id]

    @staticmethod
    def get_id_of_best_base_df(
        target_transforms: List[Transform],
        cached_transforms: Dict[DataViewId, Set[Transform]],
    ) -> Tuple[Optional[DataViewId], Set[Transform]]:
        target_transform_set: Set[Transform] = set(target_transforms)

        difference_by_id: Dict[DataViewId, Set[Transform]] = {}
        counts_and_ids: List[Tuple[int, DataViewId]] = []

        cached_data_view_ids = list(cached_transforms.keys())

        for cached_data_view_id in cached_data_view_ids:
            transforms = cached_transforms[cached_data_view_id]
            cached_transform_set = set(transforms)

            if not target_transform_set.issuperset(cached_transform_set):
                # the cached DataView's transforms are not a subset of the target's transforms
                continue

            transform_difference = target_transform_set - cached_transform_set
            difference_by_id[cached_data_view_id] = transform_difference
            counts_and_ids.append((len(transform_difference), cached_data_view_id))

        if not counts_and_ids:
            # no possible bases were found in the cache
            return None, target_transform_set

        # seek out the DataView with the fewest differences
        # TODO: another approach here would be to seek out the DataView with the fewest rows
        best_cached_id: DataViewId = sorted(counts_and_ids)[0][1]
        log.info("best_cached_id: %s", best_cached_id)
        return best_cached_id, difference_by_id[best_cached_id]

    def rich_data_view(self, data_view_id: DataViewId) -> RichDataView:
        data_view = self._data_view_handler.by_id(data_view_id)
        log.info("DataView %s from %s", data_view, data_view_id)
        return RichDataView(
            data_view=data_view,
            dataset=self._dataset_handler.by_id(data_view.dataset_id),
            user=self._user_handler.by_id(data_view.user_id),
        )

    def _load_data(self, data_view: RichDataView, path: Path) -> DataFrame:
        dataset_reader = self._get_dataset_reader(path)
        df = dataset_reader(path)
        df = df.fillna("")
        return df

    def _get_dataset_reader(self, path: Path) -> Callable[[Path], DataFrame]:
        csv_suffixes = [".csv"]
        tsv_suffixes = [".tsv"]
        excel_suffixes = [".xls", ".xlsx"]

        suffix = path.suffix

        if suffix in csv_suffixes:
            return self._load_csv_data
        elif suffix in tsv_suffixes:
            return self._load_tsv_data
        elif suffix in excel_suffixes:
            return self._load_spreadsheet_data
        else:
            log.warning('Unrecognized suffix, handling as csv: "%s"', suffix)
            return self._load_csv_data

    @classmethod
    def _load_csv_data(cls, path: Path, sep: Optional[str] = COMMA) -> DataFrame:
        log.info(f"reading data from {path}")
        return pd.read_csv(path, sep=sep, na_values=None)

    @classmethod
    def _load_tsv_data(cls, path: Path) -> DataFrame:
        return cls._load_csv_data(path, sep=TAB)

    @classmethod
    def _load_spreadsheet_data(cls, path: Path) -> DataFrame:
        return pd.read_excel(path, na_values=None)

    def get_dataset_path(self, data_view: RichDataView) -> Path:
        return self.data_dir / data_view.dataset.filename

    def active_dataframe(self, data_view: RichDataView) -> DataFrame:
        if self._active_dataframe_by_data_view.get(data_view) is None:
            try:
                path = self.get_dataset_path(data_view)
                df = self._load_data(path=path, data_view=data_view)
                self._active_dataframe_by_data_view[data_view] = df
            except Exception as exc:
                log.error(exc)
        return self._active_dataframe_by_data_view.get(data_view)

    def raw_data_for_data_view(
        self,
        data_view: RichDataView,
        sort_label: Optional[str] = None,
        sort_asc: Optional[bool] = None,
        limit: Optional[int] = None,
    ) -> Dict:
        limit = limit or self.DEFAULT_LIMIT

        start_get_df = time()
        df = self._get_df(data_view)

        elapsed = time() - start_get_df
        log.info(f"get_df took {elapsed:.2f} sec")

        if df is None:
            log.info("df is None")
            return None

        df.columns = df.columns.str.strip()

        if sort_label:
            df.sort_values(
                by=sort_label,
                inplace=True,
                ascending=sort_asc,
            )

        return df[:limit].T.to_dict()

    def get_dataset_labels(self, dataset: Dataset) -> LabelSequence:
        path = self.data_dir / dataset.filename

        dataset_reader = self._get_dataset_reader(path)
        return LabelSequence([Label(name=name) for name in dataset_reader(path).keys()])

    def unique_counts_by_column(self, column: str, data_view: RichDataView) -> Dict[str, int]:
        df = self._get_df(data_view)

        if df is None:
            log.info("df is None")
            return Counter()

        return Counter(df[column].astype(str))

    def word_counts_over_time(
        self,
        date_time_column_name: str,
        text_column_name: str,
        data_view: RichDataView,
    ) -> WordHistoryResult:
        df = self._get_df(data_view)

        if df is None:
            log.info("df is None")
            return WordHistoryResult(counts={}, totals={})

        return WordHistoryProcessor(
            text_column_name=text_column_name,
            date_time_column_name=date_time_column_name,
            df=df,
        ).process()

    def tf_idf_over_values(
        self,
        text_column_name: str,
        category_column_name: str,
        data_view: RichDataView,
        count: int = 20,
    ) -> Dict[str, Dict[str, float]]:
        tex_col = text_column_name
        cat_col = category_column_name

        df = self._get_df(data_view)

        categories = df[cat_col].unique()

        vectorizer = CountVectorizer(stop_words=stop_words)
        result = vectorizer.fit_transform(
            [' '.join(df[df[cat_col] == c][tex_col].tolist()) for c in categories]
        )

        words = vectorizer.get_feature_names()
        words_by_categories = pd.DataFrame(
            result.todense(), index=categories, columns=words,
        )
        categories_by_words = words_by_categories.T

        # extract top n by_category
        return {c: categories_by_words[c].nlargest(count).to_dict() for c in categories}
