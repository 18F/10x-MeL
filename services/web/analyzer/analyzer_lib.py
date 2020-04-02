from __future__ import annotations

from pathlib import Path
from typing import Dict, Optional, Callable
import logging

import pandas as pd

from analyzer.data_view.data_view_lib import Label, LabelSet
from analyzer.data_view.rich_data_view import RichDataView
from analyzer.dataset.dataset_lib import Dataset
from analyzer.column_processor.column_processor import ColumnHandler


log = logging.getLogger(__name__)


DataFrame = pd.DataFrame

TAB = "\t"
COMMA = ","


class Analyzer:
    def __init__(self, data_dir: Path):
        self.data_dir = data_dir
        self._column_handler = ColumnHandler()
        self._active_dataframe_by_data_view: Dict[RichDataView, DataFrame] = {}

    def _load_data(self, data_view: RichDataView, path: Path) -> DataFrame:
        dataset_reader = self._get_dataset_reader(path)

        dataframe = dataset_reader(path)
        dataframe = dataframe.fillna("")

        original_labels = self.get_dataset_labels(data_view.dataset)
        original_label_names = set([label.name for label in original_labels])

        derived_labels = set(data_view.label_names) - original_label_names

        for label in derived_labels:
            column = self._column_handler.get_column_by_label(label)
            column.apply(label, dataframe, sep=". ")

        return dataframe

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
            log.warning('Unrecognized suffix, handling as csv: "%s"', path)
            return self._load_csv_data

    @classmethod
    def _load_csv_data(cls, path: Path, sep: Optional[str] = COMMA) -> DataFrame:
        return pd.read_csv(path, sep=sep, na_values=None)

    @classmethod
    def _load_tsv_data(cls, path: Path) -> DataFrame:
        return cls._load_csv_data(path, sep=TAB)

    @classmethod
    def _load_spreadsheet_data(cls, path: Path) -> DataFrame:
        return pd.read_excel(path, na_values=None)

    def get_dataset_path(self, data_view: RichDataView) -> Path:
        return Path(self.data_dir, data_view.dataset.filename)

    def active_dataframe(self, data_view: RichDataView) -> DataFrame:
        if self._active_dataframe_by_data_view.get(data_view) is None:
            try:
                path = self.get_dataset_path(data_view)

                dataframe = self._load_data(path=path, data_view=data_view)
                self._active_dataframe_by_data_view[data_view] = dataframe
            except Exception as exc:
                log.error(exc)
        return self._active_dataframe_by_data_view.get(data_view)

    def get_entries(
        self, data_view: RichDataView, limit: Optional[int] = None
    ) -> Optional[DataFrame]:

        limit = limit or 250

        dataframe = self.active_dataframe(data_view)
        if dataframe is None:
            return None

        return dataframe[:limit].T.to_dict()

    def get_dataset_labels(self, dataset: Dataset) -> LabelSet:
        path = self.data_dir / dataset.filename

        dataset_reader = self._get_dataset_reader(path)
        return LabelSet([Label(name=name) for name in dataset_reader(path).keys()])
