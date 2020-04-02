from __future__ import annotations

from pathlib import Path
from typing import List, Tuple, Optional, Union

import logging

from analyzer.analyzer_lib import Analyzer, DataFrame
from analyzer.dataset.dataset_lib import Dataset, DatasetId
from analyzer.dataset.handler import DatasetHandler
from analyzer.data_view.data_view_lib import DataView, DataViewId, LabelSet, LabelType
from analyzer.data_view.handler import DataViewHandler, DataViewHistoryHandler
from analyzer.data_view.rich_data_view import RichDataView
from analyzer.users.users_lib import User, UserHandler, UserId


log = logging.getLogger(__name__)


class InvalidLabelTypeException(Exception):
    pass


class Session:
    DEFAULT_LIMIT = 10

    def __init__(
        self,
        config_dir: Path,
        data_dir: Path,
        users_filename: str,
        datasets_filename: str,
        data_views_filename: str,
        data_view_history_filename: str,
    ):
        log.info("Creating new session")

        self.config_dir = config_dir
        self.data_dir = data_dir

        users_path = config_dir / users_filename
        datasets_path = config_dir / datasets_filename
        data_views_path = config_dir / data_views_filename
        data_view_history_path = config_dir / data_view_history_filename

        self.user_handler = UserHandler(users_path)
        self.dataset_handler = DatasetHandler(datasets_path)
        self.data_view_handler = DataViewHandler(data_views_path)
        self.data_view_history_handler = DataViewHistoryHandler(data_view_history_path)

        self._analyzer = Analyzer(data_dir=self.data_dir)

        user = self.user_handler.default_user
        dataset_id = self.user_handler.get_last_dataset_id(user)

        if dataset_id:
            data_view_id = self.data_view_history_handler.get(user.id, dataset_id)
        else:
            data_view_id = None

        self.active_user_id: UserId = user.id
        self.active_dataset_id: DatasetId = dataset_id
        self.active_data_view_id: DataViewId = data_view_id

    @property
    def active_user(self) -> User:
        """Obtain the active User"""
        user_id = self.active_user_id
        if not user_id:
            user = self.user_handler.default_user
        else:
            user = self.user_handler.by_id(user_id)
            if user is None:
                log.error("User %s could not be found, loading default User", user_id)
                user = self.user_handler.default_user
        return user

    @property
    def active_dataset(self) -> Dataset:
        """Obtain the active Dataset"""
        return self.dataset_handler.by_id(self.active_dataset_id)

    @active_dataset.setter
    def active_dataset(self, filename: str):
        """Set the active dataset to the specified filename"""
        if not filename:
            log.error("Attempting to set dataset to empty filename: %s", filename)
            return

        active_dataset = self.dataset_handler.by_id(self.active_dataset_id)

        if active_dataset and filename == active_dataset.filename:
            # this dataset is already loaded, so do nothing
            return

        if self.dataset_handler.has_filename(filename):
            log.info("Changing dataset file to %s", filename)
            dataset = self.dataset_handler.by_filename(filename)
        else:
            log.info("Creating new dataset: %s", filename)
            dataset = self.dataset_handler.create(filename)

        self.user_handler.set_last_dataset(self.active_user, dataset)

        self.active_dataset_id = dataset.id
        self.active_data_view_id = None

    @property
    def active_data_view(self) -> Optional[RichDataView]:
        """Obtain the active DataView for the active Dataset"""
        if not self.active_dataset:
            log.info("DataView could not be loaded: no active Dataset")
            return None

        user = self.active_user
        dataset = self.active_dataset
        dataset_id = dataset.id

        # is a DataView active?
        if not self.active_data_view_id:
            # if not, does this user/dataset pair have a DataView in the history?
            if self.data_view_history_handler.has(user.id, dataset.id):
                # if so, use it
                data_view_id = self.data_view_history_handler.get(user.id, dataset.id)
                data_view = self.data_view_handler.by_id(data_view_id)
                log.info("Loaded DataView %s for %s / %s", data_view_id, user, dataset)

            else:
                # if not, search all DataViews to a match exists at all
                data_view = self.data_view_handler.find_first(user.id, dataset_id)

                # if a DataView wasn't found, create one for this user/dataset pair
                if not data_view:
                    # if not, create a DataView for this user/dataset pair
                    labels = self._analyzer.get_dataset_labels(dataset)
                    data_view = self.data_view_handler.create(user, dataset, labels)

                    self.data_view_history_handler.set(user.id, dataset.id, data_view.id)
                    self.data_view_history_handler.save()
                    log.info("Created DataView %s for %s / %s", data_view.id, user, dataset)

            self.active_data_view_id = data_view.id
        else:
            data_view = self.data_view_handler.by_id(self.active_data_view_id)

        log.info(
            "Active ID: %s, DataView: %s Dataset: %s",
            self.active_data_view_id,
            data_view,
            self.active_dataset,
        )

        return RichDataView(
            data_view=data_view,
            dataset=self.active_dataset,
            user=self.active_user,
        )

    def data_views_for_active_user(self, active_dataset: bool = False) -> List[DataView]:
        user_id = self.active_user_id
        if active_dataset:
            dataset_id = self.active_dataset_id
            return self.data_view_handler.find(user_id=user_id, dataset_id=dataset_id)
        else:
            return self.data_view_handler.find(user_id=user_id)

    def refresh_data_views(self):
        self.data_view_handler.load()

    @property
    def dataset_path(self) -> Path:
        """Obtain the path to the active dataset"""
        return Path(self.data_dir, self.active_dataset.filename)

    @property
    def active_labels(self) -> LabelSet:
        labels = self.active_data_view.labels
        if not labels:
            labels = self.get_labels_from_active_dataset(LabelType.ORIGINAL)
        return labels

    def get_labels_from_active_dataset(self, label_type: LabelType) -> LabelSet:
        try:
            label_type = LabelType(label_type)
        except ValueError:
            raise InvalidLabelTypeException(label_type)

        if not self.active_dataset:
            log.error("No active dataset")
            return LabelSet()

        if label_type == LabelType.ACTIVE:
            return self.active_data_view.labels

        elif label_type == LabelType.ORIGINAL:
            return self._analyzer.get_dataset_labels(self.active_dataset)

        elif label_type == LabelType.DERIVED:
            raise NotImplementedError("derived labels not yet implemented")

        elif label_type == LabelType.ALL:
            raise NotImplementedError("derived labels not yet implemented")

    def get_data_from_active_dataset(
        self, limit: Optional[int] = None
    ) -> Tuple[LabelSet, Union[DataFrame, List]]:

        if not self.active_dataset:
            log.warning("No active dataset")
            return LabelSet(), []

        log.info("active dataset: %s, path: %s", self.active_dataset, self.dataset_path)

        entries = self._analyzer.get_entries(self.active_data_view, limit=limit)
        labels = self.active_labels

        return labels, entries or []
