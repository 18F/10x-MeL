from __future__ import annotations

from typing import List

import logging

from analyzer.data_view.data_view_lib import DataView
from analyzer.dataset.dataset_lib import Dataset
from analyzer.users.users_lib import User


log = logging.getLogger(__name__)


class RichDataView(DataView):
    def __init__(self, data_view: DataView, dataset: Dataset, user: User):
        super().__init__(
            data_view_id=data_view.id,
            parent_data_view_id=data_view.parent_id,
            dataset_id=dataset.id,
            user_id=user.id,
            labels=data_view.labels,
            transforms=data_view.transforms,
        )
        self.data_view = data_view
        self.dataset = dataset
        self.user = user

    @property
    def label_names(self) -> List[str]:
        return [label.name for label in self.labels]
