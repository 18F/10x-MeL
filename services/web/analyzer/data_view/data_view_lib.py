from __future__ import annotations

from typing import List, Dict, Optional, Union
from enum import Enum
import logging

from analyzer.utils import Serializable
from analyzer.data_view import DataViewId
from analyzer.dataset.dataset_lib import DatasetId
from analyzer.users.users_lib import UserId


log = logging.getLogger(__name__)


class LabelType(Enum):
    ALL = "all"
    DERIVED = "derived"
    ORIGINAL = "original"
    ACTIVE = "active"


class Label(Serializable):
    KEY_NAME = "n"
    KEY_WIDTH = "w"
    KEY_FONT_SIZE = "s"

    DEFAULT_WIDTH = 150
    DEFAULT_FONT_SIZE = 18

    def __init__(
        self,
        name: str,
        width: Optional[int] = None,
        font_size: Optional[int] = None,
    ):
        self._name = name
        self._width = width
        self._font_size = font_size

    @property
    def name(self) -> str:
        return self._name

    @property
    def width(self) -> int:
        return self._width or self.DEFAULT_WIDTH

    @property
    def font_size(self) -> int:
        return self._font_size or self.DEFAULT_FONT_SIZE

    def serialize(self) -> Dict[str, Union[int, str]]:
        d = {self.KEY_NAME: str(self.name)}
        if self._width:
            d[self.KEY_WIDTH] = int(self._width)
        if self._font_size:
            d[self.KEY_FONT_SIZE] = int(self._font_size)

        return d

    @classmethod
    def deserialize(cls, d: Dict[str, Union[str, int]]) -> Label:
        label_dict = {"name": d[Label.KEY_NAME]}
        if Label.KEY_WIDTH in d:
            label_dict["width"] = int(d[Label.KEY_WIDTH])
        if Label.KEY_FONT_SIZE in d:
            label_dict["font_size"] = int(d[Label.KEY_FONT_SIZE])

        return Label(**label_dict)

    def __str__(self) -> str:
        props = []
        if self._width:
            props.append(self._width)
        if self._font_size:
            props.append(self._font_size)
        return '<Label "{name}"{props}>'.format(
            name=self._name, props=" ".join(str(p) for p in props)
        )


class LabelSet(Serializable, list):
    def __init__(self, labels: Optional[List[Label]] = None):
        super().__init__(labels or [])

    def serialize(self) -> List[List[str]]:
        return [label.serialize() for label in self]

    @classmethod
    def deserialize(cls, lst: List[Dict[str, Union[str, int]]]) -> LabelSet:
        return LabelSet([Label.deserialize(elem) for elem in lst])

    def __str__(self) -> str:
        return ", ".join(str(label) for label in self)


class DataView(Serializable):
    def __init__(
        self,
        data_view_id: DataViewId,
        dataset_id: DatasetId,
        user_id: UserId,
        labels: Optional[LabelSet] = None,
    ):
        self.id = data_view_id
        self.dataset_id = dataset_id
        self.user_id = user_id
        self.labels = labels or []

    def serialize(self) -> List[str]:
        labels = self.labels.serialize() if self.labels else LabelSet()
        return [
            self.id,
            self.dataset_id,
            self.user_id,
            labels,
        ]

    @classmethod
    def deserialize(cls, lst: List) -> DataView:
        return DataView(
            data_view_id=lst[0],
            dataset_id=lst[1],
            user_id=lst[2],
            labels=LabelSet.deserialize(lst[3]),
        )

    def __repr__(self) -> str:
        return "<DataView {id}: user: {user} dataset: {dataset}>".format(
            id=self.id, user=self.user_id, dataset=self.dataset_id
        )
