from __future__ import annotations

from typing import List, Dict, Optional, Union
from collections import deque
from enum import Enum
import logging

from analyzer.utils import Serializable
from analyzer.data_view import DataViewId
from analyzer.dataset.dataset_lib import DatasetId
from analyzer.users.users_lib import UserId
from analyzer.constraint_lib import (
    TransformList, TransformTree,
)


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

    def __eq__(self, other) -> bool:
        try:
            if all(
                [
                    self._name == other.name,
                    self._width == other.width,
                    self._font_size == other.font_size,
                ]
            ):
                return True
        except AttributeError:
            pass
        return False


class LabelSequence(Serializable, deque):
    def __init__(self, labels: Optional[Union[List[Label], LabelSequence]] = None):
        super().__init__(labels or deque())

    def remove_by_name(self, name: str):
        for elem in self:
            if elem.name == name:
                self.remove(elem)
                break

    def serialize(self) -> List[Dict[Union[str, int]]]:
        return [label.serialize() for label in self]

    @classmethod
    def deserialize(cls, lst: List[Dict[str, Union[str, int]]]) -> LabelSequence:
        return LabelSequence([Label.deserialize(elem) for elem in lst])

    def __str__(self) -> str:
        return ", ".join(str(label) for label in self)


class DataView(Serializable):
    KEY_ID = "id"
    KEY_PARENT_ID = "parent_id"
    KEY_DATASET_ID = "dataset_id"
    KEY_USER_ID = "user_id"
    KEY_COLUMN_LABELS = "column_labels"
    KEY_TRANSFORMS = "transforms"

    def __init__(
        self,
        data_view_id: DataViewId,
        parent_data_view_id: DataViewId,
        dataset_id: DatasetId,
        user_id: UserId,
        labels: Optional[LabelSequence] = None,
        transforms: Optional[TransformList] = None,
    ):
        self.id = data_view_id
        self.parent_id = parent_data_view_id
        self.dataset_id = dataset_id
        self.user_id = user_id
        self.transforms = transforms or TransformList()

        self._labels = labels or LabelSequence()
        self._label_by_name: Dict[str, Label] = {}

    @property
    def transform_tree(self) -> TransformTree:
        return TransformTree.from_transform_list(self.transforms)

    @property
    def labels(self) -> LabelSequence:
        return self._labels

    def serialize(self) -> Dict[str, str]:
        labels = self.labels.serialize() if self.labels else []
        transforms = self.transforms.serialize() if self.transforms else []
        return {
            self.KEY_ID: self.id,
            self.KEY_PARENT_ID: self.parent_id,
            self.KEY_DATASET_ID: self.dataset_id,
            self.KEY_USER_ID: self.user_id,
            self.KEY_COLUMN_LABELS: labels,
            self.KEY_TRANSFORMS: transforms,
        }

    @classmethod
    def deserialize(cls, d: Dict[str]) -> DataView:
        data_view_id = DataViewId(d[cls.KEY_ID])
        parent_data_view_id = DataViewId(d[cls.KEY_PARENT_ID])
        dataset_id = DatasetId(d[cls.KEY_DATASET_ID])
        user_id = UserId(d[cls.KEY_USER_ID])
        labels = LabelSequence.deserialize(d[cls.KEY_COLUMN_LABELS])
        transforms = TransformList.deserialize(d[cls.KEY_TRANSFORMS])

        return DataView(
            data_view_id=data_view_id,
            parent_data_view_id=parent_data_view_id,
            dataset_id=dataset_id,
            user_id=user_id,
            labels=labels,
            transforms=transforms,
        )

    def __repr__(self) -> str:
        return "<DataView {id}: user: {user} dataset: {dataset}>".format(
            id=self.id, user=self.user_id, dataset=self.dataset_id
        )
