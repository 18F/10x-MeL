from __future__ import annotations

from typing import List, Dict, Deque, Tuple, Optional, Union
from collections import deque
from pathlib import Path
import json
import logging

from analyzer.data_view.data_view_lib import (
    DataView, DataViewId, Label, LabelSequence,
)
from analyzer.constraint_lib import (
    Transform, TransformDef, TransformList, EnrichmentTransform, FilterTransform,
)
from analyzer.dataset.dataset_lib import Dataset, DatasetId
from analyzer.users.users_lib import User, UserId

from analyzer.utils import Serializable, SerializableHandler, SerializableType


log = logging.getLogger(__name__)


class HistoryKey(tuple, Serializable):
    SEPARATOR = "_"

    def __new__(cls, user_id: UserId, dataset_id: DatasetId) -> HistoryKey:
        return tuple.__new__(HistoryKey, tuple((user_id, dataset_id)))

    @property
    def user_id(self) -> UserId:
        return self[0]

    @property
    def dataset_id(self) -> DatasetId:
        return self[1]

    def serialize(self) -> SerializableType:
        return self.SEPARATOR.join([self.user_id, self.dataset_id])

    @classmethod
    def deserialize(cls, s: str) -> HistoryKey:
        user_id, dataset_id = s.split(cls.SEPARATOR)
        return HistoryKey(user_id=user_id, dataset_id=dataset_id)

    def __repr__(self) -> str:
        return "<User:%s Dataset:%s>" % (self.user_id, self.dataset_id)


HistoryLookup = Dict[HistoryKey, DataViewId]


class DataViewHistoryHandler(SerializableHandler):
    def __init__(self, path: Path):
        self._path = path
        self._data_view_history: HistoryLookup = {}

        self._loaded = False
        self.load()

    def serialize(self) -> Dict[SerializableType, SerializableType]:
        sorted_keys = sorted(self._data_view_history.keys())
        return {key.serialize(): self._data_view_history[key] for key in sorted_keys}

    @classmethod
    def deserialize(cls, d: Dict[str, str]) -> HistoryLookup:
        return {
            HistoryKey.deserialize(key): DataViewId(value) for key, value in d.items()
        }

    @classmethod
    def initialization_data(cls) -> HistoryLookup:
        return {}

    def load(self):
        try:
            data_view_history = self._load(self._path)
        except Exception as exc:
            log.error("Could not load data_view_history '%s': %s", self._path, exc)
            data_view_history = {}

        self._data_view_history = data_view_history
        self._loaded = True

    def save(self):
        if not self._loaded:
            log.warning("Attempting to save DataViews that have not been loaded")
            return
        self._save(self._path)

    def keys(self):
        return self._data_view_history.keys()

    @classmethod
    def make_key(cls, user_id: UserId, dataset_id: DatasetId) -> HistoryKey:
        return HistoryKey(user_id, dataset_id)

    def has_key(self, key: HistoryKey) -> bool:
        return key in self._data_view_history

    def get_key(self, key: HistoryKey) -> DataViewId:
        return self._data_view_history.get(key)

    def set_key(self, key: HistoryKey, data_view_id: DataViewId):
        self._data_view_history[key] = data_view_id
        self.save()

    def has(self, user_id: UserId, dataset_id: DatasetId) -> bool:
        return HistoryKey(user_id, dataset_id) in self._data_view_history

    def get(self, user_id: UserId, dataset_id: DatasetId) -> DataViewId:
        return self.get_key(HistoryKey(user_id, dataset_id))

    def set(self, user_id: UserId, dataset_id: DatasetId, data_view_id: DataViewId):
        self.set_key(HistoryKey(user_id, dataset_id), data_view_id)

    def data_view_ids_by_user_id(self, user_id: UserId) -> List[DataViewId]:
        return [
            self.get_key(dv) for dv in self._data_view_history if dv.user_id == user_id
        ]


class DataViewHandler(SerializableHandler):
    def __init__(self, path: Path):
        self._path = path

        self._data_views: List[DataView] = []
        self._data_view_by_id: Dict[DataViewId, DataView] = {}

        self._label_by_name_by_data_view: Dict[DataView, Dict[str, Label]] = {}

        self._data_view_id_by_serialization: Dict[str, DataViewId] = {}

        self._loaded = False
        self.load()

    def serialize(self) -> List:
        return [data_view.serialize() for data_view in self._data_views]

    @classmethod
    def deserialize(cls, lst: List) -> List[DataView]:
        return [DataView.deserialize(elem) for elem in lst]

    @classmethod
    def initialization_data(cls) -> List[DataView]:
        return []

    def load(self):
        try:
            data_views = self._load(self._path)
        except Exception as exc:
            log.error("Could not load '%s': %s", self._path, exc)
            data_views = self.initialization_data()

        self._data_views = data_views

        self._data_view_by_id.clear()
        for data_view in self._data_views:
            self._index_data_view(data_view)

        self._loaded = True

    def save(self):
        if not self._loaded:
            log.warning("Attempting to save DataViews that have not been loaded")
            return
        self._save(self._path)

    def create(
        self,
        parent: Optional[Union[DataView, DataViewId]],
        user: Union[User, UserId],
        dataset: Union[Dataset, DatasetId],
        labels: LabelSequence,
        transforms: Optional[TransformList] = None,
    ) -> DataView:
        log.debug("DataViewHandler.create")

        try:
            parent_id = parent.id
        except AttributeError:
            parent_id = parent

        try:
            dataset_id = dataset.id
        except AttributeError:
            dataset_id = dataset

        try:
            user_id = user.id
        except AttributeError:
            user_id = user

        if not transforms:
            transforms = TransformList()

        data_view = DataView(
            data_view_id=DataViewId(self._next_id),
            parent_data_view_id=parent_id,
            dataset_id=dataset_id,
            user_id=user_id,
            labels=labels,
            transforms=transforms,
        )

        self._data_views.append(data_view)
        self._index_data_view(data_view)

        log.info("saving new DataView: %s", data_view.id)
        self.save()

        return data_view

    @property
    def _next_id(self) -> int:
        return 1 + max((int(data_view.id) for data_view in self._data_views), default=0)

    def _index_data_view(self, data_view: DataView):
        data_view_id = data_view.id

        self._data_view_by_id[data_view_id] = data_view

        serialization = self._serialize_for_cache(
            dataset_id=data_view.dataset_id,
            transforms=data_view.transforms,
        )
        self._data_view_id_by_serialization[serialization] = data_view.id

    @classmethod
    def _serialize_for_cache(cls, dataset_id: DatasetId, transforms: TransformList) -> str:
        serialized_transforms = transforms.serialize() if transforms else []
        return json.dumps([dataset_id, serialized_transforms])

    @property
    def data_views(self) -> List[DataView]:
        return self._data_views

    def find(
        self, user_id: Optional[UserId] = None, dataset_id: Optional[DatasetId] = None
    ) -> List[DataView]:
        results: List[DataView] = []
        for data_view in self.data_views:
            if user_id and user_id != data_view.user_id:
                continue
            if dataset_id and dataset_id != data_view.dataset_id:
                continue
            results.append(data_view)
        return results

    def find_first(
        self, user_id: Optional[UserId] = None, dataset_id: Optional[DatasetId] = None
    ) -> Optional[DataView]:
        for data_view in self.data_views:
            if user_id and user_id != data_view.user_id:
                continue
            if dataset_id and dataset_id != data_view.dataset_id:
                continue
            return data_view

    def by_id(self, data_view_id: DataViewId) -> DataView:
        return self._data_view_by_id.get(data_view_id)

    @property
    def labels(self) -> List[Label]:
        raise NotImplementedError()

    def get_label(self, name: str, data_view: DataView) -> Label:
        try:
            return self._label_by_name_by_data_view.get(data_view).get(name)
        except KeyError:
            return Label(name=name)

    @classmethod
    def _delete_transform_from_data_view(
        cls,
        transform: Transform,
        updated_transforms: TransformList,
        updated_labels: LabelSequence,
        data_view: DataView,
    ) -> Tuple[TransformList, LabelSequence]:
        log.info(f"Removing transform from {data_view.id}")
        transform_tree = data_view.transform_tree

        updated_transforms = TransformList(updated_transforms)
        updated_labels = LabelSequence(updated_labels)

        # the transforms queued for removal
        del_transforms: Deque[Transform] = deque([transform])
        while del_transforms:
            log.info(f"about to pop {del_transforms[0]}")
            transform = del_transforms.popleft()

            if isinstance(transform, EnrichmentTransform):
                for label_name in transform.output_labels:
                    log.info(f"removing by name {label_name}")
                    updated_labels.remove_by_name(label_name)

                del_transforms.extend(transform_tree.get_children_of_transform(transform))
            log.info(f"removing transform: {transform.serialize()}")
            updated_transforms.remove(transform)

        return updated_transforms, updated_labels

    @classmethod
    def _add_transform_to_data_view(
        cls,
        transform: Transform,
        updated_transforms: TransformList,
        updated_labels: LabelSequence,
        data_view: DataView,
    ) -> Tuple[TransformList, LabelSequence]:
        log.info(f"Adding transform to {data_view.id}")
        updated_transforms = TransformList(updated_transforms)
        updated_labels = LabelSequence(updated_labels)

        updated_transforms.append(transform)

        if isinstance(transform, EnrichmentTransform):
            updated_labels.extendleft([Label(name) for name in transform.output_labels])

        return updated_transforms, updated_labels

    def transform_data_view(
        self,
        data_view_id: DataViewId,
        add_transforms: Optional[List[Transform]] = None,
        del_transforms: Optional[List[Transform]] = None,
    ) -> DataView:

        data_view = self.by_id(data_view_id)
        if data_view is None:
            raise ValueError(f"Could not find DataView for id {data_view_id}")

        updated_transforms = TransformList(data_view.transforms)
        updated_labels = LabelSequence(data_view.labels)

        for transforms, apply_change in [
            (del_transforms or [], self._delete_transform_from_data_view),
            (add_transforms or [], self._add_transform_to_data_view),
        ]:
            for transform in transforms:
                updated_transforms, updated_labels = apply_change(
                    transform,
                    updated_transforms,
                    updated_labels,
                    data_view,
                )

        # see if this DataView already exists
        serialization = self._serialize_for_cache(
            data_view.dataset_id,
            updated_transforms,
        )

        existing_id = self._data_view_id_by_serialization.get(serialization, None)
        if existing_id:
            log.info(f"using cached DataView {existing_id}")
            return self.by_id(existing_id)

        log.info("saving new DataView")
        return self.create(
            parent=data_view_id,
            user=data_view.user_id,
            dataset=data_view.dataset_id,
            labels=updated_labels,
            transforms=updated_transforms,
        )
