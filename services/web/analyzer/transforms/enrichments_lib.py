from __future__ import annotations

from collections import defaultdict
from typing import List, Dict, Set, Tuple, DefaultDict, Optional
from pathlib import Path
import logging

from analyzer.utils import SerializableHandler, SerializableType
from analyzer.dataset.dataset_lib import DatasetId

log = logging.getLogger(__name__)


PrimaryKey = str
PrimaryKeysByTag = DefaultDict[str, Set[PrimaryKey]]
TagsByPrimaryKey = DefaultDict[PrimaryKey, Set[str]]
# IdsByTag = Dict[str, Set[int]]
# TagsById = Dict[int, Set[str]]


class TagHandler:
    def __init__(self, tag_dir: Path, tag_prefix: str):
        self._tag_dir = tag_dir
        self._tag_prefix = tag_prefix
        self._tag_map_by_id: Dict[DatasetId, TagMap] = {}

    def _get_tag_map_filename(self, dataset_id: DatasetId) -> str:
        return f"{self._tag_prefix}_{dataset_id}.json"

    def _get_tag_map_path(self, dataset_id: DatasetId) -> Path:
        return self._tag_dir / self._get_tag_map_filename(dataset_id)

    def _load(self, dataset_id: DatasetId) -> Optional[TagMap]:
        tag_map_path = self._get_tag_map_path(dataset_id)
        if not tag_map_path.exists():
            return None

        tag_map = TagMap.load(tag_map_path)
        self._tag_map_by_id[dataset_id] = tag_map
        return tag_map

    def create(self, dataset_id: DatasetId, primary_key_name: str) -> TagMap:
        if dataset_id in self._tag_map_by_id:
            log.warning("id %s already exists in tags", dataset_id)
            return self._tag_map_by_id.get(dataset_id)

        tag_map = TagMap(
            dataset_id=dataset_id,
            primary_key_name=primary_key_name,
            path=self._get_tag_map_path(dataset_id)
        )

        self._tag_map_by_id[dataset_id] = tag_map

        return tag_map

    def get_or_create(self, dataset_id: DatasetId, primary_key_name: str) -> TagMap:
        tag_map = self.get(dataset_id)
        if tag_map is not None:
            return tag_map

        # tag map wasn't found, so create a new one
        return self.create(dataset_id, primary_key_name)

    def get(self, dataset_id: DatasetId) -> Optional[TagMap]:
        if dataset_id not in self._tag_map_by_id:
            # check if TagMap exists, but hasn't been loaded
            self._load(dataset_id)
        return self._tag_map_by_id.get(dataset_id, None)


class TagMap(SerializableHandler):
    KEY_DATASET_ID = "dataset_id"
    KEY_PRIMARY_KEY = "primary_key"
    KEY_MAP = "map"

    def __init__(
        self,
        dataset_id: DatasetId,
        primary_key_name: str,
        path: Path,
        tag_mapping: PrimaryKeysByTag = None,
    ):
        self.dataset_id = dataset_id
        self.primary_key_name = primary_key_name
        self._path = path

        if tag_mapping is None:
            tag_mapping = defaultdict(set)

        self._keys_by_tag: PrimaryKeysByTag = tag_mapping
        self._tags_by_key: TagsByPrimaryKey = self._map_tags_by_key(tag_mapping)

    @classmethod
    def _map_tags_by_key(cls, keys_by_tag: PrimaryKeysByTag) -> TagsByPrimaryKey:
        tags_by_key = defaultdict(set)
        for tag, keys in keys_by_tag.items():
            for key in keys:
                tags_by_key[key].add(tag)
        return tags_by_key

    @classmethod
    def initialization_data(cls) -> Dict:
        return {}

    def serialize(self) -> Dict[str, SerializableType]:
        sorted_tags = sorted(self._keys_by_tag.keys())
        return {
            self.KEY_DATASET_ID: self.dataset_id,
            self.KEY_PRIMARY_KEY: self.primary_key_name,
            self.KEY_MAP: {tag: list(self._keys_by_tag[tag]) for tag in sorted_tags},
        }

    @classmethod
    def deserialize(cls, d: Dict) -> Tuple[DatasetId, str, PrimaryKeysByTag]:
        dataset_id: DatasetId = DatasetId(d[cls.KEY_DATASET_ID])
        primary_key_name: str = d[cls.KEY_PRIMARY_KEY]
        keys_by_tag: Dict[str, List[str]] = d[cls.KEY_MAP]

        tag_mapping = defaultdict(set)
        for tag, keys in keys_by_tag.items():
            tag_mapping[tag] = {PrimaryKey(key) for key in keys}

        return dataset_id, primary_key_name, tag_mapping

    def keys(self) -> List[str]:
        return list(self._tags_by_key.keys())

    def get_ids_by_tag(self, tag: str) -> List[PrimaryKey]:
        return list(self._keys_by_tag.get(tag, set()))

    def get_tags_by_key(self, key: PrimaryKey) -> List[str]:
        return list(self._tags_by_key.get(key, set()))

    def add_tags(self, tags: List[str], keys: List[PrimaryKey]) -> Dict[str, Set[PrimaryKey]]:
        result = {tag: self.add_tag(tag, keys=keys) for tag in tags}
        self.save()
        return result

    def add_tag(self, tag: str, keys: List[PrimaryKey]) -> Set[PrimaryKey]:
        result = self._add_tag(tag, keys=keys)
        self.save()
        return result

    def _add_tag(self, tag: str, keys: List[PrimaryKey]) -> Set[PrimaryKey]:
        self._keys_by_tag[tag].update(keys)
        for key in keys:
            self._tags_by_key[key].add(tag)
        return self._keys_by_tag[tag]

    def remove_tags(self, tags, keys: List[PrimaryKey]) -> Dict[str, Set[PrimaryKey]]:
        result = {tag: self._remove_tag(tag, keys=keys) for tag in tags}
        self.save()
        return result

    def remove_tag(self, tag, keys: List[PrimaryKey]) -> Set[PrimaryKey]:
        result = self._remove_tag(tag, keys)
        self.save()
        return result

    def _remove_tag(self, tag, keys: List[PrimaryKey]) -> Set[PrimaryKey]:
        self._keys_by_tag[tag].difference_update(keys)
        for key in keys:
            self._tags_by_key[key].remove(tag)

        if not self._keys_by_tag[tag]:
            del self._keys_by_tag[tag]

        return self._keys_by_tag[tag]

    @property
    def tag_set(self) -> Set[str]:
        return set(self._keys_by_tag.keys())

    def save(self):
        self._save(self._path)

    @classmethod
    def load(cls, path: Path) -> TagMap:
        dataset_id, primary_key_name, tag_mapping = cls._load(path)

        return TagMap(
            dataset_id=dataset_id,
            primary_key_name=primary_key_name,
            path=path,
            tag_mapping=tag_mapping,
        )
