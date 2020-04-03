from __future__ import annotations

from typing import List, Dict, Optional
from pathlib import Path
import logging

from analyzer.utils import SerializableHandler
from analyzer.dataset.dataset_lib import Dataset, DatasetId


log = logging.getLogger(__name__)


class DatasetHandler(SerializableHandler):
    def __init__(self, path: Path):
        self._path = path

        self._datasets: Optional[List[Dataset]] = None
        self._dataset_by_filename: Dict[str, Dataset] = {}
        self._dataset_by_id: Dict[DatasetId, Dataset] = {}

        self.load()

    def find(self, string: Optional[str] = None) -> List[Dataset]:
        string = string or ""
        datasets = []
        for dataset in self._datasets:
            if string in dataset.name:
                datasets.append(dataset)
            elif string in dataset.filename:
                datasets.append(dataset)
        return datasets

    def serialize(self) -> List:
        return [dataset.serialize() for dataset in self._datasets]

    @classmethod
    def deserialize(cls, lst: List) -> List[Dataset]:
        return [Dataset.deserialize(elem) for elem in lst]

    def initialization_data(self) -> List[Dataset]:
        return []

    def load(self):
        try:
            datasets = self._load(self._path)
        except Exception as exc:
            log.error("Could not load datasets '%s': %s", self._path, exc)
            datasets = self.initialization_data()

        self._datasets = datasets

        self._dataset_by_filename = {}
        self._dataset_by_id = {}
        for dataset in self._datasets:
            self._index_dataset(dataset)

    def _index_dataset(self, dataset: Dataset):
        filename = dataset.filename
        assert not self.has_filename(filename), "filename {} already exists".format(filename)
        self._dataset_by_filename[filename] = dataset

        dataset_id = dataset.id
        assert dataset_id not in self._dataset_by_id, "id {} already exists".format(dataset_id)
        self._dataset_by_id[dataset_id] = dataset

    def save(self):
        if self._datasets is None:
            log.warning("Attempting to save Datasets that have not been loaded")
            return
        self._save(self._path)

    def create(self, filename: str) -> Dataset:
        if filename in self._dataset_by_filename:
            log.warning("filename %s already exists in datasets", filename)
            return self._dataset_by_filename.get(filename)

        dataset = Dataset(dataset_id=DatasetId(self._next_index), filename=filename)

        self._datasets.append(dataset)
        self._index_dataset(dataset)

        self.save()

        return dataset

    @property
    def _next_index(self) -> int:
        return 1 + max((int(dataset.id) for dataset in self._datasets), default=0)

    def has_filename(self, filename: str) -> bool:
        return filename in self._dataset_by_filename

    def by_filename(self, filename: str) -> Dataset:
        return self._dataset_by_filename.get(filename)

    def by_id(self, dataset_id: DatasetId) -> Optional[Dataset]:
        return self._dataset_by_id.get(dataset_id)
