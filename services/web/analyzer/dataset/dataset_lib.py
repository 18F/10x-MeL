from __future__ import annotations

from typing import List, Optional
import logging

from analyzer.utils import Serializable
from analyzer.dataset import DatasetId


log = logging.getLogger(__name__)


class Dataset(Serializable):
    def __init__(self, dataset_id: DatasetId, filename: str, name: Optional[str] = None):
        self.id = dataset_id
        self.filename = filename
        self.name = name or filename

    def serialize(self) -> List:
        return [self.id, self.filename, self.name]

    @classmethod
    def deserialize(cls, d: List) -> Dataset:
        return Dataset(dataset_id=d[0], filename=d[1], name=d[2])

    def __repr__(self) -> str:
        return "<Dataset {id}: {filename}>".format(id=self.id, filename=self.filename)
