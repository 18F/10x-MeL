from __future__ import annotations
from abc import ABCMeta
from typing import List, Dict, Union
from pathlib import Path
import json
import logging


log = logging.getLogger(__name__)


SerializableType = Union[str, float, List, Dict]


class Serializable:
    def serialize(self) -> SerializableType:
        raise NotImplementedError()

    @classmethod
    def deserialize(cls, d: SerializableType):
        raise NotImplementedError()


class SerializableHandler(Serializable, metaclass=ABCMeta):
    @classmethod
    def initialization_data(cls):
        raise NotImplementedError()

    @classmethod
    def _load(cls, path: Path):
        """Load the items from disk"""
        if not path.exists():
            path.touch()
            path.write_text(json.dumps(cls.initialization_data()))

        try:
            log.debug("start load %s", cls.__name__)
            with path.open() as f:
                return cls.deserialize(json.load(f))
        finally:
            log.debug("end load %s", cls.__name__)

    def _save(self, path: Path) -> None:
        """Save items to disk"""
        try:
            log.debug("start save %s", self.__class__.__name__)
            """
            path = str(path.absolute())
            with open(path, "w") as f:
                json.dump(obj=self.serialize(), fp=f)
            """
            path.touch(exist_ok=True)
            with path.absolute().open(mode="w") as f:
                json.dump(obj=self.serialize(), fp=f)
        finally:
            log.debug("end save %s", self.__class__.__name__)


class BijectiveMap:
    def __init__(self, u: List, v: List):
        left_to_right = {}
        right_to_left = {}
        for left, right in zip(u, v):
            assert left not in left_to_right, "Not a bijection: {}".format(left)
            assert right not in right_to_left, "Not a bijection: {}".format(right)
            left_to_right[left] = right
            right_to_left[right] = left

        self._left_to_right = left_to_right
        self._right_to_left = right_to_left

    @classmethod
    def from_dict(cls, d: Dict) -> BijectiveMap:
        keys = []
        values = []
        for key, value in d.items():
            keys.append(key)
            values.append(value)
        return cls(keys, values)
