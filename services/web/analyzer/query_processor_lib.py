from __future__ import annotations

from typing import List, Dict, Optional
import json
import logging
import pandas as pd

from analyzer.utils import Serializable, SerializableType
from analyzer.constraint_lib import transform_manager, Transform, TransformList

DataFrame = pd.DataFrame

log = logging.getLogger(__name__)


class QueryResponse(Serializable, dict):
    KEY_DATA = "data"
    KEY_LABELS = "labels"
    KEY_ERROR = "error"
    KEY_MSG = "msg"

    EMPTY = None

    active_keys = [KEY_ERROR, KEY_MSG, KEY_DATA, KEY_LABELS]

    def __init__(
        self,
        msg: Optional[str] = None,
        data: Optional[SerializableType] = None,
        labels: Optional[List[str]] = None,
        error: int = 0,
    ):
        super().__init__(
            {
                self.KEY_DATA: data,
                self.KEY_LABELS: labels,
                self.KEY_ERROR: error,
                self.KEY_MSG: msg,
            }
        )

    @property
    def error(self) -> int:
        return self.get(self.KEY_ERROR)

    @property
    def msg(self) -> str:
        return self.get(self.KEY_MSG, "")

    @property
    def data(self) -> DataFrame:
        return self.get(self.KEY_DATA, DataFrame())

    def serialize(self) -> SerializableType:
        result = {}
        for key in self.active_keys:
            try:
                result[key] = self[key].serialize()
            except AttributeError:
                result[key] = self[key]
        return result

    @classmethod
    def deserialize(cls, d: Dict) -> QueryResponse:
        return QueryResponse(**d)


class QueryErrorResponse(QueryResponse):
    def __init__(self, msg):
        super().__init__(error=-1, msg=msg)


class Query:
    def __init__(self, transforms: TransformList):
        self.transforms = transforms

    def __hash__(self) -> str:
        return ",".join(sorted(repr(transform) for transform in self.transforms))

    def apply(self, df: DataFrame) -> DataFrame:
        new_df = df

        '''
        from functools import partial
        reduce(lambda result, f: f.apply(result), self.transforms, df)
        '''

        for transform in self.transforms:
            new_df = transform.apply(new_df)
        return new_df


class QueryParser:
    KEY_TRANSFORMS = "transforms"
    KEY_CLASS_NAME = "name"
    KEY_ARGS = "args"

    @classmethod
    def from_dict(cls, d: Dict) -> Query:
        transform_dicts = d.get(cls.KEY_TRANSFORMS, [])
        transforms: TransformList = TransformList()

        for d in transform_dicts:
            class_name: List[Dict] = d.get(cls.KEY_CLASS_NAME, None)
            args = d.get(cls.KEY_ARGS, {})

            if class_name is None:
                log.error("Transform has no class name - skipping: {}".format(d))
                continue

            transform_cls = transform_manager.transform_by_name(class_name)
            transform = transform_cls(**args)
            transforms.append(transform)

        return Query(
            transforms=transforms,
        )

    @classmethod
    def from_string(cls, string: str) -> Query:
        return cls.from_dict(json.loads(string))
