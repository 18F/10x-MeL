from __future__ import annotations

from typing import List, Optional
import logging

import pandas as pd

DataFrame = pd.DataFrame


log = logging.getLogger(__name__)

SPACE = " "

'''
class BaseDerivedColumn:
    def __init__(self, column_labels: List[str]):
        self.column_labels = column_labels

    @property
    def type(self) -> str:
        raise NotImplementedError()

    def apply(self, name: str, data_frame: DataFrame, **params):
        raise NotImplementedError()


DerivedColumn = BaseDerivedColumn


class DerivedColumnSpec:
    def __init__(self, required: List[str], optional: Optional[List[str]] = None):
        self.required = required
        self.optional = optional


class StringConcatenatedColumn(BaseDerivedColumn):
    def __init__(self, column_labels: List[str], sep: Optional[str] = SPACE):
        super().__init__(column_labels)
        self.sep = sep

    @classmethod
    def type(cls) -> str:
        return "StringConcat"

    @classmethod
    def spec(cls) -> DerivedColumnSpec:
        return DerivedColumnSpec(
            required=["column_labels"],
            optional=["sep"],
        )

    def apply(self, name: str, df: DataFrame, sep: Optional[str] = " "):
        column_labels = self.column_labels

        # choosing a character that will never occur in input
        null_sep = "\x01"

        df[name] = df[column_labels[0]].astype(str).str.strip()
        for column_label in column_labels[1:]:
            df[name] += null_sep + df[column_label].astype(str).str.strip()

        all_sep = "^[{sep}]+$".format(sep=null_sep)
        start_sep = "^[{sep}]+".format(sep=null_sep)
        end_sep = "[{sep}]+$".format(sep=null_sep)
        repeated_sep = "[{sep}]+".format(sep=null_sep)

        df[name] = df[name].str.replace(all_sep, "")
        df[name] = df[name].str.replace(start_sep, "")
        df[name] = df[name].str.replace(end_sep, "")
        df[name] = df[name].str.replace(repeated_sep, sep)



active_column_cls_list = [
    StringConcatenatedColumn,
]

column_cls_by_type = {column_cls.type(): column_cls for column_cls in active_column_cls_list}

class ColumnHandler:
    """
    column_def_by_label = {
        "Text": [
            "StringConcat", {"column_labels": ["Q3", "Q5", "Q6", "Q7"], "sep": ". "},
        ],
    }
    """
    column_def_by_label = {}

    def get_column_by_label(self, label) -> DerivedColumn:
        log.info("column_cls_by_type: %s", column_cls_by_type)
        try:
            column_def = self.column_def_by_label[label]
        except KeyError:
            raise ValueError(f'Derived label "{label}" not found')

        column_type, params = column_def

        try:
            column_cls = column_cls_by_type[column_type]
        except KeyError:
            raise ValueError('Column type "{}" not found'.format(column_type))

        derived_column = column_cls(**params)
        return derived_column
'''