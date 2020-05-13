from __future__ import annotations

from typing import List, Dict, Iterable, Union, Optional
import logging
import json
import pandas as pd

from analyzer.utils import Serializable, SerializableType

DataFrame = pd.DataFrame

COMMA = ","

Value = Union[str, float]


log = logging.getLogger(__name__)


class Parameter(Serializable):
    TYPE_TEXT = "text"
    TYPE_ = "text"
    TYPE_TEXT_LIST = "text_list"

    KEY_TYPE = "type"
    KEY_NAME = "name"
    KEY_LABEL = "label"
    KEY_EXAMPLE = "example"

    def __init__(self, parameter_type: str, name: str, label: str, example: str):
        self.type = parameter_type
        self.name = name
        self.label = label
        self.example = example

    def serialize(self) -> Dict[str, str]:
        return dict(
            type=self.type,
            name=self.name,
            label=self.label,
            example=self.example,
        )

    @classmethod
    def deserialize(cls, d: Dict) -> Parameter:
        return Parameter(
            parameter_type=d[cls.KEY_TYPE],
            name=d[cls.KEY_NAME],
            label=d[cls.KEY_LABEL],
            example=d[cls.KEY_EXAMPLE],
        )


class TextParameter(Parameter):
    def __init__(self, name: str, label: str, example: str):
        super().__init__(Parameter.TYPE_TEXT, name, label, example)


class TextListParameter(Parameter):
    def __init__(self, name: str, label: str, example: str):
        super().__init__(Parameter.TYPE_TEXT_LIST, name, label, example)


class ConstraintDef(Serializable):
    KEY_TYPE = "type"
    KEY_DESC = "description"
    KEY_PARAMETERS = "params"

    def __init__(self, constraint_type: str, description: List[str], parameters: List[Parameter]):
        self.type = constraint_type
        self.description = description
        self.parameters = parameters

    def serialize(self) -> Dict[str, str]:
        return {
            self.KEY_TYPE: self.type,
            self.KEY_DESC: self.description,
            self.KEY_PARAMETERS: [p.serialize() for p in self.parameters]
        }

    @classmethod
    def deserialize(cls, d: SerializableType):
        return ConstraintDef(
            constraint_type=d[cls.KEY_TYPE],
            description=d[cls.KEY_DESC],
            parameters=[p.deserialize() for p in d[cls.KEY_PARAMETERS]],
        )


class ConstraintManager:
    def __init__(self):
        self._constraint_by_name: Dict[str, Constraint] = {}

    def register(self, constraint: Constraint):
        self._constraint_by_name[constraint.type()] = constraint

    def constraint_by_name(self, name: str) -> Constraint:
        return self._constraint_by_name[name]

    def get_constraints(self) -> Iterable[Constraint]:
        return self._constraint_by_name.values()

    def get_constraint_defs(self) -> Iterable[ConstraintDef]:
        for constraint in self._constraint_by_name.values():
            yield ConstraintDef(
                constraint_type=constraint.type(),
                description=constraint.description(),
                parameters=constraint.parameters(),
            )


# a singleton for constraint class registration
constraint_manager = ConstraintManager()


def register(cls):
    constraint_manager.register(cls)
    return cls


class ConstraintList(Serializable, list):
    def __init__(self, constraints: Optional[List[Constraint]] = None):
        super().__init__(constraints or [])

    def __eq__(self, other):
        if len(self) != len(other):
            return False
        for elem, elem_other in zip(self, other):
            if elem != elem_other:
                return False
        return True

    def serialize(self) -> List[List[str]]:
        return [constraint.serialize() for constraint in self]

    @classmethod
    def deserialize(cls, lst: List[List[Union[str, int]]]) -> ConstraintList:
        return ConstraintList([Constraint.deserialize(elem) for elem in lst])


class Constraint(Serializable):
    KEY_TYPE = "type"
    KEY_DESC = "description"
    KEY_PARAMETERS = "params"

    def apply(self, df: DataFrame) -> bool:
        raise NotImplementedError()

    @staticmethod
    def type() -> str:
        return "Base"

    def __eq__(self, other):
        try:
            return self.__dict__ == other.__dict__
        except AttributeError:
            return False

    @staticmethod
    def description() -> List[str]:
        raise NotImplementedError()

    @staticmethod
    def parameters() -> List[Parameter]:
        raise NotImplementedError()

    def __repr__(self):
        raise NotImplementedError()

    def __hash__(self):
        return hash(json.dumps(self.serialize()))

    @classmethod
    def serialize(cls) -> Dict:
        raise NotImplementedError()

    @classmethod
    def deserialize(cls, lst: List) -> Constraint:
        constraint_cls = constraint_manager.constraint_by_name(lst[0])
        return constraint_cls.deserialize(lst)


@register
class ExactMatch(Constraint):
    KEY_COLUMN = "column"
    KEY_VALUE = "value"

    def __init__(self, key: str, value: Value):
        self.key = key
        self.value = value

    def apply(self, df: DataFrame) -> bool:
        return df[df[self.key].astype(str) == self.value]

    def __repr__(self) -> str:
        return "{}:{}={}".format(self.type(), self.key, self.value)

    @staticmethod
    def type() -> str:
        return "ExactMatch"

    @staticmethod
    def description() -> List[str]:
        return ["{key}", " = ", "{value}"]

    @staticmethod
    def parameters() -> List[Parameter]:
        return [
            TextParameter(name="key", label="column", example="Country"),
            TextParameter(name="value", label="value", example="United States"),
        ]

    def serialize(self) -> List[str]:
        return [self.type(), self.key, self.value]

    @classmethod
    def deserialize(cls, lst: Dict) -> ExactMatch:
        assert cls.type() == lst[0]
        return cls(key=lst[1], value=lst[2])


'''
ExactMatchDef = ConstraintDef(
    type=ExactMatch.type(),
    description=["{key}", " = ", "{value}"],
    parameters=[
        TextParameter(name="key", label="column", example="Country"),
        TextParameter(name="value", label="value", example="United States"),
    ],
)
'''


@register
class MatchAny(Constraint):
    def __init__(self, key: str, values: List[Union[str, float]]):
        self.key = key
        self.values = values

    @classmethod
    def from_string(cls, key: str, values: str, sep: Optional[str] = COMMA):
        return MatchAny(
            key=key,
            values=[s.strip() for s in values.split(sep)],
        )

    def apply(self, df: DataFrame) -> bool:
        return df[df[self.key].isin(self.values)]

    def __repr__(self) -> str:
        return "{}:{}={}".format(self.type(), self.key, self.values)

    @staticmethod
    def type() -> str:
        return "MatchAny"

    @staticmethod
    def description() -> List[str]:
        return ["{key}", " in [", "{values}", "]"]

    @staticmethod
    def parameters() -> List[Parameter]:
        return [
            TextParameter(name="key", label="column", example="State"),
            TextListParameter(name="values", label="values", example="GA,ME,IL,WI"),
        ]

    def serialize(self) -> List[str]:
        return [self.type(), self.key, self.values]

    @classmethod
    def deserialize(cls, lst: List) -> MatchAny:
        assert cls.type() == lst[0]
        return cls(key=lst[1], values=lst[2])


@register
class DoesNotMatch(Constraint):
    def __init__(self, key: str, value: Value):
        self.key = key
        self.value = value

    def apply(self, df: DataFrame) -> bool:
        return df[df[self.key] != self.value]

    def __repr__(self) -> str:
        return "{}:{}={}".format(self.type(), self.key, self.value)

    @staticmethod
    def type() -> str:
        return "DoesNotMatch"

    @staticmethod
    def description() -> List[str]:
        return ["{key}", " != ", "{value}"]

    @staticmethod
    def parameters() -> List[Parameter]:
        return [
            TextParameter(name="key", label="column", example="Country"),
            TextParameter(name="value", label="value", example="United States"),
        ]

    def serialize(self) -> List[str]:
        return [self.type(), self.key, self.value]

    @classmethod
    def deserialize(cls, lst: List) -> DoesNotMatch:
        assert cls.type() == lst[0]
        return cls(key=lst[1], value=lst[2])


@register
class DoesNotMatchAny(Constraint):
    def __init__(self, key: str, values: List[Union[str, float]]):
        self.key = key
        self.values = values

    @classmethod
    def from_string(cls, key: str, values: str, sep: Optional[str] = COMMA):
        return MatchAny(
            key=key,
            values=[s.strip() for s in values.split(sep)],
        )

    def apply(self, df: DataFrame) -> bool:
        return df[~df[self.key].isin(self.values)]

    def __repr__(self) -> str:
        return "{}:{}={}".format(self.type(), self.key, self.values)

    @staticmethod
    def type() -> str:
        return "DoesNotMatchAny"

    @staticmethod
    def description() -> List[str]:
        return ["{key}", " not in [", "{values}", "]"]

    @staticmethod
    def parameters() -> List[Parameter]:
        return [
            TextParameter(name="key", label="column", example="State"),
            TextListParameter(name="values", label="values", example="GA,ME,IL,WI"),
        ]

    def serialize(self) -> List[str]:
        return [self.type(), self.key, self.values]

    @classmethod
    def deserialize(cls, lst: List) -> DoesNotMatchAny:
        assert cls.type() == lst[0]
        return cls(key=lst[1], values=lst[2])


@register
class HasText(Constraint):
    def __init__(self, key: str, value: str):
        self.key = key
        self.value = str(value)

    def apply(self, df: DataFrame) -> bool:
        return df[df[self.key].astype(str).str.lower().str.contains(self.value.lower())]

    def __repr__(self) -> str:
        return "{}:{}={}".format(self.type(), self.key, self.value)

    @staticmethod
    def type() -> str:
        return "HasText"

    @staticmethod
    def description() -> List[str]:
        return ["{key}", " contains ", "{value}"]

    @staticmethod
    def parameters() -> List[Parameter]:
        return [
            TextParameter(name="key", label="column", example="Comments"),
            TextParameter(name="value", label="value", example="help"),
        ]

    def serialize(self) -> List[str]:
        return [self.type(), self.key, self.value]

    @classmethod
    def deserialize(cls, lst: List) -> HasText:
        assert cls.type() == lst[0]
        return cls(key=lst[1], value=lst[2])


@register
class DoesNotHaveText(Constraint):
    def __init__(self, key: str, value: str):
        self.key = key
        self.value = str(value)

    def apply(self, df: DataFrame) -> bool:
        return df[~df[self.key].astype(str).str.lower().str.contains(self.value.lower())]

    def __repr__(self) -> str:
        return "{}:{}={}".format(self.type(), self.key, self.value)

    @staticmethod
    def type() -> str:
        return "DoesNotHaveText"

    @staticmethod
    def description() -> List[str]:
        return ["{key}", " does not contain ", "{value}"]

    @staticmethod
    def parameters() -> List[Parameter]:
        return [
            TextParameter(name="key", label="column", example="email"),
            TextParameter(name="value", label="value", example=".gov"),
        ]

    def serialize(self) -> List[str]:
        return [self.type(), self.key, self.value]

    @classmethod
    def deserialize(cls, lst: List) -> DoesNotHaveText:
        assert cls.type() == lst[0]
        return cls(key=lst[1], value=lst[2])


'''
@register
class Calculation(Constraint):
    def __init__(self, expression):
        self.expression = expression

    def apply(self, df: DataFrame) -> bool:
        raise NotImplementedError

    def __repr__(self) -> str:
        return "{}:{}".format(self.type(), self.expression)

    @staticmethod
    def type() -> str:
        return "Calculation"

    @staticmethod
    def description() -> List[str]:
        return ["{key}", " does not contain ", "{value}"]

    @staticmethod
    def parameters() -> List[Parameter]:
        return [
            TextParameter(name="expression", label="expression", example="time1 + time2"),
            TextParameter(name="equivalenceType", label="equivalenceType", example="="),
            FloatParameter(name="value", label="value", example="10.5"),
        ]
'''

Transform = Constraint
TransformList = ConstraintList
