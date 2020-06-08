from __future__ import annotations

from typing import List, Dict, Tuple, Iterable, Union, Type, Optional
from collections import defaultdict
import logging
import json
import pandas as pd

from analyzer.utils import Serializable, SerializableType
from analyzer.contrib.problem_detector import (
    ProblemReportDetector,
    ResponseMapper,
)
from analyzer.transforms.enrichments_lib import TagHandler, TagMap, DatasetId

Value = Union[str, float]

COMMA = ","
SPACE = " "

DataFrame = pd.DataFrame


log = logging.getLogger(__name__)


class Parameter(Serializable):
    TYPE_TEXT = "text"
    TYPE_TEXT_LIST = "text_list"
    TYPE_INT = "int"
    TYPE_FLOAT = "float"
    TYPE_COLUMN_NAME = "column_name"

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


class IntegerParameter(Parameter):
    def __init__(self, name: str, label: str, example: str):
        super().__init__(Parameter.TYPE_INT, name, label, example)


class ColumnNameParameter(Parameter):
    def __init__(self, name: str, label: str, example: str):
        super().__init__(Parameter.TYPE_COLUMN_NAME, name, label, example)


class ConstraintDef(Serializable):
    KEY_TYPE = "type"
    KEY_DESC = "description"
    KEY_PARAMETERS = "params"
    KEY_OPS = "ops"

    def __init__(
        self,
        constraint_type: str,
        description: List[str],
        parameters: List[Parameter],
        operations: List[str],
    ):
        self.type = constraint_type
        self.description = description
        self.parameters = parameters
        self.operations = operations

    def serialize(self) -> Dict[str, str]:
        return {
            self.KEY_TYPE: self.type,
            self.KEY_DESC: self.description,
            self.KEY_PARAMETERS: [p.serialize() for p in self.parameters],
            self.KEY_OPS: [op for op in self.operations],
        }

    @classmethod
    def deserialize(cls, d: SerializableType):
        return ConstraintDef(
            constraint_type=d[cls.KEY_TYPE],
            description=d[cls.KEY_DESC],
            parameters=[p.deserialize() for p in d[cls.KEY_PARAMETERS]],
            operations=[op for op in d[cls.KEY_OPS]],
        )


class TransformManager:
    operations = {"filter", "enrich"}

    def __init__(self):
        self._transform_by_name: Dict[str, ConstraintType] = {}
        self._transforms_by_operation: Dict[str, List[ConstraintType]] = defaultdict(list)
        self._operations_by_transform: Dict[ConstraintType, List[str]] = defaultdict(list)

    def register(self, transform_cls: ConstraintType):
        self._transform_by_name[transform_cls.type()] = transform_cls

        for operation in self.operations:
            try:
                getattr(transform_cls, operation)
                self._transforms_by_operation[operation].append(transform_cls)
                self._operations_by_transform[transform_cls].append(operation)
            except AttributeError:
                pass

    def constraint_by_name(self, name: str) -> ConstraintType:
        return self._transform_by_name[name]

    def get_constraints(self) -> Iterable[ConstraintType]:
        return self._transform_by_name.values()

    def get_constraint_defs(self) -> Iterable[ConstraintDef]:
        for transform in self._transform_by_name.values():
            yield ConstraintDef(
                constraint_type=transform.type(),
                description=transform.description(),
                parameters=transform.parameters(),
                operations=self._operations_by_transform[transform]
            )


# a singleton for constraint class registration
transform_manager = TransformManager()


def register(cls):
    print(f"registering {cls.__name__}")
    transform_manager.register(cls)
    return cls


class TransformResourceHandler:
    def __init__(self, tag_handler: TagHandler):
        self.tag_handler = tag_handler

    def instance(self, data_view) -> TransformResource:
        return TransformResource(
            tag=self.tag_handler.get(data_view.dataset_id),
            tag_handler=self.tag_handler,
            dataset_id=data_view.dataset_id,
        )


class TransformResource:
    def __init__(self, tag: TagMap, tag_handler: TagHandler, dataset_id: DatasetId):
        self.tag = tag
        self.tag_handler = tag_handler
        self.dataset_id = dataset_id


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

    @staticmethod
    def type() -> str:
        return "Base"

    def __eq__(self, other):
        try:
            return self.__dict__ == other.__dict__
        except AttributeError:
            return False

    @property
    def operation(self) -> str:
        raise NotImplementedError()

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
        constraint_cls = transform_manager.constraint_by_name(lst[0])
        return constraint_cls.deserialize(lst)


class FilterTransform(Constraint):
    def filter(self, df: DataFrame, resources: TransformResource = None) -> bool:
        raise NotImplementedError()

    @property
    def operation(self) -> str:
        raise NotImplementedError()

    @staticmethod
    def description() -> List[str]:
        raise NotImplementedError()

    @staticmethod
    def parameters() -> List[Parameter]:
        raise NotImplementedError()

    def __repr__(self):
        raise NotImplementedError()

    @classmethod
    def serialize(cls) -> Dict:
        raise NotImplementedError()


class EnrichmentTransform(Constraint):
    def enrich(self, df: DataFrame, resources: TransformResource = None) -> EnrichmentResult:
        raise NotImplementedError()

    @property
    def operation(self) -> str:
        raise NotImplementedError()

    @property
    def labels(self) -> List[str]:
        raise NotImplementedError()

    @staticmethod
    def description() -> List[str]:
        raise NotImplementedError()

    @staticmethod
    def parameters() -> List[Parameter]:
        raise NotImplementedError()

    def __repr__(self):
        raise NotImplementedError()

    @classmethod
    def serialize(cls) -> Dict:
        raise NotImplementedError()


Transform = Constraint
ConstraintType = Type[Constraint]


@register
class ExactMatch(FilterTransform):
    KEY_COLUMN = "column"
    KEY_VALUE = "value"

    def __init__(self, key: str, value: Value, operation: str):
        self.key = key
        self.value = value
        self._operation = operation

    @property
    def operation(self) -> str:
        return self._operation

    def filter(self, df: DataFrame, resources: TransformResource = None) -> bool:
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
            ColumnNameParameter(name="key", label="column", example="Country"),
            TextParameter(name="value", label="value", example="United States"),
        ]

    def serialize(self) -> List[str]:
        return [self.type(), self.operation, self.key, self.value]

    @classmethod
    def deserialize(cls, lst: List) -> ExactMatch:
        assert cls.type() == lst[0]
        return cls(
            operation=lst[1], key=lst[2], value=lst[3],
        )


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
class MatchAny(FilterTransform):
    def __init__(self, key: str, values: List[Value], operation: str):
        self.key = key
        self.values = values
        self._operation = operation

    @property
    def operation(self) -> str:
        return self._operation

    def filter(self, df: DataFrame, resources: TransformResource = None) -> bool:
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
            ColumnNameParameter(name="key", label="column", example="State"),
            TextListParameter(name="values", label="values", example="GA,ME,IL,WI"),
        ]

    def serialize(self) -> List[str]:
        return [self.type(), self.operation, self.key, self.values]

    @classmethod
    def deserialize(cls, lst: List) -> MatchAny:
        assert cls.type() == lst[0]
        return cls(operation=lst[1], key=lst[2], values=lst[3])


@register
class DoesNotMatch(FilterTransform):
    def __init__(self, key: str, value: Value, operation: str):
        self.key = key
        self.value = value
        self._operation = operation

    @property
    def operation(self) -> str:
        return self._operation

    def filter(self, df: DataFrame, resources: TransformResource = None) -> bool:
        return df[df[self.key].astype(str) != self.value]

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
            ColumnNameParameter(name="key", label="column", example="Country"),
            TextParameter(name="value", label="value", example="United States"),
        ]

    def serialize(self) -> List[str]:
        return [self.type(), self.operation, self.key, self.value]

    @classmethod
    def deserialize(cls, lst: List) -> DoesNotMatch:
        assert cls.type() == lst[0]
        return cls(operation=lst[1], key=lst[2], value=lst[3])


@register
class DoesNotMatchAny(FilterTransform):
    def __init__(self, key: str, values: List[Value], operation: str):
        self.key = key
        self.values = values
        self._operation = operation

    @property
    def operation(self) -> str:
        return self._operation

    def filter(self, df: DataFrame, resources: TransformResource = None) -> bool:
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
            ColumnNameParameter(name="key", label="column", example="State"),
            TextListParameter(name="values", label="values", example="GA,ME,IL,WI"),
        ]

    def serialize(self) -> List[str]:
        return [self.type(), self.operation, self.key, self.values]

    @classmethod
    def deserialize(cls, lst: List) -> DoesNotMatchAny:
        assert cls.type() == lst[0]
        return cls(operation=lst[1], key=lst[2], values=lst[3])


@register
class HasText(FilterTransform):
    def __init__(self, key: str, value: str, operation: str):
        self.key = key
        self.value = str(value)
        self._operation = operation

    @property
    def operation(self) -> str:
        return self._operation

    def filter(self, df: DataFrame, resources: TransformResource = None) -> bool:
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
            ColumnNameParameter(name="key", label="column", example="Comments"),
            TextParameter(name="value", label="value", example="help"),
        ]

    def serialize(self) -> List[str]:
        return [self.type(), self.operation, self.key, self.value]

    @classmethod
    def deserialize(cls, lst: List) -> HasText:
        assert cls.type() == lst[0]
        return cls(operation=lst[1], key=lst[2], value=lst[3])


@register
class DoesNotHaveText(FilterTransform):
    def __init__(self, key: str, value: str, operation: str):
        self.key = key
        self.value = str(value)
        self._operation = operation

    @property
    def operation(self) -> str:
        return self._operation

    def filter(self, df: DataFrame, resources: TransformResource = None) -> bool:
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
            ColumnNameParameter(name="key", label="column", example="email"),
            TextParameter(name="value", label="value", example=".gov"),
        ]

    def serialize(self) -> List[str]:
        return [self.type(), self.operation, self.key, self.value]

    @classmethod
    def deserialize(cls, lst: List) -> DoesNotHaveText:
        assert cls.type() == lst[0]
        return cls(operation=lst[1], key=lst[2], value=lst[3])


'''
@register
class Calculation(Constraint):
    def __init__(self, expression):
        self.expression = expression

    def filter(self, df: DataFrame, resources: TransformResource = None) -> bool:
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

TransformDef = ConstraintDef
TransformList = ConstraintList


class EnrichmentResult:
    def __init__(
        self,
        labels: Optional[List[str]] = None,
        sort: Optional[Tuple[str, bool]] = None,
    ):
        self.labels = labels or []
        self.sort = sort or (None, None)


@register
class MergeColumnText(EnrichmentTransform):
    DEFAULT_SEP = SPACE

    def __init__(
        self,
        name: str,
        column_labels: List[str],
        operation: str,
    ):
        self.name = name
        self.column_labels = column_labels
        self._operation = operation

    @property
    def operation(self) -> str:
        return self._operation

    @staticmethod
    def type() -> str:
        return "MergeColumnText"

    def enrich(self, df: DataFrame, resources: TransformResource = None) -> EnrichmentResult:
        name = self.name
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
        df[name] = df[name].str.replace(repeated_sep, SPACE)

        return EnrichmentResult(labels=[name])

    @property
    def labels(self) -> List[str]:
        return [self.name]

    def __repr__(self) -> str:
        return "{}:{}".format(self.type(), COMMA.join(self.column_labels))

    @staticmethod
    def description() -> List[str]:
        return ["Merge(", "{column_labels}", ") as ", "{name}"]

    @staticmethod
    def parameters() -> List[Parameter]:
        return [
            TextParameter(name="name", label="name", example="MyText"),
            TextListParameter(name="column_labels", label="columns", example="Q1,Q4,Q7"),
        ]

    def serialize(self) -> List:
        return [
            self.type(),
            self.operation,
            self.name,
            self.column_labels,
        ]

    @classmethod
    def deserialize(cls, lst: List) -> MergeColumnText:
        assert cls.type() == lst[0]
        operation = lst[1]
        name = lst[2]
        column_labels = lst[3]

        return cls(
            name=name,
            column_labels=column_labels,
            operation=operation,
        )


@register
class ProblemReport(EnrichmentTransform):
    RESPONSE_GROUPS: List[List[str]] = [
        ["Very poor", "Poor", "Fair", "Good", "Very good"],
        ["Very unlikely", "Unlikely", "Neither likely nor unlikely", "Likely", "Very likely"],
        [
            "No",
            "Not yet, but still trying",
            "Just browsing / not trying to accomplish anything specific",
            "Yes, partly",
            "Yes, fully",
        ],
    ]

    def __init__(
        self,
        operation: str,
        text_column_labels: List[str],
        rating_column_labels: List[str],
    ):
        self.text_column_labels = text_column_labels
        self.rating_column_labels = rating_column_labels
        self._operation = operation

        response_mapper = ResponseMapper()
        rating_map = response_mapper.get_maps(
            {
                "Q1": self.RESPONSE_GROUPS[0],
                "Q4": self.RESPONSE_GROUPS[2],
                "Q8": self.RESPONSE_GROUPS[1],
                "Q9": self.RESPONSE_GROUPS[1],
            }
        )

        self.detector = ProblemReportDetector(
            name=self.type(),
            text_column_labels=self.text_column_labels,
            rating_column_labels=self.rating_column_labels,
            rating_map=rating_map,
        )

    @property
    def operation(self) -> str:
        return self._operation

    def enrich(self, df: DataFrame, resources: TransformResource = None) -> EnrichmentResult:
        detector = self.detector
        detector.apply(df)

        return EnrichmentResult(
            labels=[detector.text_label, detector.score_label],
            sort=(detector.score_label, False),
        )

    @property
    def labels(self) -> List[str]:
        return [self.detector.score_label, self.detector.text_label]

    @staticmethod
    def type() -> str:
        return "ProblemReport"

    @staticmethod
    def description() -> List[str]:
        return [
            "problem reports in ",
            "{text_column_labels}",
            " with ratings from ",
            "{rating_column_labels}",
        ]

    @staticmethod
    def parameters() -> List[Parameter]:
        return [
            TextListParameter(
                name="text_column_labels",
                label="text columns",
                example="Written text",
            ),
            TextListParameter(
                name="rating_column_labels",
                label="rating columns",
                example="On a scale from 1 to 5",
            ),
        ]

    def __repr__(self):
        text_columns = ",".join(sorted(self.text_column_labels))
        rating_columns = ",".join(sorted(self.rating_column_labels))
        return "{}:{}:{}".format(self.type(), text_columns, rating_columns)

    def serialize(self) -> List[str]:
        return [self.type(), self.operation, self.text_column_labels, self.rating_column_labels]

    @classmethod
    def deserialize(cls, lst: List) -> ProblemReport:
        assert cls.type() == lst[0]
        return cls(
            operation=lst[1],
            text_column_labels=lst[2],
            rating_column_labels=lst[3],
        )

'''
@register
class Categorization(EnrichmentTransform):
    sample_categories = {
        "passports": [
            "application",
            "renewal",
            "forms",
            "process",
            "visas",
            "help",
            "vacation",
            "travel",
            "wizard",
            "expiration",
            "post office",
        ],
        "corona": [
            "stimulus",
            "check",
            "outbreak",
            "local",
            "concern",
            "task force",
            "closed",
            "businesses",
        ],
        "voting": [
            "elections",
            "registration",
            "officials",
            "selective service",
            "absentee",
            "license",
        ],
        "jobs": [
            "small business",
            "fraud",
            "scams",
            "applications",
            "unemployment",
            "compensation",
            "benefits",
        ],
        "money": [
            "unclaimed",
            "credit report",
            "social security",
            "fraud",
            "scam",
            "phish",
        ],
        "web": [
            "site",
            "link",
            "page",
            ".gov",
            "survey",
            "online",
        ],
        "license": [
            "marriage",
            "birth",
            "replacement",
            "birth certificate",
            "social security card",
            "replacement",
        ],
        "tax": [
            "refund",
            "direct deposit",
            "state",
            "website",
            "federal tax refund",
            "clear instruction",
            "credit card",
            "consumer action",
            "low income",

        ]
    }

    LABEL_CATEGORY = "autocat1"

    def __init__(
        self,
        name: str,
        operation: str,
        column_label: str,
    ):
        self.name = name
        self.column_label = column_label
        self._operation = operation

    @property
    def operation(self) -> str:
        return self._operation

    @staticmethod
    def type() -> str:
        return "Autocat"

    def enrich(self, df: DataFrame, resources: TransformResource = None) -> EnrichmentResult:
        column_label = self.column_label

        text = df[column_label]

        name = self.name or self.LABEL_CATEGORY

        df[name] = text.apply(self.count_words, args=(self.sample_categories,))

        resources.dataset_id

        return EnrichmentResult(labels=[self.LABEL_CATEGORY])

    @classmethod
    def count_words(cls, text: str, categories: Dict[str, List[str]]):
        misc = "misc"
        text = text.lower()

        category_words = list(categories.keys())
        category_counts = [
            (text.count(word), word) for word in category_words
        ]

        best_pair = sorted(category_counts, reverse=True)[0]
        if best_pair[0] == 0:
            return "misc / misc"
        else:
            category = best_pair[1]

        subcategory_words = categories[category]

        subcategory_counts = [
            (text.count(word), word) for word in subcategory_words
        ]

        best_pair = sorted(subcategory_counts, reverse=True)[0]
        if best_pair[0] == 0:
            subcategory = misc
        else:
            subcategory = best_pair[1]
        return f"{category} / {subcategory}"

    @property
    def labels(self) -> List[str]:
        return [self.name]

    def __repr__(self) -> str:
        return "{}:{}".format(self.type(), self.column_label)

    @staticmethod
    def description() -> List[str]:
        return ["Autocat(", "{column_label}", ")"]

    @staticmethod
    def parameters() -> List[Parameter]:
        return [
            ColumnNameParameter(name="column_label", label="column", example="Comments"),
        ]

    def serialize(self) -> List:
        return [self.type(), self.operation, self.column_label]

    @classmethod
    def deserialize(cls, lst: List) -> Categorization:
        assert cls.type() == lst[0]
        operation = lst[1]
        column_label = lst[2]

        try:
            name = lst[3]
        except IndexError:
            name = cls.LABEL_CATEGORY

        return cls(
            column_label=column_label,
            operation=operation,
            name=name,
        )
'''


@register
class HasTag(FilterTransform):
    def __init__(self, tag: str, operation: str):
        self.tag = str(tag)
        self._operation = operation

    @property
    def operation(self) -> str:
        return self._operation

    def filter(self, df: DataFrame, resources: TransformResource = None) -> bool:
        tags_by_key = resources.tag.get_tags_by_key
        primary_key_name = resources.tag.primary_key_name
        tag = self.tag

        def has_tag(key):
            return tag in tags_by_key(key)

        return df[df[primary_key_name].apply(has_tag)]

    def __repr__(self) -> str:
        return "{}:{}".format(self.type(), self.tag)

    @staticmethod
    def type() -> str:
        return "HasTag"

    @staticmethod
    def description() -> List[str]:
        return ["has tag ", "{tag}"]

    @staticmethod
    def parameters() -> List[Parameter]:
        return [
            TextParameter(name="tag", label="tag", example="pending"),
        ]

    def serialize(self) -> List[str]:
        return [self.type(), self.operation, self.tag]

    @classmethod
    def deserialize(cls, lst: List) -> HasTag:
        assert cls.type() == lst[0]
        return cls(operation=lst[1], tag=lst[2])


@register
class Tag(EnrichmentTransform):
    TAG_COLUMN_LABEL = "tag"

    def __init__(
        self,
        operation: str,
        primary_key_column_label: str,
    ):
        self._operation = operation
        self.primary_key_column_label = primary_key_column_label

    @staticmethod
    def type() -> str:
        return "Tag"

    @property
    def operation(self) -> str:
        return self._operation

    def enrich(self, df: DataFrame, resources: TransformResource = None) -> EnrichmentResult:
        primary_key_column_label = self.primary_key_column_label

        if not resources.tag:
            resources.tag_handler.create(
                dataset_id=resources.dataset_id,
                primary_key_name=primary_key_column_label,
            )

        return EnrichmentResult(labels=[self.TAG_COLUMN_LABEL])

    @property
    def labels(self) -> List[str]:
        return [self.TAG_COLUMN_LABEL]

    @staticmethod
    def description() -> List[str]:
        return [
            "tag on ",
            "{primary_key_column_label}",
        ]

    @staticmethod
    def parameters() -> List[Parameter]:
        return [
            ColumnNameParameter(
                name="primary_key_column_label",
                label="unique row identifier",
                example="ResponseId",
            ),
        ]

    def __repr__(self):
        return f"{self.type()}:{self.primary_key_column_label}"

    def serialize(self) -> List:
        return [self.type(), self.operation, self.primary_key_column_label]

    @classmethod
    def deserialize(cls, lst: List) -> Tag:
        assert cls.type() == lst[0]
        operation = lst[1]
        primary_key_column_label = lst[2]

        return cls(
            primary_key_column_label=primary_key_column_label,
            operation=operation,
        )


@register
class ExtractNth(EnrichmentTransform):
    DEFAULT_SEP = SPACE

    def __init__(
        self,
        position: int,
        separator: str,
        name: str,
        column_label: str,
        operation: str,
    ):
        self.position = position
        self.separator = separator
        self.name = name
        self.column_label = column_label
        self._operation = operation

    @property
    def operation(self) -> str:
        return self._operation

    @staticmethod
    def type() -> str:
        return "ExtractNth"

    def enrich(self, df: DataFrame, resources: TransformResource = None) -> EnrichmentResult:
        name = self.name
        column_label = self.column_label
        index = self.position - 1
        separator = self.separator

        '''
        def extract_nth(string):
            try:
                return string.split(separator)[index]
            except IndexError:
                return ""

        df[name] = df[column_label].apply(extract_nth)
        '''

        def extract_nth(series):
            string = str(series[column_label])
            try:
                return string.split(separator)[index]
            except IndexError:
                return ""

        df[name] = df.T.apply(extract_nth)

        return EnrichmentResult(labels=[name])

    @property
    def labels(self) -> List[str]:
        return [self.name]

    def __repr__(self) -> str:
        return f"{self.type()}:{self.position}:{self.separator}:{self.column_label}:{self.name}"

    @staticmethod
    def description() -> List[str]:
        return ["Extract instance ", "{pos}", " from ", "{column_label}"]

    @staticmethod
    def parameters() -> List[Parameter]:
        return [
            IntegerParameter(
                name="position", label="position", example='"1" to indicate the first instance',
            ),
            TextParameter(name="separator", label="separator", example=","),
            TextParameter(name="name", label="name", example="FirstUrl"),
            ColumnNameParameter(name="column_label", label="column", example="History"),
        ]

    def serialize(self) -> List:
        return [
            self.type(),
            self.operation,
            self.position,
            self.separator,
            self.column_label,
            self.name,
        ]

    @classmethod
    def deserialize(cls, lst: List) -> ExtractNth:
        assert cls.type() == lst[0]
        operation = lst[1]
        position = int(lst[2])
        separator = str(lst[3])
        name = str(lst[4])
        column_label = str(lst[5])

        return cls(
            position=position,
            separator=separator,
            name=name,
            column_label=column_label,
            operation=operation,
        )


@register
class MatchingColumns(FilterTransform):
    DEFAULT_SEP = SPACE

    def __init__(
        self,
        column_label_i: str,
        column_label_j: str,
        operation: str,
    ):
        self.column_label_i = column_label_i
        self.column_label_j = column_label_j
        self._operation = operation

    @property
    def operation(self) -> str:
        return self._operation

    @staticmethod
    def type() -> str:
        return "MatchingColumns"

    def filter(self, df: DataFrame, resources: TransformResource = None) -> EnrichmentResult:
        return df[df[self.column_label_i] == df[self.column_label_j]]

    @property
    def labels(self) -> List[str]:
        return [self.name]

    def __repr__(self) -> str:
        return f"{self.type()}:{self.column_label_i}:{self.column_label_j}"

    @staticmethod
    def description() -> List[str]:
        return ["ColumnMatch", "{column_label_i}", " == ", "{column_label_j}"]

    @staticmethod
    def parameters() -> List[Parameter]:
        return [
            ColumnNameParameter(name="column_label_i", label="column1", example="Q8"),
            ColumnNameParameter(name="column_label_j", label="column2", example="Q9"),
        ]

    def serialize(self) -> List:
        return [
            self.type(),
            self.operation,
            self.column_label_i,
            self.column_label_j,
        ]

    @classmethod
    def deserialize(cls, lst: List) -> MatchingColumns:
        assert cls.type() == lst[0]
        operation = lst[1]
        column_label_i = str(lst[2])
        column_label_j = str(lst[3])

        return cls(
            column_label_i=column_label_i,
            column_label_j=column_label_j,
            operation=operation,
        )
