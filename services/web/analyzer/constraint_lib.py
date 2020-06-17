from __future__ import annotations
from abc import ABC, abstractmethod

from typing import List, Set, Dict, Tuple, Iterable, Union, Type, Optional
from time import time
from collections import defaultdict, deque
from datetime import datetime
import logging
import json
import pandas as pd

from analyzer.utils import Serializable, SerializableType
from analyzer.contrib.problem_detector import (
    ProblemReportDetector,
    ResponseMapper,
)
from analyzer.transforms.enrichments_lib import TagHandler, TagMap, DatasetId
from analyzer.contrib.autocat_lib import autocat_handler

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
    TYPE_COLUMN_NAME_LIST = "column_name_list"
    TYPE_DATE_RANGE = "date_range"
    TYPE_DATE_RANGE_LIST = "date_range_list"

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


class ColumnNameListParameter(Parameter):
    def __init__(self, name: str, label: str, example: str):
        super().__init__(Parameter.TYPE_COLUMN_NAME_LIST, name, label, example)


class DateRangeParameter(Parameter):
    def __init__(self, name: str, label: str, example: str):
        super().__init__(Parameter.TYPE_DATE_RANGE, name, label, example)


class DateRangeListParameter(Parameter):
    def __init__(self, name: str, label: str, example: str):
        super().__init__(Parameter.TYPE_DATE_RANGE_LIST, name, label, example)


class TransformDef(Serializable):
    KEY_TYPE = "type"
    KEY_DESC = "description"
    KEY_PARAMETERS = "params"
    KEY_OPS = "ops"

    OPERATION_FILTER = "filter"
    OPERATION_ENRICH = "enrich"

    def __init__(
        self,
        transform_type: str,
        description: List[str],
        parameters: List[Parameter],
        operations: List[str],
    ):
        self.type = transform_type
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
        return TransformDef(
            transform_type=d[cls.KEY_TYPE],
            description=d[cls.KEY_DESC],
            parameters=[p.deserialize() for p in d[cls.KEY_PARAMETERS]],
            operations=[op for op in d[cls.KEY_OPS]],
        )


class TransformTree:
    def __init__(self):
        self._node_by_transform: Dict[Transform, TransformNode] = {}

    def add_node(self, transform: Transform):
        self._node_by_transform[transform] = TransformNode(transform)

    def add_edge(self, parent_transform: Transform, child_transform: Transform):
        parent_node = self._node_by_transform[parent_transform]
        child_node = self._node_by_transform[child_transform]
        parent_node.add_child(child_node)
        child_node.add_parent(parent_node)

    def get_parents_of_transform(self, transform: Transform) -> Set[Transform]:
        return {node.transform for node in self._node_by_transform[transform].parent_nodes}

    def get_children_of_transform(self, transform: Transform) -> Set[Transform]:
        return {node.transform for node in self._node_by_transform[transform].child_nodes}

    @classmethod
    def from_transform_list(cls, transforms: TransformList) -> TransformTree:
        tree = cls()

        # add each transform to the tree
        for transform in transforms:
            tree.add_node(transform)

        # get a mapping of all output labels to the transform that produces them
        transform_by_label = {}
        for transform in transforms:
            if isinstance(transform, EnrichmentTransform):
                for label in transform.output_labels:
                    assert label not in transform_by_label, f"DUPLICATE LABEL: {label}"
                    transform_by_label[label] = transform

        # get a mapping from each transform to the transform it depends on (its parents)
        for transform in transforms:
            for label in transform.input_labels:
                if label in transform_by_label:
                    parent_transform = transform_by_label[label]
                    tree.add_edge(parent_transform, transform)

        return tree


class TransformNode:
    def __init__(
        self,
        transform: Transform,
        parent_nodes: Optional[Set[TransformNode]] = None,
        child_nodes: Optional[Set[TransformNode]] = None,
    ):
        self.transform = transform
        self._parent_nodes = parent_nodes or set()
        self._child_nodes = child_nodes or set()

    def add_child(self, child_node: TransformNode):
        self._child_nodes.add(child_node)

    def add_parent(self, parent_node: TransformNode):
        self._parent_nodes.add(parent_node)

    @property
    def child_nodes(self) -> Set[TransformNode]:
        return self._child_nodes

    @property
    def parent_nodes(self) -> Set[TransformNode]:
        return self._parent_nodes


class TransformManager:
    operations = {TransformDef.OPERATION_FILTER, TransformDef.OPERATION_ENRICH}

    def __init__(self):
        self._transform_by_name: Dict[str, TransformType] = {}
        self._transforms_by_operation: Dict[str, List[TransformType]] = defaultdict(list)
        self._operations_by_transform: Dict[TransformType, List[str]] = defaultdict(list)

    def register(self, transform_cls: TransformType):
        self._transform_by_name[transform_cls.type()] = transform_cls

        for operation in self.operations:
            try:
                getattr(transform_cls, operation)
                self._transforms_by_operation[operation].append(transform_cls)
                self._operations_by_transform[transform_cls].append(operation)
            except AttributeError:
                pass

    def transform_by_name(self, name: str) -> TransformType:
        return self._transform_by_name[name]

    def get_transforms(self) -> Iterable[TransformType]:
        return self._transform_by_name.values()

    def get_transform_defs(self) -> Iterable[TransformDef]:
        for transform in self._transform_by_name.values():
            yield TransformDef(
                transform_type=transform.type(),
                description=transform.description(),
                parameters=transform.parameters(),
                operations=self._operations_by_transform[transform]
            )


# a singleton for transform class registration
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


class TransformList(Serializable, deque):
    def __init__(self, transforms: Optional[Union[List[Transform], TransformList]] = None):
        super().__init__(transforms or deque())

    def __eq__(self, other):
        if len(self) != len(other):
            return False
        for elem, elem_other in zip(self, other):
            if elem != elem_other:
                return False
        return True

    def serialize(self) -> List[List[str]]:
        return [transform.serialize() for transform in self]

    @classmethod
    def deserialize(cls, lst: List[List[Union[str, int]]]) -> TransformList:
        return TransformList([Transform.deserialize(elem) for elem in lst])


class Transform(Serializable, ABC):
    KEY_TYPE = "type"
    KEY_DESC = "description"
    KEY_PARAMETERS = "params"

    def __init__(self, operation: str):
        self._operation = operation

    @staticmethod
    @abstractmethod
    def type() -> str:
        pass

    def __eq__(self, other):
        try:
            return self.__dict__ == other.__dict__
        except AttributeError:
            return False

    @property
    @abstractmethod
    def input_labels(self) -> Set[str]:
        pass

    @property
    def operation(self) -> str:
        return self._operation

    @staticmethod
    @abstractmethod
    def description() -> List[str]:
        pass

    @staticmethod
    @abstractmethod
    def parameters() -> List[Parameter]:
        pass

    @abstractmethod
    def __repr__(self) -> str:
        pass

    def __hash__(self) -> int:
        return hash(json.dumps(self.serialize()))

    @abstractmethod
    def serialize(self) -> Dict:
        pass

    @classmethod
    def deserialize(cls, lst: List) -> Transform:
        transform_cls = transform_manager.transform_by_name(lst[0])
        return transform_cls.deserialize(lst)


class FilterTransform(Transform, ABC):
    @abstractmethod
    def filter(self, df: DataFrame, resources: TransformResource = None) -> bool:
        pass

    @classmethod
    @abstractmethod
    def deserialize(cls, data):
        pass


class EnrichmentTransform(Transform, ABC):
    @abstractmethod
    def enrich(self, df: DataFrame, resources: TransformResource = None) -> EnrichmentResult:
        pass

    @classmethod
    @abstractmethod
    def deserialize(cls, data):
        pass

    @property
    @abstractmethod
    def output_labels(self) -> List[str]:
        pass


TransformType = Type[Transform]


@register
class ExactMatch(FilterTransform):
    KEY_COLUMN = "column"
    KEY_VALUE = "value"

    def __init__(self, column_name: str, value: Value, operation: str):
        self.column_name = column_name
        self.value = value
        super().__init__(operation)

    @property
    def input_labels(self) -> Set[str]:
        return {self.column_name}

    def filter(self, df: DataFrame, resources: TransformResource = None) -> bool:
        return df[df[self.column_name].astype(str) == self.value]

    def __repr__(self) -> str:
        return "{}:{}={}".format(self.type(), self.column_name, self.value)

    @staticmethod
    def type() -> str:
        return "ExactMatch"

    @staticmethod
    def description() -> List[str]:
        return ["{column_name}", " = ", "{value}"]

    @staticmethod
    def parameters() -> List[Parameter]:
        return [
            ColumnNameParameter(name="column_name", label="column", example="Country"),
            TextParameter(name="value", label="value", example="United States"),
        ]

    def serialize(self) -> List[str]:
        return [self.type(), self.operation, self.column_name, self.value]

    @classmethod
    def deserialize(cls, lst: List) -> ExactMatch:
        assert cls.type() == lst[0]
        return cls(
            operation=lst[1], column_name=lst[2], value=lst[3],
        )


@register
class MatchAny(FilterTransform):
    def __init__(self, column_name: str, values: List[Value], operation: str):
        self.column_name = column_name
        self.values = values
        super().__init__(operation)

    @property
    def input_labels(self) -> Set[str]:
        return {self.column_name}

    def filter(self, df: DataFrame, resources: TransformResource = None) -> bool:
        return df[df[self.column_name].isin(self.values)]

    def __repr__(self) -> str:
        return "{}:{}={}".format(self.type(), self.column_name, self.values)

    @staticmethod
    def type() -> str:
        return "MatchAny"

    @staticmethod
    def description() -> List[str]:
        return ["{column_name}", " in [", "{values}", "]"]

    @staticmethod
    def parameters() -> List[Parameter]:
        return [
            ColumnNameParameter(name="column_name", label="column", example="State"),
            TextListParameter(name="values", label="values", example="GA,ME,IL,WI"),
        ]

    def serialize(self) -> List[str]:
        return [self.type(), self.operation, self.column_name, self.values]

    @classmethod
    def deserialize(cls, lst: List) -> MatchAny:
        assert cls.type() == lst[0]
        return cls(operation=lst[1], column_name=lst[2], values=lst[3])


@register
class DoesNotMatch(FilterTransform):
    def __init__(self, column_name: str, value: Value, operation: str):
        self.column_name = column_name
        self.value = value
        super().__init__(operation)

    @property
    def input_labels(self) -> Set[str]:
        return {self.column_name}

    def filter(self, df: DataFrame, resources: TransformResource = None) -> bool:
        return df[df[self.column_name].astype(str) != self.value]

    def __repr__(self) -> str:
        return "{}:{}={}".format(self.type(), self.column_name, self.value)

    @staticmethod
    def type() -> str:
        return "DoesNotMatch"

    @staticmethod
    def description() -> List[str]:
        return ["{column_name}", " != ", "{value}"]

    @staticmethod
    def parameters() -> List[Parameter]:
        return [
            ColumnNameParameter(name="column_name", label="column", example="Country"),
            TextParameter(name="value", label="value", example="United States"),
        ]

    def serialize(self) -> List[str]:
        return [self.type(), self.operation, self.column_name, self.value]

    @classmethod
    def deserialize(cls, lst: List) -> DoesNotMatch:
        assert cls.type() == lst[0]
        return cls(operation=lst[1], column_name=lst[2], value=lst[3])


@register
class DoesNotMatchAny(FilterTransform):
    def __init__(self, column_name: str, values: List[Value], operation: str):
        self.column_name = column_name
        self.values = values
        super().__init__(operation)

    @property
    def input_labels(self) -> Set[str]:
        return {self.column_name}

    def filter(self, df: DataFrame, resources: TransformResource = None) -> bool:
        return df[~df[self.column_name].isin(self.values)]

    def __repr__(self) -> str:
        return "{}:{}={}".format(self.type(), self.column_name, self.values)

    @staticmethod
    def type() -> str:
        return "DoesNotMatchAny"

    @staticmethod
    def description() -> List[str]:
        return ["{column_name}", " not in [", "{values}", "]"]

    @staticmethod
    def parameters() -> List[Parameter]:
        return [
            ColumnNameParameter(name="column_name", label="column", example="State"),
            TextListParameter(name="values", label="values", example="GA,ME,IL,WI"),
        ]

    def serialize(self) -> List[str]:
        return [self.type(), self.operation, self.column_name, self.values]

    @classmethod
    def deserialize(cls, lst: List) -> DoesNotMatchAny:
        assert cls.type() == lst[0]
        return cls(operation=lst[1], column_name=lst[2], values=lst[3])


@register
class HasText(FilterTransform):
    def __init__(self, column_name: str, value: str, operation: str):
        self.column_name = column_name
        self.value = str(value)
        super().__init__(operation)

    @property
    def input_labels(self) -> Set[str]:
        return {self.column_name}

    def filter(self, df: DataFrame, resources: TransformResource = None) -> bool:
        return df[df[self.column_name].astype(str).str.lower().str.contains(self.value.lower())]

    def __repr__(self) -> str:
        return "{}:{}={}".format(self.type(), self.column_name, self.value)

    @staticmethod
    def type() -> str:
        return "HasText"

    @staticmethod
    def description() -> List[str]:
        return ["{column_name}", " contains ", "{value}"]

    @staticmethod
    def parameters() -> List[Parameter]:
        return [
            ColumnNameParameter(name="column_name", label="column", example="Comments"),
            TextParameter(name="value", label="value", example="help"),
        ]

    def serialize(self) -> List[str]:
        return [self.type(), self.operation, self.column_name, self.value]

    @classmethod
    def deserialize(cls, lst: List) -> HasText:
        assert cls.type() == lst[0]
        return cls(operation=lst[1], column_name=lst[2], value=lst[3])


@register
class DoesNotHaveText(FilterTransform):
    def __init__(self, column_name: str, value: str, operation: str):
        self.column_name = column_name
        self.value = str(value)
        super().__init__(operation)

    @property
    def input_labels(self) -> Set[str]:
        return {self.column_name}

    def filter(self, df: DataFrame, resources: TransformResource = None) -> bool:
        return df[~df[self.column_name].astype(str).str.lower().str.contains(self.value.lower())]

    def __repr__(self) -> str:
        return "{}:{}={}".format(self.type(), self.column_name, self.value)

    @staticmethod
    def type() -> str:
        return "DoesNotHaveText"

    @staticmethod
    def description() -> List[str]:
        return ["{column_name}", " does not contain ", "{value}"]

    @staticmethod
    def parameters() -> List[Parameter]:
        return [
            ColumnNameParameter(name="column_name", label="column", example="email"),
            TextParameter(name="value", label="value", example=".gov"),
        ]

    def serialize(self) -> List[str]:
        return [self.type(), self.operation, self.column_name, self.value]

    @classmethod
    def deserialize(cls, lst: List) -> DoesNotHaveText:
        assert cls.type() == lst[0]
        return cls(operation=lst[1], column_name=lst[2], value=lst[3])


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
        new_column_label: str,
        column_labels: List[str],
        operation: str,
    ):
        self.new_column_label = new_column_label
        self.column_labels = column_labels
        super().__init__(operation)

    @staticmethod
    def type() -> str:
        return "MergeColumnText"

    @property
    def input_labels(self) -> Set[str]:
        return set(self.column_labels)

    @property
    def output_labels(self) -> List[str]:
        return [self.new_column_label]

    def enrich(self, df: DataFrame, resources: TransformResource = None) -> EnrichmentResult:
        new_column_label = self.new_column_label
        column_labels = self.column_labels

        # choosing a character that will never occur in input
        null_sep = "\x01"

        df[new_column_label] = df[column_labels[0]].astype(str).str.strip()
        for column_label in column_labels[1:]:
            df[new_column_label] += null_sep + df[column_label].astype(str).str.strip()

        all_sep = "^[{sep}]+$".format(sep=null_sep)
        start_sep = "^[{sep}]+".format(sep=null_sep)
        end_sep = "[{sep}]+$".format(sep=null_sep)
        repeated_sep = "[{sep}]+".format(sep=null_sep)

        df[new_column_label] = df[new_column_label].str.replace(all_sep, "")
        df[new_column_label] = df[new_column_label].str.replace(start_sep, "")
        df[new_column_label] = df[new_column_label].str.replace(end_sep, "")
        df[new_column_label] = df[new_column_label].str.replace(repeated_sep, SPACE)

        return EnrichmentResult(labels=[new_column_label])

    def __repr__(self) -> str:
        return "{}:{}".format(self.type(), COMMA.join(self.column_labels))

    @staticmethod
    def description() -> List[str]:
        return ["Merge(", "{column_labels}", ") as ", "{new_column_label}"]

    @staticmethod
    def parameters() -> List[Parameter]:
        return [
            TextParameter(name="new_column_label", label="name", example="MyText"),
            ColumnNameListParameter(name="column_labels", label="columns", example="Q1,Q4,Q7"),
        ]

    def serialize(self) -> List:
        return [
            self.type(),
            self.operation,
            self.new_column_label,
            self.column_labels,
        ]

    @classmethod
    def deserialize(cls, lst: List) -> MergeColumnText:
        assert cls.type() == lst[0]
        operation = lst[1]
        new_column_label = lst[2]
        column_labels = lst[3]

        return cls(
            new_column_label=new_column_label,
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
        text_column_label: str,
        rating_column_labels: List[str],
    ):
        self.text_column_label = text_column_label
        self.rating_column_labels = rating_column_labels
        super().__init__(operation)

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
            text_column_label=self.text_column_label,
            rating_column_labels=self.rating_column_labels,
            rating_map=rating_map,
        )

    @property
    def input_labels(self) -> Set[str]:
        deps = {self.text_column_label}
        deps.update(self.rating_column_labels)
        return deps

    def enrich(self, df: DataFrame, resources: TransformResource = None) -> EnrichmentResult:
        detector = self.detector
        detector.apply(df)

        return EnrichmentResult(
            labels=[detector.text_label, detector.score_label],
            sort=(detector.score_label, False),
        )

    @property
    def output_labels(self) -> List[str]:
        return [self.detector.score_label, self.detector.text_label]

    @staticmethod
    def type() -> str:
        return "ProblemReport"

    @staticmethod
    def description() -> List[str]:
        return [
            "problem reports in ",
            "{text_column_label}",
            " with ratings from ",
            "{rating_column_labels}",
        ]

    @staticmethod
    def parameters() -> List[Parameter]:
        return [
            ColumnNameParameter(
                name="text_column_label",
                label="text column",
                example="Column containing written text",
            ),
            TextListParameter(
                name="rating_column_labels",
                label="rating columns",
                example="On a scale from 1 to 5",
            ),
        ]

    def __repr__(self):
        rating_columns = ",".join(sorted(self.rating_column_labels))
        return "{}:{}:{}".format(self.type(), self.text_column_label, rating_columns)

    def serialize(self) -> List[str]:
        return [self.type(), self.operation, self.text_column_label, self.rating_column_labels]

    @classmethod
    def deserialize(cls, lst: List) -> ProblemReport:
        assert cls.type() == lst[0]
        return cls(
            operation=lst[1],
            text_column_label=lst[2],
            rating_column_labels=lst[3],
        )


@register
class Categorization(EnrichmentTransform):
    LABEL_CATEGORY = "autocat1"

    def __init__(
        self,
        new_column_name: str,
        text_column_name: str,
        date_column_name: str,
        pkey_column_name: str,
        operation: str,
    ):
        self.new_column_name = new_column_name
        self.text_column_name = text_column_name
        self.date_column_name = date_column_name
        self.pkey_column_name = pkey_column_name
        super().__init__(operation)

    @staticmethod
    def type() -> str:
        return "Autocat"

    def input_labels(self) -> Set[str]:
        return {self.text_column_name, self.date_column_name, self.pkey_column_name}

    @property
    def output_labels(self) -> List[str]:
        return [self.new_column_name]

    def enrich(self, df: DataFrame, resources: TransformResource = None) -> EnrichmentResult:
        new_column_name = self.new_column_name
        text_column_name = self.text_column_name
        date_column_name = self.date_column_name
        pkey_column_name = self.pkey_column_name

        def f(series):
            pairs = corpus_processor.categorize_text(
                text=series[text_column_name],
            )
            return SPACE.join(f"{cat1}/{cat2}" for cat1, cat2 in pairs)

        pkeys = list(df[self.pkey_column_name])

        try:
            entry_ids = autocat_handler.pkeys_to_entry_ids(pkeys)
        except ValueError:
            log.info("Loading corpus")
            autocat_handler.load_corpus(
                df=df,
                pkey_column_name=pkey_column_name,
                text_column_name=text_column_name,
                date_column_name=date_column_name,
            )

            entry_ids = autocat_handler.pkeys_to_entry_ids(pkeys)

        start = time()
        log.info(f"building model...")
        corpus_processor = autocat_handler.build_model(entry_ids)
        log.info(f"building model completed in {time() - start:6.2f}")

        log.info(f"applying...")
        start = time()
        df[new_column_name] = df.T.apply(f)
        log.info(f"applying completed in {time() - start:6.2f}")

        return EnrichmentResult(labels=[new_column_name])

    def __repr__(self) -> str:
        return ":".join(
            [
                self.type(),
                self.new_column_name,
                self.text_column_name,
                self.date_column_name,
                self.pkey_column_name,
            ]
        )

    @staticmethod
    def description() -> List[str]:
        return ["Autocat1(", "{text_column_name}", ")"]

    @staticmethod
    def parameters() -> List[Parameter]:
        return [
            TextParameter(name="new_column_name", label="new column name", example="autocat1"),
            ColumnNameParameter(name="text_column_name", label="text", example="Text, Q3"),
            ColumnNameParameter(name="date_column_name", label="date", example="StartDate"),
            ColumnNameParameter(
                name="pkey_column_label",
                label="unique row identifier",
                example="ResponseId",
            ),
        ]

    def serialize(self) -> List:
        return [
            self.type(),
            self.operation,
            self.new_column_name,
            self.text_column_name,
            self.date_column_name,
            self.pkey_column_name,
        ]

    @classmethod
    def deserialize(cls, lst: List) -> Categorization:
        assert cls.type() == lst[0]
        operation = lst[1]
        new_column_name = lst[2]
        text_column_name = lst[3]
        date_column_name = lst[4]
        pkey_column_name = lst[5]

        return cls(
            new_column_name=new_column_name,
            text_column_name=text_column_name,
            date_column_name=date_column_name,
            pkey_column_name=pkey_column_name,
            operation=operation,
        )


@register
class HasTag(FilterTransform):
    def __init__(self, tag: str, operation: str):
        self.tag = str(tag)
        super().__init__(operation)

    @property
    def input_labels(self) -> Set[str]:
        return {Tag.TAG_COLUMN_LABEL}

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
        primary_key_column_label: str,
        operation: str,
    ):
        self.primary_key_column_label = primary_key_column_label
        super().__init__(operation)

    @property
    def input_labels(self) -> Set[str]:
        return {self.primary_key_column_label}

    @staticmethod
    def type() -> str:
        return "Tag"

    def enrich(self, df: DataFrame, resources: TransformResource = None) -> EnrichmentResult:
        primary_key_column_label = self.primary_key_column_label

        if not resources.tag:
            resources.tag_handler.create(
                dataset_id=resources.dataset_id,
                primary_key_name=primary_key_column_label,
            )

        return EnrichmentResult(labels=[self.TAG_COLUMN_LABEL])

    @property
    def output_labels(self) -> List[str]:
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
        new_column_label: str,
        column_label: str,
        operation: str,
    ):
        self.position = position
        self.separator = separator
        self.new_column_label = new_column_label
        self.column_label = column_label
        super().__init__(operation)

    @property
    def input_labels(self) -> Set[str]:
        return {self.column_label}

    @staticmethod
    def type() -> str:
        return "ExtractNth"

    def enrich(self, df: DataFrame, resources: TransformResource = None) -> EnrichmentResult:
        new_column_label = self.new_column_label
        column_label = self.column_label
        if self.position == -1:
            index = -1
        else:
            index = self.position - 1
        separator = self.separator

        '''
        def extract_nth(string):
            try:
                return string.split(separator)[index]
            except IndexError:
                return ""

        df[new_column_label] = df[column_label].apply(extract_nth)
        '''

        def extract_nth(series):
            string = str(series[column_label])
            try:
                return string.split(separator)[index]
            except IndexError:
                return ""

        df[new_column_label] = df.T.apply(extract_nth)

        return EnrichmentResult(labels=[new_column_label])

    @property
    def output_labels(self) -> List[str]:
        return [self.new_column_label]

    def __repr__(self) -> str:
        return ":".join(
            [
                self.type(),
                str(self.position),
                self.separator,
                self.column_label,
                self.new_column_label
            ]
        )

    @staticmethod
    def description() -> List[str]:
        return ["Extract ", "{pos}", " from ", "{column_label}"]

    @staticmethod
    def parameters() -> List[Parameter]:
        return [
            ColumnNameParameter(name="column_label", label="column", example="History"),
            IntegerParameter(
                name="position", label="position", example='"1" to indicate the first instance',
            ),
            TextParameter(name="separator", label="separator", example=","),
            TextParameter(name="new_column_label", label="new column name", example="FirstUrl"),
        ]

    def serialize(self) -> List:
        return [
            self.type(),
            self.operation,
            self.column_label,
            self.position,
            self.separator,
            self.new_column_label,
        ]

    @classmethod
    def deserialize(cls, lst: List) -> ExtractNth:
        assert cls.type() == lst[0]
        operation = lst[1]
        column_label = str(lst[2])
        position = int(lst[3])
        separator = str(lst[4])
        new_column_label = str(lst[5])

        return cls(
            position=position,
            separator=separator,
            new_column_label=new_column_label,
            column_label=column_label,
            operation=operation,
        )


@register
class DateRange(FilterTransform):
    DATE_SEPARATOR = ":"

    def __init__(self, column_name: str, date_string: str, operation: str):
        self.column_name = column_name
        self.date_string = date_string
        super().__init__(operation)

    @classmethod
    def type(cls) -> str:
        return "DateRange"

    def filter(self, df: DataFrame, resources: TransformResource = None) -> bool:
        column_name = self.column_name
        start_string, end_string = self.date_string.split(self.DATE_SEPARATOR)
        start_dt = datetime.fromisoformat(start_string)
        end_dt = datetime.fromisoformat(end_string)
        return df[(df[column_name] >= start_dt) & (df[column_name] <= end_dt)]

    @property
    def input_labels(self) -> Set[str]:
        return {self.column_name}

    @staticmethod
    def description() -> List[str]:
        return ["DateRange ", "{date_string}"]

    @staticmethod
    def parameters() -> List[Parameter]:
        return [
            ColumnNameParameter(name="column_name", label="date_column", example="StartDate"),
            DateRangeParameter(name="date_string", label="date range", example=""),
        ]

    def __repr__(self) -> str:
        return f"{self.type()}:{self.column_name}:{self.date_string}"

    def serialize(self) -> List:
        return [
            self.type(),
            self.operation,
            self.column_name,
            self.date_string,
        ]

    @classmethod
    def deserialize(cls, lst: List) -> DateRange:
        assert cls.type() == lst[0]
        operation = lst[1]
        column_name = str(lst[2])
        date_string = str(lst[3])

        return cls(
            date_string=date_string,
            column_name=column_name,
            operation=operation,
        )


@register
class DateRanges(EnrichmentTransform):
    DATE_SEPARATOR = ":"

    def __init__(
        self,
        date_column_name: str,
        date_strings: List[str],
        new_column_name: str,
        operation: str,
    ):
        self.date_column_name = date_column_name
        self.date_strings = date_strings
        self.new_column_name = new_column_name
        super().__init__(operation)

        self._date_ranges = self._init_date_ranges(date_strings)

    @classmethod
    def type(cls) -> str:
        return "DateRanges"

    @classmethod
    def _init_date_ranges(cls, date_strings: List[str]) -> List[Tuple[datetime, datetime]]:
        date_strings = deque(date_strings)

        date_ranges = []
        for date_string in date_strings:
            start_date_string, end_date_string = date_string.split(cls.DATE_SEPARATOR)
            start_dt = datetime.fromisoformat(start_date_string)
            end_dt = datetime.fromisoformat(end_date_string)
            date_ranges.append((start_dt, end_dt))

        return date_ranges

    def enrich(self, df: DataFrame, resources: TransformResource = None) -> EnrichmentResult:
        date_column_name = self.date_column_name
        date_ranges = self._date_ranges
        new_column_name = self.new_column_name

        def f(series):
            target = series[date_column_name]
            for i, (start, end) in enumerate(date_ranges):
                if start <= target < end:
                    return i + 1
            return ""

        df[new_column_name] = df.T.apply(f)
        return EnrichmentResult(labels=[new_column_name])

    @property
    def input_labels(self) -> Set[str]:
        return {self.date_column_name}

    @property
    def output_labels(self) -> List[str]:
        return [self.new_column_name]

    @staticmethod
    def description() -> List[str]:
        return [
            "DateRanges ", "{date_strings}", " via ",
            "{date_column_name}", " as ", "{new_column_name}",
        ]

    @staticmethod
    def parameters() -> List[Parameter]:
        return [
            ColumnNameParameter(name="date_column_name", label="date column", example="StartDate"),
            TextParameter(name="new_column_name", label="new column name", example="DateRanges"),
            DateRangeListParameter(name="date_strings", label="date ranges", example=""),
        ]

    def __repr__(self) -> str:
        return ":".join(
            [
                self.type(),
                self.date_column_name,
                self.new_column_name,
                self.DATE_SEPARATOR.join(self.date_strings),
            ]
        )

    def serialize(self) -> List:
        return [
            self.type(),
            self.operation,
            self.date_column_name,
            self.new_column_name,
            self.date_strings,
        ]

    @classmethod
    def deserialize(cls, lst: List) -> DateRanges:
        assert cls.type() == lst[0]
        operation = lst[1]
        date_column_name = str(lst[2])
        new_column_name = str(lst[3])
        date_strings = lst[4]

        return cls(
            date_column_name=date_column_name,
            new_column_name=new_column_name,
            date_strings=date_strings,
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
        super().__init__(operation)

    @property
    def input_labels(self) -> Set[str]:
        return {self.column_label_i, self.column_label_j}

    @staticmethod
    def type() -> str:
        return "MatchingColumns"

    def filter(self, df: DataFrame, resources: TransformResource = None) -> bool:
        return df[df[self.column_label_i] == df[self.column_label_j]]

    @property
    def output_labels(self) -> List[str]:
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
