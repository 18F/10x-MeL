from analyzer.constraint_lib import (
    ConstraintList,
    ExactMatch, DoesNotMatch, MatchAny,
)
from analyzer.data_view.data_view_lib import (
    DataView, DataViewId, DatasetId, UserId,
    Label, LabelSet,
)

import logging

logging.basicConfig(level=logging.DEBUG)
log = logging.getLogger(__name__)


class TestLabels:
    def test_label_attributes(self):
        name0, width0, font_size0 = "foo", 100, 14

        label0 = Label(name=name0, width=width0, font_size=font_size0)

        assert label0.name == name0
        assert label0.width == width0
        assert label0.font_size == font_size0

        invalid_labels = {"", True, "xxx", 0, -1}
        assert label0.name not in invalid_labels
        assert label0.width not in invalid_labels
        assert label0.font_size not in invalid_labels

    def test_label_serialization(self):
        name0, width0, font_size0 = "foo", 100, 14

        label0_base = Label(name=name0, width=width0, font_size=font_size0)

        label0 = Label.deserialize(label0_base.serialize())

        assert label0.name == name0
        assert label0.width == width0
        assert label0.font_size == font_size0

        assert label0.name == label0_base.name
        assert label0.width == label0_base.width
        assert label0.font_size == label0_base.font_size

        invalid_labels = {"", True, "xxx", 0, -1}
        assert label0.name not in invalid_labels
        assert label0.width not in invalid_labels
        assert label0.font_size not in invalid_labels

    def test_label_set_serialization(self):
        name0, width0, font_size0 = "foo", 100, 14
        name1, width1, font_size1 = "bar", 200, 16
        name2, width2, font_size2 = "baz", 300, 18

        label0 = Label(name=name0, width=width0, font_size=font_size0)
        label1 = Label(name=name1, width=width1, font_size=font_size1)
        label2 = Label(name=name2, width=width2, font_size=font_size2)

        label_set0 = LabelSet([label0, label1, label2])
        assert label0 == label_set0[0]
        assert label1 == label_set0[1]
        assert label2 == label_set0[2]

        label_set0_copy = LabelSet.deserialize(label_set0.serialize())

        assert label0 == label_set0_copy[0]
        assert label1 == label_set0_copy[1]
        assert label2 == label_set0_copy[2]

        assert label0 != label_set0_copy[1]
        assert label1 != label_set0_copy[2]
        assert label2 != label_set0_copy[0]


class TestDataView:
    data_view_data = {
        "id": DataViewId(5),
        "parent_id": DataViewId(4),
        "dataset_id": DatasetId(20),
        "user_id": UserId(10),
    }

    label_data = [
        ["foo", 100, 14],
        ["bar", 200, 16],
        ["baz", 300, 18],
    ]

    transform_data = [
        [ExactMatch, "foo", "FOO"],
        [DoesNotMatch, "bar", "BAR"],
        [MatchAny, "baz", [1, 3, 5]],
    ]

    def get_label_set(self) -> LabelSet:
        data0, data1, data2 = self.label_data

        label0 = Label(name=data0[0], width=data0[1], font_size=data0[2])
        label1 = Label(name=data1[0], width=data1[1], font_size=data1[2])
        label2 = Label(name=data2[0], width=data2[1], font_size=data2[2])

        return LabelSet([label0, label1, label2])

    def get_transforms(self) -> ConstraintList:
        data0, data1, data2 = self.transform_data

        transform0 = data0[0](key=data0[1], value=data0[2])
        transform1 = data1[0](key=data1[1], value=data1[2])
        transform2 = data2[0](key=data2[1], values=data2[2])

        return ConstraintList([transform0, transform1, transform2])

    def test_data_view_attributes(self):
        data = self.data_view_data
        labels = self.get_label_set()
        transforms = self.get_transforms()

        data_view_id = data["id"]
        parent_data_view_id = data["parent_id"]
        dataset_id = data["dataset_id"]
        user_id = data["user_id"]

        data_view0 = DataView(
            data_view_id=data_view_id,
            parent_data_view_id=parent_data_view_id,
            dataset_id=dataset_id,
            user_id=user_id,
            labels=labels,
            transforms=transforms,
        )

        assert data_view0.id == data_view_id
        assert data_view0.parent_id == parent_data_view_id
        assert data_view0.dataset_id == dataset_id
        assert data_view0.user_id == user_id
        assert data_view0.labels == labels
        assert data_view0.transforms[0] == transforms[0]
        assert data_view0.transforms[1] == transforms[1]
        assert data_view0.transforms[2] == transforms[2]
        assert data_view0.transforms == transforms

    def test_data_view_serialization(self):
        data = self.data_view_data
        labels = self.get_label_set()
        transforms = self.get_transforms()

        data_view_id = data["id"]
        parent_data_view_id = data["parent_id"]
        dataset_id = data["dataset_id"]
        user_id = data["user_id"]

        data_view0_base = DataView(
            data_view_id=data_view_id,
            parent_data_view_id=parent_data_view_id,
            dataset_id=dataset_id,
            user_id=user_id,
            labels=labels,
            transforms=transforms,
        )

        data_view0 = DataView.deserialize(data_view0_base.serialize())

        assert data_view0.id == data_view_id
        assert data_view0.parent_id == parent_data_view_id
        assert data_view0.dataset_id == dataset_id
        assert data_view0.user_id == user_id
        assert data_view0.labels == labels
        assert data_view0.transforms == transforms
