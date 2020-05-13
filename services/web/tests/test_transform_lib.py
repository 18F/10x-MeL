from analyzer.constraint_lib import (
    ExactMatch, MatchAny, DoesNotMatch, DoesNotMatchAny, HasText, DoesNotHaveText,
    Parameter, TransformList,
)

import logging

logging.basicConfig(level=logging.DEBUG)
log = logging.getLogger(__name__)


class TestParameters:
    parameter_data = [
        [Parameter.TYPE_TEXT, "key", "column", "Country"],
        [Parameter.TYPE_TEXT_LIST, "values", "values", "MD,NY,NJ"],
    ]

    def test_parameter_attributes(self):
        data0, data1 = self.parameter_data
        parameter0 = Parameter(data0[0], data0[1], data0[2], data0[3])
        parameter1 = Parameter(data1[0], data1[1], data1[2], data1[3])

        assert parameter0.type == data0[0]
        assert parameter0.name == data0[1]
        assert parameter0.label == data0[2]
        assert parameter0.example == data0[3]

        assert parameter1.type == data1[0]
        assert parameter1.name == data1[1]
        assert parameter1.label == data1[2]
        assert parameter1.example == data1[3]

    def test_parameter_serialization(self):
        data0, data1 = self.parameter_data
        parameter0_base = Parameter(data0[0], data0[1], data0[2], data0[3])
        parameter1_base = Parameter(data1[0], data1[1], data1[2], data1[3])
        parameter0 = Parameter.deserialize(parameter0_base.serialize())
        parameter1 = Parameter.deserialize(parameter1_base.serialize())

        assert parameter0.type == data0[0]
        assert parameter0.name == data0[1]
        assert parameter0.label == data0[2]
        assert parameter0.example == data0[3]

        assert parameter1.type == data1[0]
        assert parameter1.name == data1[1]
        assert parameter1.label == data1[2]
        assert parameter1.example == data1[3]


class TestTransforms:
    transform_data = [
        ["foo", "FOO"],
        ["bar", [1, 3, 5]],
    ]

    def test_transform_attributes(self):
        data0, data1 = self.transform_data

        for transform_cls in [
            ExactMatch, DoesNotMatch, HasText, DoesNotHaveText,
        ]:
            transform = transform_cls(key=data0[0], value=data0[1])

            assert transform.key == data0[0]
            assert transform.key != data0[0] + "x"
            assert transform.value == data0[1]
            assert transform.value != []

        for transform_cls in [
            MatchAny, DoesNotMatchAny,
        ]:
            transform = transform_cls(key=data1[0], values=data1[1])

            assert transform.key == data1[0]
            assert transform.key != data1[0] + "x"
            assert transform.values == data1[1]
            assert transform.values != []

    def test_transform_deserialization(self):
        data0, data1 = self.transform_data
        match_any0 = MatchAny(key=data1[0], values=data1[1])
        match_any1 = MatchAny.deserialize(match_any0.serialize())

        assert match_any1.key == data1[0]
        assert match_any1.key != data1[0] + "x"
        assert match_any1.values == data1[1]
        assert match_any1.values != []


def test_comparison():
    tx1 = ExactMatch("aaa", "bbb")
    tx2 = ExactMatch("aaa", "bbb")
    tx3 = ExactMatch("ccc", "ddd")
    tx4 = DoesNotMatchAny("aaa", ["bbb"]),
    tx5 = DoesNotMatchAny("aaa", ["bbb", "ccc"])
    tx6 = DoesNotMatchAny("aaa", ["bbb"]),
    assert tx1 == tx1
    assert tx1 == tx2
    assert tx1 != tx3
    assert tx1 != tx4
    assert tx1 != tx5
    assert tx1 != tx6

    assert tx2 == tx2
    assert tx2 != tx3
    assert tx2 != tx4
    assert tx2 != tx5
    assert tx2 != tx6

    assert tx3 == tx3
    assert tx3 != tx4
    assert tx3 != tx5
    assert tx3 != tx6

    assert tx4 != tx3
    assert tx4 == tx4
    assert tx4 != tx5
    assert tx4 == tx6
    assert tx5 == tx5
    assert tx5 != tx6


def test_set_comparison():
    t = [
        ExactMatch("aaa", "bbb"),
        ExactMatch("aaa", "bbb"),
        HasText("aaa", "bbb"),
        DoesNotMatchAny("aaa", ["bbb", "ccc"]),
    ]

    lists = [
        TransformList([t[0], t[2]]),
        TransformList([t[1], t[2]]),
        TransformList([t[0], t[3]]),
        TransformList([t[0]]),
        TransformList([]),
    ]

    sets = [set(transform_list) for transform_list in lists]

    assert sets[0] == sets[0]
    assert sets[0] == sets[1]
    assert sets[0] != sets[2]
    assert sets[0] != sets[3]
    assert sets[0] != sets[4]

    assert sets[1] == sets[1]
    assert sets[1] != sets[2]
    assert sets[1] != sets[3]
    assert sets[1] != sets[4]

    assert sets[2] == sets[2]
    assert sets[2] != sets[3]
    assert sets[2] != sets[4]

    assert sets[3] == sets[3]
    assert sets[3] != sets[4]

    assert sets[4] == sets[4]
