from analyzer.constraint_lib import (
    TransformList, ExactMatch, HasText, DoesNotMatchAny
)

import logging

logging.basicConfig(level=logging.DEBUG)

log = logging.getLogger(__name__)


def test_comparison():
    tx1 = ExactMatch("aaa", "bbb")
    tx2 = ExactMatch("aaa", "bbb")
    tx3 = ExactMatch("ccc", "ddd")
    tx4 = DoesNotMatchAny("aaa", ["bbb"]),
    tx5 = DoesNotMatchAny("aaa", ["bbb", "ccc"])
    assert tx1 == tx1
    assert tx1 == tx2
    assert tx1 != tx3
    assert tx1 != tx4
    assert tx1 != tx5

    assert tx2 == tx2
    assert tx2 != tx3
    assert tx2 != tx4
    assert tx2 != tx5

    assert tx3 == tx3
    assert tx3 != tx4
    assert tx3 != tx5

    assert tx4 != tx3
    assert tx4 == tx4
    assert tx5 == tx5
    
    
def test_set_comparison():
    transforms = TransformList(
        [
            ExactMatch("aaa", "bbb"),
            ExactMatch("aaa", "bbb"),
            HasText("aaa", "bbb"),
            DoesNotMatchAny("aaa", ["bbb", "ccc"]),
        ]
    )

    assert {transforms[0]} == {transforms[0]}
    assert {transforms[0], transforms[2]} == {transforms[0], transforms[2]}
    assert {transforms[0], transforms[2]} == {transforms[1], transforms[2]}
    assert {transforms[0], transforms[1]} == {transforms[0]}
    assert {transforms[0], transforms[2]} != {transforms[0], transforms[3]}
    assert {transforms[0]} != {transforms[2]}
    assert {transforms[0]} != {transforms[3]}
