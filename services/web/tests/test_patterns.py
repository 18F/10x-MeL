from __future__ import division

from itertools import izip_longest, chain
from collections import defaultdict

import pytest
import logging

from ev_common.models.phase import Phase
from ev_common.models.transcript.transcript_lib import TranscriptWord, TranscriptWordList

from services.web.patterns.surface_pattern_lib import make_wildcard
from ..phases.structured import make_phase
from ..phases.structured.surface_pattern_lib import make_wildcard
from ..phases.structured.context_lib import Context
from ..phases.structured.match_predicate_lib import (TextExtractor,
                                                     Repeat,
                                                     NotPrecededBy,
                                                     InOrder,
                                                     Text,
                                                     AnyOf)
from ..phases.structured.phase_builder_lib import (PhaseType,
                                                   IntroBuilder,
                                                   StructuredPhaseDetector,
                                                   phase_block)
from ..phases.structured.transcript_match_lib import TranscriptMatch

from . import MockTranscript, get_words_from_test_samples_xml, get_words_from_test_samples_json

logging.basicConfig(
    level=logging.DEBUG, filename="log/tests.log", format="%(message)s",
)

log = logging.getLogger(__name__)


@pytest.mark.unittest
def test_wildcards_maker():
    assert "([a-z']+ ){,3}" == make_wildcard(0, 3)
    assert "([a-z']+ ){1,4}" == make_wildcard(1, 4)
    assert "([a-z']+ ){,3}" == make_wildcard(0, 3, alpha=True, numeric=False)
    assert "([a-z']+ ){1,4}" == make_wildcard(1, 4, alpha=True, numeric=False)
    assert "([0-9]+ ){,3}" == make_wildcard(0, 3, alpha=False, numeric=True)
    assert "([0-9]+ ){1,4}" == make_wildcard(1, 4, alpha=False, numeric=True)
    assert "([a-z0-9']+ ){,3}" == make_wildcard(0, 3, alpha=True, numeric=True)
    assert "([a-z0-9']+ ){1,4}" == make_wildcard(1, 4, alpha=True, numeric=True)


@pytest.mark.unittest
def test_surface_pattern_construction_and_cleansing():
    assert ("(?<= )aaa ([a-z']+ ){,2}bbb(?= )", "") == TextExtractor("aaa {w2} bbb").patterns
    assert ("(?<= )aaa ([a-z']+ ){,2}bbb(?= )", "") == TextExtractor("aaa {w2}bbb").patterns

    assert TextExtractor("test1 {w3}test2").patterns == TextExtractor("test1 {w3} test2").patterns

    assert ("(?<= )aaa ([a-z']+ ){,2}bbb ([a-z']+ ){,1}ccc(?= )", "") == TextExtractor("aaa {w2} bbb {w1} ccc").patterns
    assert ("(?<= )aaa ([a-z']+ ){,2}bbb ([a-z']+ ){,1}ccc(?= )", "") == TextExtractor("aaa {w2} bbb {w1}ccc").patterns
    assert ("(?<= )aaa ([a-z']+ ){,2}bbb ([a-z']+ ){,1}ccc(?= )", "") == TextExtractor("aaa {w2}bbb {w1} ccc").patterns
    assert ("(?<= )aaa ([a-z']+ ){,2}bbb ([a-z']+ ){,1}ccc(?= )", "") == TextExtractor("aaa {w2}bbb {w1}ccc").patterns

    assert TextExtractor("test1 {w3}test2").patterns == TextExtractor("test1 {w3} test2").patterns


@pytest.mark.unittest
def test_surface_pattern1():
    text1 = " hello this is dave "
    text2 = " so i'm like hello this is 2018 "

    sp1 = TextExtractor("hello", "like hello", (10, 0))

    matches1 = list(sp1._matches(text1))
    matches2 = list(sp1._matches(text2))

    assert 1 == len(matches1)
    assert 0 == len(matches2)


@pytest.mark.unittest
def test_negation_block():
    text = "then again if they say oh hello how are you then it's hello right back"
    transcript = MockTranscript.from_string(text)

    hello = Text(TextExtractor("hello"))
    quoting = Text(TextExtractor("if they say"))
    hello_without_quoting = NotPrecededBy(hello, quoting, 5)

    expected1 = [("hello", 6, 7),
                 ("hello", 12, 13)]
    expected2 = [("hello", 12, 13)]

    context1 = Context(transcript)
    context2 = Context(transcript)

    matches1 = hello.matches(context1)
    matches2 = hello_without_quoting.matches(context2)

    assert len(expected1) == len(matches1)
    assert len(expected2) == len(matches2)

    sorted_matches1 = sorted(matches1, key=lambda m: (m.start, m.end, context1.text(*m)))
    sorted_matches2 = sorted(matches2, key=lambda m: (m.start, m.end, context2.text(*m)))

    for matches, expected in ((sorted_matches1, expected1), (sorted_matches2, expected2)):
        for match, (text, start, end) in izip_longest(matches, expected, fillvalue=None):
            assert isinstance(match, TranscriptMatch)
            assert text == context1.text(*match).strip()
            assert start == match.start
            assert end == match.end


@pytest.mark.unittest
def test_sequence_block():
    one = Text("one")
    two = Text("two")
    block = InOrder(one, two, durations=[1.5])

    text = "check one and two and one two check one wait wait wait two three again one two and three there"

    transcript = MockTranscript.from_string(text)
    context = Context(transcript)

    matches = block.matches(context)

    expected = [("one and two", 1, 4),
                ("one two", 5, 7),
                ("one two", 15, 17)]

    assert len(expected) == len(matches)

    sorted_matches = sorted(matches, key=lambda m: (m.start, m.end, context.text(*m)))

    for match, (text, start, end) in izip_longest(sorted_matches, expected):
        assert isinstance(match, TranscriptMatch)
        assert text == context.text(*match).strip()
        assert start == match.start
        assert end == match.end


@pytest.mark.unittest
def test_repeat_block1():
    yes = Text("yes")
    block = Repeat(yes, duration=2.5)

    text = "no yes no no no yes yes no no no yes no yes no no no yes no no yes no"

    transcript = MockTranscript.from_string(text)
    context = Context(transcript)

    matches = block.matches(context)

    expected = [("yes", 1, 2),
                ("yes yes", 5, 7),
                ("yes", 6, 7),
                ("yes no yes", 10, 13),
                ("yes", 12, 13),
                ("yes no no yes", 16, 20),
                ("yes", 19, 20),
    ]

    assert len(expected) == len(matches)

    sorted_matches = sorted(matches, key=lambda m: (m.start, m.end, context.text(*m)))

    for match, (text, start, end) in izip_longest(sorted_matches, expected):
        assert isinstance(match, TranscriptMatch)
        assert text == context.text(*match).strip()
        assert start == match.start
        assert end == match.end


@pytest.mark.unittest
def test_repeat_block2():
    yes = Text("yes", "yep")
    block = Repeat(yes, duration=2.5)

    text = "no yes no no no yep yes no no no yep no yes no no no yep no no yes no"

    transcript = MockTranscript.from_string(text)
    context = Context(transcript)

    matches = block.matches(context)

    expected = [("yes", 1, 2),
                ("yep yes", 5, 7),
                ("yes", 6, 7),
                ("yep no yes", 10, 13),
                ("yes", 12, 13),
                ("yep no no yes", 16, 20),
                ("yes", 19, 20),
    ]

    assert len(expected) == len(matches)

    sorted_matches = sorted(matches, key=lambda m: (m.start, m.end, context.text(*m)))

    for match, (text, start, end) in izip_longest(sorted_matches, expected):
        assert isinstance(match, TranscriptMatch)
        assert text == context.text(*match).strip()
        assert start == match.start
        assert end == match.end


@pytest.mark.unittest
def test_repeat_block3():
    yes = Text("yes")
    yep = Text("yep")
    yes_or_yep = AnyOf(yes, yep)
    block = Repeat(yes_or_yep, duration=2.5)

    text = "no yes no no no yep yes no no no yep no yes no no no yep no no yes no"

    transcript = MockTranscript.from_string(text)
    context = Context(transcript)

    matches = block.matches(context)

    expected = [("yes", 1, 2),
                ("yep yes", 5, 7),
                ("yes", 6, 7),
                ("yep no yes", 10, 13),
                ("yes", 12, 13),
                ("yep no no yes", 16, 20),
                ("yes", 19, 20)]

    assert len(expected) == len(matches)

    sorted_matches = sorted(matches, key=lambda m: (m.start, m.end, context.text(*m)))

    for match, (text, start, end) in izip_longest(sorted_matches, expected):
        assert isinstance(match, TranscriptMatch)
        assert text == context.text(*match).strip()
        assert start == match.start
        assert end == match.end


@pytest.mark.unittest
def test_disjunction_block():
    one = Text("one")
    two = Text("two")
    block = AnyOf(one, two)

    text = "well let us see one two three again one and two and three there"

    transcript = MockTranscript.from_string(text)
    context = Context(transcript)

    matches = block.matches(context)
    expected = [
        ("one", 4, 5),
        ("two", 5, 6),
        ("one", 8, 9),
        ("two", 10, 11)
    ]

    assert len(expected) == len(matches)

    sorted_matches = sorted(matches, key=lambda m: (m.start, m.end, context.text(*m)))

    for match, (text, start, end) in izip_longest(sorted_matches, expected):
        assert isinstance(match, TranscriptMatch)
        assert text == context.text(*match).strip()
        assert start == match.start
        assert end == match.end


@pytest.mark.unittest
def test_overlapping_occurrence():
    text = "impress one press two press three oh wait press foursquare"
    transcript = MockTranscript.from_string(text)
    context = Context(transcript)

    """
    [x.group() for x in re.finditer("(?<![a-z])press (oh|zero|one|two|three|four|five|six|seven|eight|nine|0|1|2|3|4|5|6|7|8|9)(?![a-z])", s)]
    """
    press_digit = Text("press (oh|zero|one|two|three|four|five|six|seven|eight|nine|0|1|2|3|4|5|6|7|8|9)")
    matches = press_digit.matches(context)

    bounds = [(2.0, 4.0), (4.0, 6.0)]

    assert len(bounds) == len(matches)
    for match, bound in izip_longest(matches, bounds):
        assert bound == match


@pytest.mark.unittest
def test_initial_occurrence():
    text = "welcome hello welcome hello welcome hello welcome"

    transcript = MockTranscript.from_string(text)
    context = Context(transcript)

    welcome = Text("welcome")
    matches = welcome.matches(context)

    bounds = [(0.0, 1.0), (2.0, 3.0), (4.0, 5.0), (6.0, 7.0)]

    for match, bound in izip_longest(matches, bounds):
        assert bound == match


@pytest.mark.unittest
def test_word_list_in_time_range1():
    text = "well let us see one two three again one and two and three there"

    transcript = MockTranscript.from_string(text)
    context = Context(transcript)

    assert 5 == context.word_index_by_start_time[5]

    word_list = context.word_list_in_time_range(4, 6)
    assert "one two" == word_list.raw_text


@pytest.mark.unittest
def test_word_list_in_time_range2():
    time_per_word = 1.5
    text = "well let us see one two three again one and two and three there"

    transcript = MockTranscript.from_string(text, time_per_word=time_per_word)
    context = Context(transcript)

    assert 5 == context.word_index_by_start_time[7.5]

    assert "one two three" == context.text(6, 10.5).strip()
    assert "two three again one" == context.text(7.5, 13.5).strip()


@pytest.mark.unittest
def test_merge_spans():
    def make_word_list(u, v):
        return TranscriptWordList([TranscriptWord(symbol=str(i), start=i, end=i + 1) for i in range(u, v)])

    duration = 5
    lists = [
        make_word_list(0, 6),
        make_word_list(3, 8),
        make_word_list(14, 20),
        make_word_list(25, 30),
        make_word_list(34, 40),
    ]

    spans = [(l.start, l.end) for l in lists]

    merged_matches = IntroBuilder.merge_spans(spans, duration)

    assert 2 == len(merged_matches)
    assert (0, 8) == merged_matches[0]
    assert (14, 40) == merged_matches[1]


@pytest.mark.unittest
def test_intro_matches():
    words = get_words_from_test_samples_xml("test_transcription2.xml")
    transcript = MockTranscript(words)
    context = Context(transcript)

    matches = phase_block[PhaseType.INTRO].matches(context)

    expected_matches = [
        (14.34, 17.34, "hello hi ken this is vivian calling from"),
        (14.34, 18.75, "hello hi ken this is vivian calling from core provisions how are you"),
        (14.34, 20.15, "hello hi ken this is vivian calling from core provisions how are you all right"),
        (15.74, 17.34, "hi ken this is vivian calling from"),
        (15.74, 18.75, "hi ken this is vivian calling from core provisions how are you"),
        (15.74, 20.15, "hi ken this is vivian calling from core provisions how are you all right"),
    ]

    sorted_matches = sorted(matches, key=lambda m: (m.start, m.end, context.text(*m).strip()))

    assert len(expected_matches) == len(sorted_matches)

    for match, (start, end, text) in izip_longest(sorted_matches, expected_matches):
        assert start == match.start
        assert end == match.end
        assert text == context.text(*match).strip()


@pytest.mark.unittest
def test_match_and_build_intro_phase():
    words = get_words_from_test_samples_xml("test_transcription2.xml")
    transcript = MockTranscript(words)

    context = Context(transcript)

    intro_builder = IntroBuilder()

    expected_matches = [
        (14.34, 17.34, "hello hi ken this is vivian calling from"),
        (14.34, 18.75, "hello hi ken this is vivian calling from core provisions how are you"),
        (14.34, 20.15, "hello hi ken this is vivian calling from core provisions how are you all right"),
        (15.74, 17.34, "hi ken this is vivian calling from"),
        (15.74, 18.75, "hi ken this is vivian calling from core provisions how are you"),
        (15.74, 20.15, "hi ken this is vivian calling from core provisions how are you all right"),
    ]

    expected_phases = [
        (14.34, 20.15),
    ]

    transcript_matches = intro_builder.matches(context)
    matches = [context.word_list_in_time_range(x, y) for x, y in transcript_matches]

    sorted_matches = sorted(matches, key=lambda m: (m.start, m.end, m.raw_text))

    for match, (start, end, text) in izip_longest(sorted_matches, expected_matches):
        assert start == match.start
        assert end == match.end
        assert text == match.raw_text

    phases = intro_builder.build(context)
    sorted_phases = sorted(phases, key=lambda m: (m.start_time, m.end_time))

    for phase, (start, end) in izip_longest(sorted_phases, expected_phases):
        assert start == phase.start_time
        assert end == phase.end_time


def test_transcript_intro_phase2():
    words = get_words_from_test_samples_xml("test_transcription2.xml")
    transcript = MockTranscript(words)

    phase_detector = StructuredPhaseDetector.load_current()

    for builder in phase_detector.builders:
        from ..phases.structured.phase_builder_lib import NextStepsBuilder
        if isinstance(builder, NextStepsBuilder):
            builder.duration = 15

    phases = phase_detector.process_transcript(transcript)

    phases_by_type = defaultdict(list)
    for phase in phases:
        phases_by_type[get_phase_type(phase)].append(phase)

    # intro

    phases = phases_by_type[PhaseType.INTRO]
    expected_phases = [
        (14.34, 20.15),
    ]

    assert len(expected_phases) == len(phases)

    sorted_phases = sorted(phases, key=lambda m: (m.start_time, m.end_time))

    for phase, (start, end) in izip_longest(sorted_phases, expected_phases):
        assert isinstance(phase, Phase)
        assert get_phase_type(phase) == PhaseType.INTRO
        assert start == phase.start_time
        assert end == phase.end_time

    # next steps

    phases = phases_by_type[PhaseType.NEXT_STEPS]

    expected_phases = [
        (148.49, 150.54),
        (196.2, 208.77),
    ]

    assert len(expected_phases) == len(phases)

    sorted_phases = sorted(phases, key=lambda m: (m.start_time, m.end_time))

    for phase, (start, end) in izip_longest(sorted_phases, expected_phases):
        assert isinstance(phase, Phase)
        assert get_phase_type(phase) == PhaseType.NEXT_STEPS
        assert start == phase.start_time
        assert end == phase.end_time


def test_transcript3():
    words = get_words_from_test_samples_json("test_transcript3.json")
    transcript = MockTranscript(words)

    phase_detector = StructuredPhaseDetector.load_current()

    phases = phase_detector.process_transcript(transcript)
    phases_by_type = get_phases_by_type(phases)

    expected_phases_by_type = {
        PhaseType.INTRO: [
            (21.74, 32.33),
        ],
        PhaseType.NEXT_STEPS: [
            (109.32, 139.85),
        ],
        PhaseType.OBJECTION: [
            (144.79, 147.52),
        ],
    }

    evaluate(phases_by_type, expected_phases_by_type)

    # to troubleshoot, uncomment the following:
    # debug_display_phases_by_type(phases_by_type, transcript, title="OBTAINED")
    # debug_display_phases_by_type(to_phases(expected_phases_by_type), transcript, title="EXPECTED")


def test_transcript4():
    words = get_words_from_test_samples_json('test_transcript4.json')
    transcript = MockTranscript(words)

    phase_detector = StructuredPhaseDetector.load_current()

    phases = phase_detector.process_transcript(transcript)

    phases_by_type = get_phases_by_type(phases)

    expected_phases_by_type = {
        PhaseType.IVR: [
            (18.699, 25.609),
            (100.939, 107.879),
            (545.325, 553.545),
            (664.0, 666.84),
        ],
        PhaseType.INTRO: [
            (168.44, 170.94),
        ]
    }

    evaluate(phases_by_type, expected_phases_by_type)

    # to troubleshoot, uncomment the following:
    # debug_display_phases_by_type(phases_by_type, transcript, title="OBTAINED")
    # debug_display_phases_by_type(to_phases(expected_phases_by_type), transcript, title="EXPECTED")


def test_transcript5():
    words = get_words_from_test_samples_json("test_transcript5.json")
    transcript = MockTranscript(words)

    phase_detector = StructuredPhaseDetector.load_current()

    phases = phase_detector.process_transcript(transcript)

    phases_by_type = get_phases_by_type(phases)

    expected_phases_by_type = {
        PhaseType.NEXT_STEPS: [
            (1626.16, 1626.56),
        ],
        PhaseType.INTRO: [
            (1610.6, 1616.78),
        ],
        PhaseType.IVR: [
            (4.04, 29.611),
            (95.671, 109.973),
            (138.32, 142.62),
            (294.8, 307.56),
            (320.64, 348.06),
            (395.81, 409.886),
            (583.746, 624.176),
            (928.01, 989.594),
            (1061.27, 1069.2),
            (1555.22, 1565.76),
            (1893.24, 1909.87),
            (1957.41, 1968.2),
            (1991.76, 2027.21),
            (2048.16, 2054.82),
            (2101.6, 2156.86),
            (2203.79, 2244.01),
            (2269.83, 2272.36),
        ]
    }

    evaluate(phases_by_type, expected_phases_by_type)

    # to troubleshoot, uncomment the following:
    # debug_display_phases_by_type(phases_by_type, transcript, title="OBTAINED", show_text=False)
    # debug_display_phases_by_type(to_phases(expected_phases_by_type), transcript, title="EXPECTED")


def test_transcript6():
    words = get_words_from_test_samples_json('test_transcript6.json')
    transcript = MockTranscript(words)

    phase_detector = StructuredPhaseDetector.load_current()

    phases = phase_detector.process_transcript(transcript)

    phases_by_type = get_phases_by_type(phases)

    expected_phases_by_type = {
        PhaseType.IVR: [
            (97.867, 120.847),
            (212.621, 219.521),
            (438.24, 458.05),
        ]
    }

    evaluate(phases_by_type, expected_phases_by_type)

    # to troubleshoot, uncomment the following:
    # debug_display_phases_by_type(phases_by_type, transcript, show_text=False)
    # debug_display_phases_by_type(to_phases(expected_phases_by_type), transcript, show_text=False)


def test_transcript7():
    words = get_words_from_test_samples_json('test_transcript7.json')
    transcript = MockTranscript(words)

    phase_detector = StructuredPhaseDetector.load_current()
    phases = phase_detector.process_transcript(transcript)

    phases_by_type = get_phases_by_type(phases)

    expected_phases_by_type = {
        PhaseType.AGREEMENT: [
            (84.99, 85.98),
        ],
        PhaseType.IVR: [
            (2.84, 28.95),
        ]
    }

    evaluate(phases_by_type, expected_phases_by_type)

    # to troubleshoot, uncomment the following:
    # debug_display_phases_by_type(phases_by_type, transcript, show_text=False)
    # debug_display_phases_by_type(to_phases(expected_phases_by_type), transcript, show_text=False)

"""
def test_transcript8():
    words = get_words_from_test_samples_json('test_transcript8.json')
    transcript = MockTranscript(words)

    phase_detector = StructuredPhaseDetector.load_current()
    phases = phase_detector.process_transcript(transcript)

    phases_by_type = get_phases_by_type(phases)

    expected_phases_by_type = {
        PhaseType.INTRO: [
            (1610.6, 1616.78),
        ],
        PhaseType.NEXT_STEPS: [
            (1626.16, 1626.56),
        ],
        PhaseType.IVR: [
            (8.96, 28.95),
        ]
    }

    #evaluate(phases_by_type, expected_phases_by_type)

    # to troubleshoot, uncomment the following:
    debug_display_phases_by_type(phases_by_type, transcript, show_text=False)
    # debug_display_phases_by_type(to_phases(expected_phases_by_type), transcript, show_text=False)
"""


def test_transcript9():
    words = get_words_from_test_samples_json('test_transcript9.json')
    transcript = MockTranscript(words)

    phase_detector = StructuredPhaseDetector.load_current()

    phases = phase_detector.process_transcript(transcript)

    phases_by_type = get_phases_by_type(phases)

    expected_phases_by_type = {
        PhaseType.INTRO: [
            (5497.44, 5501.66),
        ],
        PhaseType.NEXT_STEPS: [
            (578.77, 579.64),
            (3432.72, 3433.55),
            (4726.18, 4726.74),
            (5505.75, 5506.38),
        ],
        PhaseType.AGREEMENT: [
            (438.19, 439.48),
            (520.9, 569.49),
            (2162.95, 2164.14),
            (5106.29, 5110.09),
            (5232.58, 5233.79),
        ],
        PhaseType.DEMO: [
            (524.78, 526.57),
            (1283.39, 1284.59),
        ],
        PhaseType.IVR: [
            (7.36, 30.71),
        ],
        PhaseType.VALUE_PROP: [
            (4582.62, 4583.69),
        ]
    }

    evaluate(phases_by_type, expected_phases_by_type)

    # to troubleshoot, uncomment the following:
    # debug_display_phases_by_type(phases_by_type, transcript)
    # debug_display_phases_by_type(to_phases(expected_phases_by_type), transcript, show_text=False)


def evaluate(phases_by_type, expected_phases_by_type):
    phase_types = set(chain(phases_by_type.keys(), expected_phases_by_type.keys()))
    for phase_type in phase_types:
        observed_phases = sorted(phases_by_type[phase_type], key=lambda p: (p.start_time, p.end_time))
        expected_phases = sorted(expected_phases_by_type[phase_type], key=lambda p: (p[0], p[1]))

        assert len(observed_phases) == len(expected_phases)

        for observed_phase, expected_phase in izip_longest(observed_phases, expected_phases):
            assert observed_phase.start_time == expected_phase[0]
            assert observed_phase.end_time == expected_phase[1]


def to_phases(phases_by_type):
    return {phase_type: [make_phase(p[0], p[1], phase_type) for p in phases_by_type[phase_type]] for phase_type in phases_by_type}


def get_phases_by_type(phases):
    """
    :param collection[Phase] phases:
    :rtype: dict[str,list[Phase]]
    """
    phases_by_type = defaultdict(list)
    for phase in phases:
        phase_type = get_phase_type(phase)
        phases_by_type[phase_type].append(phase)
    return phases_by_type


def get_phase_type(phase):
    return [p for p in PhaseType if p.name == phase.name][0]


def debug_display_phases_by_type(phases_by_type, transcript, phase_types_to_display=None, show_text=True, title=None):
    """
    :param dict[str,list[Phase]] phases_by_type:
    :param Transcript or MockTranscript transcript:
    :param str or list[str] phase_types_to_display:
    :param bool show_text:
    :param str title:
    """
    WIDTH = 50
    index_by_start = {x.start: i for i, x in enumerate(transcript.words)}
    index_by_end = {x.end: i for i, x in enumerate(transcript.words)}
    word_count = len(transcript.words)

    def text(start, end):
        """:rtype: str"""
        left_buffer = 10
        right_buffer = 10
        start_index = index_by_start[start]
        end_index = index_by_end[end] + 1
        left_index = max(start_index - left_buffer, 0)
        right_index = min(end_index + right_buffer, word_count)

        raw_text = '{} "{}" {}'.format(
            transcript.words[left_index:start_index].raw_text,
            transcript.words[start_index:end_index].raw_text,
            transcript.words[end_index:right_index].raw_text)
        tokens = raw_text.split(' ')
        pretty_text = []
        line = ''
        for token in tokens:
            if len(line) + len(token) >= WIDTH:
                pretty_text.append(line)
                line = ''
            line += ' ' + token
        pretty_text.append(line)
        return "\n".join(pretty_text)

    if title:
        log.info("\n\n" + title)

    for phase_type in phases_by_type:
        if phase_types_to_display and phase_type not in phase_types_to_display:
            continue
        phases = phases_by_type[phase_type]
        log.info("\n\n\n{}: {}".format(phase_type, len(phases)))
        for phase in phases:

            log.info("\n({}, {})\n{} sec\n{}".format(
                phase.start_time,
                phase.end_time,
                phase.end_time - phase.start_time,
                text(phase.start_time, phase.end_time) if show_text else ''))
