import pytest

import logging

from ..phases.structured.transcript_match_lib import TranscriptMatchResolver
from ..phases import PatternMatch
from . import MockTranscript

logging.basicConfig(
    level=logging.DEBUG, filename="log/tests.log", format="%(message)s",
)

log = logging.getLogger(__name__)


@pytest.mark.unittest
def test_simple_word_single_match():
    text = "AAA BBB CCC DDD EEE FFF GGG HHH III"

    transcript = MockTranscript.from_string(text)

    pattern_matches = [PatternMatch("DDD EEE FFF", 12, 24)]

    resolver = TranscriptMatchResolver(transcript)
    matches = resolver.resolve_matches(pattern_matches)

    assert 1 == len(matches)
    match0 = matches[0]

    assert 3.0 == match0.start
    assert 6.0 == match0.end


@pytest.mark.unittest
def test_simple_word_multi_match():
    text = "AAA BBB CCC DDD EEE FFF GGG HHH III"

    transcript = MockTranscript.from_string(text)

    pattern_matches = [PatternMatch("DDD EEE FFF", 12, 24),
                       PatternMatch("GGG HHH", 24, 31)]

    resolver = TranscriptMatchResolver(transcript)
    matches = resolver.resolve_matches(pattern_matches)

    assert 2 == len(matches)
    match0, match1 = matches

    assert 3.0 == match0.start
    assert 6.0 == match0.end

    assert 6.0 == match1.start
    assert 8.0 == match1.end


@pytest.mark.unittest
def test_mid_word_single_match():
    text = "AAA BBB CCCDDD EEE FFFGGG HHH III"

    pattern_matches = [PatternMatch("DDD EEE FFF", 11, 22)]

    transcript = MockTranscript.from_string(text)
    resolver = TranscriptMatchResolver(transcript)
    matches = resolver.resolve_matches(pattern_matches)

    assert 1 == len(matches)
    match0 = matches[0]

    assert 2.0 == match0.start
    assert 5.0 == match0.end
