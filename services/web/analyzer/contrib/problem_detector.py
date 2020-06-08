from typing import List, Dict
import logging

import pandas as pd

import re

log = logging.getLogger(__name__)


DataFrame = pd.DataFrame
Series = pd.Series
Pattern = re.Pattern

replace = re.compile(r"[.,?!:;*/\n\t()]")
remove = re.compile(r"-")

SPACE = " "


class BaseTwoPassSurfacePatternDetector:
    REGEX_REPLACE_CHARS = r'[.,?!:;*/\n\t()"]'
    REGEX_REMOVE_CHARS = r'( - |-)'
    REGEX_NORMALIZE_WHITESPACE = r' [ ]+'

    def __init__(
        self,
        name: str,
        text_column_labels: List[str],
        pattern_phase1: Pattern,
        pattern_phase2: Pattern,
        rating_column_labels: List[str],
        rating_map: Dict[str, Dict[str, int]]
    ):
        self.name = name

        self.column_labels = text_column_labels
        self.rating_column_labels = rating_column_labels
        self.rating_map = rating_map

        self.scores: List[int] = []

        self.pattern_phase1: Pattern = pattern_phase1
        self.pattern_phase2: Pattern = pattern_phase2

        self.pattern_replace_chars = re.compile(self.REGEX_REPLACE_CHARS)
        self.pattern_remove_chars = re.compile(self.REGEX_REMOVE_CHARS)
        self.pattern_normalize_ws = re.compile(self.REGEX_NORMALIZE_WHITESPACE)

    @classmethod
    def type(cls) -> str:
        return "BaseTwoPassSurfacePatternDetector"

    @property
    def text_label(self) -> str:
        return f"{self.name}_text"

    @property
    def score_label(self) -> str:
        return f"{self.name}_score"

    def _normalize_text(self, text: str) -> str:
        text = self.pattern_replace_chars.sub(SPACE, text)
        text = self.pattern_normalize_ws.sub(SPACE, text)
        text = self.pattern_remove_chars.sub("", text)
        return text.strip().lower()

    def _process_text(self, series: Series) -> str:
        text_fields = []
        for label in self.column_labels:
            value = series[label]
            text = "" if pd.isnull(value) or not value else str(value)
            text_fields.append(text)

        return self._normalize_text(
            SPACE.join(text_fields)
        )

    def _should_ignore(self, text: str, ratings: List[int]) -> bool:
        if min(ratings) < 0 or max(ratings) < 1:
            return False
        if self.pattern_phase1.search(text):
            return False
        return True

    def _process_ratings(self, series: Series) -> List[int]:
        values = []
        for label in self.rating_column_labels:
            row_value = series[label]
            value = self.rating_map[label].get(row_value, 0)
            values.append(value)
        return values

    def _score(self, series: Series) -> int:
        text = series[self.text_label]
        if pd.isnull(text):
            return 0

        ratings = self._process_ratings(series)

        if self._should_ignore(text, ratings):
            return 0

        return sum(1 for _ in self.pattern_phase1.findall(text))

    def _format_text(self, series: Series) -> str:
        score = series[self.score_label]
        if score == 0:
            return ""

        text = series[self.text_label]

        for match_group in self.pattern_phase1.findall(text):
            term = match_group[0]
            text = text.replace(
                term, f'<span style="font-weight: bold; font-size:110%">{term}</span>',
            )

        return text

    def apply(self, df: DataFrame):
        df[self.text_label] = df.T.apply(self._process_text)
        df[self.score_label] = df.T.apply(self._score)
        df[self.text_label] = df.T.apply(self._format_text)


class ResponseMapper:
    def __init__(self: List[List[str]]):
        # `raw` maps values on a range from -n to n (where the range has 2n values),
        # `abs` maps values only to -1, 0, or 1, e.g., [-1, -1, -1, 0, 1, 1, 1]
        self._raw_value_maps: List[Dict[str, int]] = []
        self._abs_value_maps: List[Dict[str, int]] = []

    @classmethod
    def _raw_range(cls, responses: List[str]) -> Dict[str, int]:
        midpoint = int(len(responses) / 2)
        return {
            response: i - midpoint for i, response in enumerate(responses)
        }

    @classmethod
    def _abs_range(cls, responses: List[str]) -> Dict[str, int]:
        return {
            response: cls._to_abs(value) for response, value in cls._raw_range(responses).items()
        }

    @classmethod
    def _to_abs(cls, value: int):
        """map integers less than 0 to -1, greater than 0 to 1, and anything else to 0"""
        if not value:
            return 0
        return 1 if int(value) > 0 else -1

    def get_maps(
        self, responses_by_column_label: Dict[str, List[str]],
    ) -> Dict[str, Dict[str, int]]:
        return {
            label: self._abs_range(responses) for label, responses in
            responses_by_column_label.items()
        }


class ProblemReportDetector(BaseTwoPassSurfacePatternDetector):
    PREFIX_PREVENTION = r"(?<![a-zA-z])"
    SUFFIX_PREVENTION = r"(?![a-zA-Z])"

    PHASE1_TERMS = [
        r"error(|s|ed|red)",
        r"fail(|ed|s|ing|ure|ures)",
        r"crash(|es|ed|ing)",
        r"unauthorized",
        r"unexpected error",
        r"error has occurred",
        r"system",
        r"browser",
        r"navigat(e|ing|ion)",
        r"survey(s)",
        r"site",
        r"web(s| s)ite(|s)",
        r"web(p| p)age(|s)",
        r"(|hyper)link(|s|ed)",
        r"click(|ed|ing)",
        r"broken",
        r"password(|s)",
        r"wrong page",
        r"faq",
        r"drop(| |-)down",
        r"download(|s|ed|ing)",
        r"log(|ged|ging)(| |-)(in|out)",
        r"security question",
        # r"creat(e|ed|ing)",
        r"server",
        r"gateway",
        r"redirect(|s|ed|ing)",
        r"responding",
        r"tim(e|ed|ing)(| |-)out",
        r"not found",
        r"window",
        r"tab(|s)",
        r"pop(| |-)up(|s)",
        r"internet explorer",
        r"chrome",
        r"firefox",
        r"safari",
        r"android",
        r"os x",
        r"osx",
        r"macos",
    ]

    PHASE2_TERMS = [
        r"confus(e|ed|ing)",
        r"frustrat(e|ed|ing)",
        r"difficult(|y|ies)",
        r"broke(|n)",
        r"wrong",
        r"not right",
        r"(wasn't|wasnt|isn't|isnt|not) clear",
        r"overwhelm(s|ed|ing)",
        r"error(|r)(|s|ed|ing)",
        r"problem(|s|atic)",
        r"fail(|s|ed|ure|ures)",
        r"issue(|s)",
        r"unexpected error",
        r"error has occurred",
        r"g(a|i)ve(|n) up",
        r"trie(s|d)",
        r"try(|ing)",
        r"attempt(|s|ed|ing)",
        r"unfortunate(|ly)",
        r"unclear",
        r"(wasn't |wasnt| un)able",
        r"(could|did)(n't|nt| not) (find|see|understand)",
        r"(could|would|did|does|is)(n't|nt| not)",
        r"can('t|t|not| not)",
        r"(cant|can't) find",
        r"incomplete",
        r"could(n't|nt| not) tell",
        r"(does|did)(n't|nt| not) .{0,10}work",
        r"'find (what|the|any| )(info|information)",
    ]

    def __init__(
        self,
        name: str,
        text_column_labels: List[str],
        rating_column_labels: List[str],
        rating_map: Dict[str, Dict[str, int]],
    ):
        patterns = self._generate_patterns()

        super().__init__(
            name=name,
            text_column_labels=text_column_labels,
            pattern_phase1=patterns[0],
            pattern_phase2=patterns[1],
            rating_column_labels=rating_column_labels,
            rating_map=rating_map
        )

    def _generate_patterns(self) -> List[Pattern]:
        patterns = []
        for terms in [self.PHASE1_TERMS, self.PHASE2_TERMS]:
            unioned_terms = r"{prefix}({terms}){suffix}".format(
                prefix=self.PREFIX_PREVENTION,
                terms="|".join(term for term in terms),
                suffix=self.SUFFIX_PREVENTION,
            )

            patterns.append(re.compile(unioned_terms))
        return patterns


class CategoryDetector:
    def __init__(self, column_label: str):
        self.column_label = column_label

    def get_counts(self, series: Series, words: List[str]) -> str:
        text = series[self.column_label]

        counts = [
            (text.count(word), word) for word in words
        ]

        highest_count_pair = sorted(counts, reverse=True)[0]

        return highest_count_pair[1]
