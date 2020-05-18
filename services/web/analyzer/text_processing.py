from __future__ import annotations
from typing import Dict, List
from collections import Counter, defaultdict
import pandas as pd
from time import time

import logging

import nltk
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize


log = logging.getLogger(__name__)

DataFrame = pd.DataFrame
Series = pd.Series
Timestamp = pd.Timestamp


try:
    nltk.data.find('tokenizers/punkt.zip')
except LookupError:
    nltk.download('punkt')

try:
    _ = set(stopwords.words('english'))
except LookupError:
    nltk.download('stopwords')


class CalendarUtils:
    def __init__(self):
        self._days_in_year_cache = {}

    @staticmethod
    def compute_days_in_year(year: int) -> int:
        return 366 if CalendarUtils.is_leap_year(year) else 365

    @staticmethod
    def is_leap_year(year: int) -> bool:
        """
        leap years - any year that is
        a multiple of 4 (2016, 2020, 2024 are leap years)
            except if it's a multiple of 100 (2100, 2200, 2300 are not leap years)
                unless it's a multiple of 400 (2000, 2400, 2800 are leap years)
        """
        if year % 4 == 0:
            if year % 100 == 0:
                if year % 400 == 0:
                    return True
                return False
            return True
        return False

    def days_in_year(self, year):
        if year not in self._days_in_year_cache:
            self._days_in_year_cache[year] = self.compute_days_in_year(year)
        return self._days_in_year_cache[year]


class WordHistoryResult(dict):
    KEY_COUNTS = "counts"
    KEY_TOTALS = "totals"

    def __init__(self, counts: Dict[str, List[int]], totals: Dict[str, int]):
        super().__init__({self.KEY_COUNTS: counts, self.KEY_TOTALS: totals})

    @property
    def counts(self) -> Dict[str, List[int]]:
        return self[self.KEY_COUNTS]

    @property
    def totals(self) -> List[int]:
        return self[self.KEY_TOTALS]

    def serialize(self):
        return dict(
            counts=self.counts,
            totals=self.totals,
        )


class WordHistoryProcessor:
    def __init__(
        self, df: DataFrame,
        text_column_name: str,
        date_time_column_name: str,
        window_in_days: int = 1,
        min_word_length: int = 3,
    ):
        self.df = df
        self.window_in_days = window_in_days
        self.text_column_key: str = text_column_name
        self.date_time_column_key: str = date_time_column_name
        self.derived_date_time_column_key = None

        self._min_day_index: int = -1
        self._min_word_length = min_word_length

        self.stop_words = set(stopwords.words('english'))

        self.calendar_utils = CalendarUtils()
        self.counter: Dict[str, Counter] = defaultdict(Counter)
        self.totals: Counter = Counter()

    def _get_word_counts_by_time(self) -> None:
        df = self.df

        nonce = str(int(time()))
        dt_column_key = "date_time_column" + nonce
        self.derived_date_time_column_key = dt_column_key

        '''
        date_time_format = "%a, %d %b %Y %H:%M:%S %Z"
        df[dt_column_key] = pd.to_datetime(
            df[self.date_time_column_key],
            errors="ignore",
            format=date_time_format,
        )
        '''

        df.T.apply(self._extract_counts_over_time)

    def _extract_counts_over_time(self, series: Series) -> None:
        text_column_key = self.text_column_key
        date_time_column_key = self.derived_date_time_column_key
        stop_words = self.stop_words
        counter = self.counter
        totals = self.totals

        try:
            # date_time: Timestamp = series.get(date_time_column_key)
            date_time: Timestamp = pd.to_datetime(
                series.get(self.date_time_column_key),
                errors="ignore",
                # format=date_time_format,
            )

            day_index = (date_time.year, date_time.dayofyear)
            if self._min_day_index == -1:
                self._min_day_index = day_index
            else:
                self._min_day_index = min(day_index, self._min_day_index)

        except AttributeError as exc:
            log.info(exc)
            return

        '''
        # round to the nearest n-day window
        if self.window_in_days > 1:
            day_index = day_index // self.window_in_days * self.window_in_days
        '''

        text = series[text_column_key]

        for w in tokenize(text, self._min_word_length):
            if w not in stop_words:
                counter[w][day_index] += 1
                totals[w] += 1

    def _map_word_counts_to_histories(self) -> Dict[str, Dict[int, int]]:
        word_counts_over_time: Dict[str, Dict[int, int]] = {}
        min_day_int = self.day_index_to_int(self._min_day_index)
        log.info("min_day_int: %s", min_day_int)

        for word, counts_by_day in self.counter.items():
            day_index_max = max(counts_by_day.keys())
            day_index_int_max = self.day_index_to_int(day_index_max) - min_day_int

            # word_counts_over_time[word] = [0] * (day_index_int_max + 1)
            word_counts_over_time[word] = {}
            counts = word_counts_over_time[word]

            for day_index in counts_by_day:
                day_index_int = self.day_index_to_int(day_index) - min_day_int
                counts[day_index_int] = counts_by_day.get(day_index, 0)
        return word_counts_over_time

    def get_top_word_counts_over_time(
        self,
        word_counts_over_time: Dict[str, Dict[int, int]],
        n: int
    ) -> Dict[str, List[int]]:
        sorted_total_tuples = sorted(
            [(count, word) for word, count in self.totals.items()], reverse=True,
        )

        results = {}
        for total, word in sorted_total_tuples[:n]:
            word_counts = word_counts_over_time[word]
            max_day = max(word_counts.keys())
            count_history = [0] * (max_day + 1)
            for day, count in word_counts.items():
                count_history[day] = count
            results[word] = count_history

        return results

    def process(self) -> WordHistoryResult:
        self._get_word_counts_by_time()
        word_counts_over_time = self._map_word_counts_to_histories()
        top_word_counts_over_time = self.get_top_word_counts_over_time(word_counts_over_time, n=50)
        return WordHistoryResult(
            counts=top_word_counts_over_time,
            totals=self.totals,
        )

    def day_index_to_int(self, day_index):
        year, day_of_year = day_index
        min_year = self._min_day_index[0]
        day_count = sum(
            [self.calendar_utils.days_in_year(year) for year in range(min_year, year + 1)]
        )
        return day_count + day_of_year


def tokenize(text: str, min_word_length: int) -> List[str]:
    apostrophe = "'"
    space = " "
    strip_chars = r'[()"\']'
    replace_chars = ".,?!:;*/\n\t"
    remove_chars = "-"
    max_word_length = 12
    results = []

    for char in replace_chars:
        text = text.replace(char, " ")
    for char in remove_chars:
        text = text.replace(char, "")
    tokens = text.split(space)

    for token in tokens:
        if not token:
            continue

        token = token.strip(strip_chars)
        if apostrophe in token:
            token = token.split(apostrophe)[0]

        if token.isupper() and (3 <= len(token) < 5):
            continue
        else:
            if len(token) < min_word_length or len(token) > max_word_length:
                continue
            token = token.lower()

        results.append(token)
    return results
