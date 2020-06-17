from collections import Counter, defaultdict, deque, namedtuple
from typing import List, Dict, DefaultDict, Set, Tuple, Union, Optional
from itertools import chain
from time import time
from copy import deepcopy
import math
import logging

import numpy as np
from scipy.stats import entropy
from pandas import DataFrame

# spaCy based imports
from spacy.tokens import Token as SpacyToken
from spacy.lang.en.stop_words import STOP_WORDS
from spacy.lang.en import English as SpacyParser

import en_core_web_sm

spacy_parser = en_core_web_sm.load(disable=['ner'])

log = logging.getLogger(__name__)

SPACE = " "

dep_direct_obj = "dobj"
dep_indirect_obj = "iobj"
dep_obj_of_prep = "pobj"
dep_obj = "obj"
dep_subj = "nsubj"
dep_prep = "prep"
dep_sub_clausal = "csubj"
dep_sub_clausal_pass = "csubjpass"
dep_appos = "appos"
dep_root = "ROOT"
pos_verb = "VERB"
pos_noun = "NOUN"
pos_pronoun = "PRON"
pos_proper_noun = "PROPN"
pos_symbol = "SYM"
pos_punc = "PUNCT"
pos_det = "DET"
tag_personal_pronoun = "PRP"
tag_possessive = "POS"

TokenEntry = namedtuple("TokenEntry", "id age dep pos tag token")

DatasetEntry = namedtuple("DatasetEntry", "id pkey age text")

CategoryTree = Union[Dict[str, List[str]], DefaultDict[str, List[str]]]


class EntryId(int):
    pass


class TextProcessor:
    def __init__(self, parser: SpacyParser):
        self.parser = parser

        # lookup spacy produced documents by entry id
        self.doc_by_id = {}

        # correct spacy-specific lemmatization issues
        self._spacy_lemmatization_corrections = {
            "taxis": "taxes",
        }

        self._text_corrections = [
            ("`", "'"),
            ("“", '"'),
            ("/", ' '),
            ("web page", "site"),
            ("web site", "site"),
            ("webpage", "site"),
            ("website", "site"),
        ]

        self._collapse_hyphens = True
        self._do_add_phrases = False
        self._do_add_bigrams = True
        self._do_add_proper_noun = False

        self.pos_ignore = {pos_pronoun, pos_det, pos_symbol, pos_punc}
        self.tag_ignore = {tag_personal_pronoun, tag_possessive}

    def is_oov(self, word: str) -> bool:
        if word not in self.parser.vocab and not self.parser.vocab.has_vector(word):
            return True
        return False

    @staticmethod
    def get_bigrams(words: List[str]) -> List[str]:
        if len(words) < 2:
            return []
        last_word = words[-1]
        return [f"{word} {last_word}".lower() for word in words[:-1]]

    def cleanse_text(self, text: str) -> str:
        if self._collapse_hyphens:
            text = text.replace("-", "")

        for original, replacement in self._text_corrections:
            text = text.replace(original, replacement)

        return text

    def process(self, text: str, doc_id: str) -> DefaultDict[str, List[SpacyToken]]:
        doc = self.parser(self.cleanse_text(text))

        if doc_id:
            self.doc_by_id[doc_id] = doc

        tokens = defaultdict(list)
        for chunk in doc.noun_chunks:
            self._process_noun_chunk(chunk, tokens)

        return tokens

    def _process_noun_chunk(self, chunk, tokens: DefaultDict[str, List[SpacyToken]]):
        pos_ignore = self.pos_ignore
        tag_ignore = self.tag_ignore
        lemmatization_corrections = self._spacy_lemmatization_corrections
        do_add_proper_noun = self._do_add_proper_noun
        max_len = 50

        chunk_words = []

        last_spacy_token = None
        for spacy_token in chunk:
            if spacy_token.tag_ in tag_ignore or spacy_token.pos_ in pos_ignore:
                continue

            lemmatized_token = spacy_token.lemma_.lower()
            chunk_word = lemmatization_corrections.get(lemmatized_token, lemmatized_token)

            if self.is_oov(chunk_word):
                tokens[chunk_word] = spacy_token

            elif not chunk_word.isnumeric() and not chunk_word.isalpha():
                tokens[chunk_word] = spacy_token

            elif do_add_proper_noun:
                if spacy_token.pos_ == pos_proper_noun and len(chunk_word) < max_len:
                    tokens[chunk_word] = spacy_token

            chunk_words.append(chunk_word)
            last_spacy_token = spacy_token

        if chunk_words:
            if self._do_add_bigrams and last_spacy_token:
                for bigram in self.get_bigrams(chunk_words):
                    if bigram not in tokens:
                        tokens[bigram] = last_spacy_token

            if self._do_add_phrases:
                chunk_phrase = SPACE.join(chunk_words).lower()
                tokens[chunk_phrase] = last_spacy_token


class Corpus:
    def __init__(self, text_processor):
        self.text_processor = text_processor
        self.ids_by_age: DefaultDict[int, List[EntryId]] = defaultdict(list)

        self.pkey_by_id: Dict[EntryId, str] = {}
        self.id_by_pkey: Dict[str, EntryId] = {}

        self.token_entry_lookup: DefaultDict[str, List[TokenEntry]] = defaultdict(list)
        self.tokens_by_id = defaultdict(list)

        self.text_by_id: DefaultDict[EntryId, str] = defaultdict(str)
        self.unigrams_by_id: DefaultDict[EntryId, List[str]] = defaultdict(list)

        self.age_in_weeks_max = -1
        self.age_in_weeks_min = 520000

    def add_entry(self, entry: DatasetEntry):
        raw_text = entry.text
        age_in_weeks = int(entry.age) // 7

        # record the age (in weeks) of the entry
        self.ids_by_age[age_in_weeks].append(entry.id)

        # keep track of the oldest and newest entries
        self.age_in_weeks_max = max(age_in_weeks, self.age_in_weeks_max)
        self.age_in_weeks_min = min(age_in_weeks, self.age_in_weeks_min)

        # create a mapping between internal entry id and primary key
        self.pkey_by_id[entry.id] = entry.pkey
        self.id_by_pkey[entry.pkey] = entry.id

        self.text_by_id[entry.id] = raw_text

        for string, token in self.text_processor.process(raw_text, entry.id).items():

            token_entry = TokenEntry(
                id=entry.id, age=age_in_weeks, dep=token.dep_, pos=token.pos_, tag=token.tag_,
                token=token,
            )

            self.token_entry_lookup[string].append(token_entry)
            self.tokens_by_id[entry.id].append(string)

    @classmethod
    def from_df(
        cls,
        df: DataFrame,
        pkey_column_name: str,
        text_column_name: str,
        age_column_name: str,
        text_processor: TextProcessor,
    ):
        corpus = cls(text_processor)

        start_time = time()
        log.info(f"processing rows...")
        index = 0
        for _, row in df.iterrows():
            text = row[text_column_name].strip()

            pkey = row[pkey_column_name]
            age = row[age_column_name]

            entry = DatasetEntry(index, pkey, age, text)

            corpus.add_entry(entry)
            index += 1

        time_elapsed = time() - start_time
        log.info(f"processed {index} rows in {time_elapsed:.1f} sec")

        return corpus


LmCategoryLookup = DefaultDict[str, Counter]
LmSubcategoryLookup = DefaultDict[str, DefaultDict[str, Counter]]


class CorpusProcessor:
    DEFAULT_CATEGORY = "misc"
    DEFAULT_SUBCATEGORY = "misc"
    DEFAULT_PAIR = DEFAULT_CATEGORY, DEFAULT_SUBCATEGORY

    def __init__(self, corpus: Corpus, exclude_words: Set[str] = None):
        self._corpus = corpus
        self._category_tree = None

        self._text_processor = corpus.text_processor
        self._ids_by_age: DefaultDict[int, List[EntryId]] = corpus.ids_by_age

        self._pkey_by_id: Dict[EntryId, str] = corpus.pkey_by_id
        self._id_by_pkey: Dict[str, EntryId] = corpus.id_by_pkey

        self._token_entry_lookup: DefaultDict[str, List[TokenEntry]] = corpus.token_entry_lookup
        self._tokens_by_id: DefaultDict[EntryId, List[str]] = corpus.tokens_by_id

        self.text_by_id: DefaultDict[EntryId, str] = corpus.text_by_id
        self._unigrams_by_id: DefaultDict[EntryId, List[str]] = corpus.unigrams_by_id

        self.age_in_weeks_max = corpus.age_in_weeks_max
        self.age_in_weeks_min = corpus.age_in_weeks_min

        self.exclude_words = exclude_words or set()
        self.num_categories = None

        self.lm_by_category: LmCategoryLookup = defaultdict(Counter)
        self.lm_by_subcategory: LmSubcategoryLookup = defaultdict(lambda: defaultdict(Counter))

    def build_model(self, entry_ids: Optional[List[EntryId]] = None):
        category_tree = self._build_category_tree(entry_ids)
        category_tree = self._build_language_models(category_tree)

        self._category_tree = category_tree

    @classmethod
    def _category_count_heuristic(cls, counts: Counter):
        ignore_count = 5
        top_count = None
        i = 1
        for i, (token, count) in enumerate(counts.most_common()):
            if i < ignore_count:
                continue
            if top_count is None:
                top_count = count

            if count < top_count / 2:
                break

        return i * 4

    def _build_category_tree(
        self, entry_ids: Optional[List[EntryId]] = None,
    ) -> DefaultDict[str, List[str]]:
        exclude_words = self.exclude_words

        include_deps = {dep_direct_obj, dep_obj_of_prep, dep_root, dep_appos}
        # include_deps.add(dep_subj)
        # include_deps.add(dep_sub_clausal)
        # include_deps.add(dep_sub_clausal_pass)

        token_counts = self._get_time_weighted_counts(
            include_deps=include_deps,
            exclude_words=exclude_words,
            entry_ids=entry_ids,
        )

        self.debug_token_counts = token_counts

        category_tree = self._build_initial_category_tree(token_counts)

        merged_category_tree = self._merge_lower_rank_categories(
            category_tree=category_tree,
            token_counts=token_counts,
        )

        return merged_category_tree

    def _get_time_weighted_counts(
        self,
        include_deps: Set[str],
        exclude_words: Set[str],
        entry_ids: Optional[List[EntryId]] = None,
    ) -> Counter:
        min_len = 2
        base = 2

        # set min to 2 weeks and max to encompass all entries
        min_age_exponent = 2
        max_age_exponent = int(1 + math.log(self.age_in_weeks_max, base))

        weighted_token_counts = Counter()

        # at each iteration, consider a time window `base` times the window of the iteration before
        # for example, when `base` is 2, the window doubles every iteration
        for age_exponent in range(min_age_exponent, max_age_exponent + 1):
            age = base ** age_exponent

            token_counts = self._count_tokens_in_time_window(
                max_age=age,
                include_deps=include_deps,
                exclude_words=exclude_words,
                entry_ids=entry_ids,
            )

            for token, count in token_counts.items():
                if len(token) < min_len:
                    continue
                weighted_token_counts[token] = base * weighted_token_counts[token] + count

        return weighted_token_counts

    def _count_tokens_in_time_window_x(
        self, max_age: int, include_deps: Set[str], exclude_words: Set[str],
    ) -> Counter:

        def is_relevant(entry: TokenEntry) -> bool:
            return entry.age <= max_age and entry.dep in include_deps

        token_counts = Counter()
        for token, token_entries in self._token_entry_lookup.items():
            # TODO: Replace with regex
            if any(exclude_word in token for exclude_word in exclude_words):
                continue

            # count only the relevant entries
            token_counts[token] += len([entry for entry in token_entries if is_relevant(entry)])

        return token_counts

    def _count_tokens_in_time_window(
        self,
        max_age: int,
        include_deps: Set[str],
        exclude_words: Set[str],
        entry_ids: Optional[List[EntryId]] = None,
    ) -> Counter:

        allowable_entry_ids = set(entry_ids or [])

        def is_relevant(entry: TokenEntry) -> bool:
            return entry.age <= age and entry.dep in include_deps

        token_counts = Counter()
        for age in range(0, max_age + 1):
            for entry_id in self._ids_by_age.get(age, []):
                if allowable_entry_ids and entry_id not in allowable_entry_ids:
                    continue

                tokens = self._tokens_by_id.get(entry_id, [])
                for token in tokens:
                    if any(exclude_word in token for exclude_word in exclude_words):
                        continue

                    token_entries = self._token_entry_lookup.get(token, [])

                    # count only the relevant entries
                    token_counts[token] += len(
                        [entry for entry in token_entries if
                         entry.id == entry_id and is_relevant(entry)]
                    )

        return token_counts

    def _build_initial_category_tree(self, counts: Counter) -> Dict[str, List[str]]:
        filter_len_min = 3
        merge_len_min = 4

        categories_top_n = 50
        subcategories_top_n = 100

        def filter_token(t):
            if len(t) < filter_len_min:
                return True
            return False

        # get top N most commonly occurring tokens
        tokens = set(chain.from_iterable(
            token[0].split(SPACE) for token in counts.most_common(categories_top_n)))

        # filter the top N into preliminary category names
        categories = [t for t in tokens if not filter_token(t)]

        counts_by_category = Counter()
        category_tree = defaultdict(list)

        num_categories = self._category_count_heuristic(counts)
        log.info(f"num_categories: {num_categories}")
        self.num_categories = num_categories

        for token, count in counts.most_common(subcategories_top_n):
            for category in categories:
                do_modification = False
                if len(category) < merge_len_min:
                    category_spc = category + SPACE
                    spc_category = SPACE + category
                    spc_category_spc = spc_category + SPACE

                    if any([
                        token.startswith(category_spc),
                        token.endswith(spc_category),
                        spc_category_spc in token,
                    ]):
                        do_modification = True

                elif category in token:
                    do_modification = True

                if do_modification:
                    # log.info(f"Merging {token} into {category}")
                    category_tree[category].append(token)
                    counts_by_category[category] += count

        self.debug_counts_by_category = counts_by_category

        counts_and_categories = sorted(
            [(counts_by_category[category], category) for category in category_tree.keys()],
            reverse=True
        )

        pruned_category_tree = {
            cat: category_tree[cat] for _, cat in counts_and_categories[:num_categories]
        }

        return pruned_category_tree

    def _merge_lower_rank_categories(
        self,
        category_tree: CategoryTree,
        token_counts: Counter,
    ) -> DefaultDict[str, List[str]]:
        is_oov = self._text_processor.is_oov
        short_token_len = 4

        tokens_by_rank = deque(
            chain(
                (t for t, _ in token_counts.most_common(300) if SPACE not in t and len(t) > 2),
                (t for t, _ in token_counts.most_common(300) if SPACE in t),
            )
        )

        category_tree = deepcopy(category_tree)

        while tokens_by_rank:
            token = tokens_by_rank.popleft()
            is_short_token = len(token) < short_token_len

            tokens_to_remove = []

            token_spc = token + SPACE
            spc_token = SPACE + token
            spc_token_spc = spc_token + SPACE

            for lower_rank_token in tokens_by_rank:
                do_modification = False
                if (is_oov(token) or is_short_token) and any([
                    lower_rank_token.startswith(token_spc),
                    lower_rank_token.endswith(spc_token),
                    spc_token_spc in lower_rank_token,
                ]):
                    do_modification = True

                elif token in lower_rank_token or lower_rank_token in token:
                    do_modification = True

                if do_modification:
                    try:
                        if lower_rank_token not in category_tree[token]:
                            category_tree[token].append(lower_rank_token)
                    except KeyError:
                        continue

                    tokens_to_remove.append(lower_rank_token)

            for remove_token in tokens_to_remove:
                log.debug(f"removing '{remove_token}' from queue, into '{token}'")
                tokens_by_rank.remove(remove_token)

        return category_tree

    def _build_language_model(self, category: str, category_tree: CategoryTree) -> None:
        tokens_by_id = self._tokens_by_id
        token_entry_lookup = self._token_entry_lookup

        min_len = 3

        lm = self.lm_by_category[category]
        lm.clear()
        for subcategory in category_tree[category]:

            lm_sub = self.lm_by_subcategory[category][subcategory]
            lm_sub.clear()
            for token_entry in token_entry_lookup[subcategory]:
                subcategory_tokens = [t for t in tokens_by_id[token_entry.id] if len(t) >= min_len]
                lm.update(subcategory_tokens)
                lm_sub.update(subcategory_tokens)

    def _build_language_models(self, category_tree: CategoryTree):
        for category in category_tree:
            self._build_language_model(category=category, category_tree=category_tree)

        category_tree = self._merge_categories_on_lm_similarity(category_tree)

        return category_tree

    def _merge_categories_on_lm_similarity(self, category_tree: CategoryTree) -> CategoryTree:
        threshold = 1
        category_tree = deepcopy(category_tree)
        lm_by_category = self.lm_by_category

        categories = deque(category_tree.keys())
        while categories:
            c1 = categories.popleft()
            lesser_categories = deque(categories)
            for c2 in lesser_categories:

                val = self.compare_language_models(lm_by_category[c1], lm_by_category[c2])
                if val <= threshold:
                    category_tree[c1].extend(category_tree[c2])
                    category_tree[c1] = list(set(category_tree[c1]))
                    categories.remove(c2)
                    del category_tree[c2]

        return category_tree

    @classmethod
    def compare_language_models(cls, lm1: Counter, lm2: Counter) -> float:
        common_vocab = lm1.keys() ^ lm2.keys()
        common_counts = [[lm.get(v, 0.001) for v in common_vocab] for lm in (lm1, lm2)]
        return entropy(*common_counts)

    def _get_best_category_for_text(self, category, text) -> Optional[List[Tuple[str, str]]]:
        for category_term in set(category.split(SPACE)):
            if category_term in text:
                break
        else:
            return []

        best_subcategory = self.DEFAULT_SUBCATEGORY
        best_len = 0
        for subcategory in self._category_tree[category]:

            subcategory_terms = set(subcategory.split(SPACE))
            if category_term in subcategory_terms:
                subcategory_terms.remove(category_term)

            for subcategory_term in subcategory_terms:
                if subcategory_term in text:
                    subcategory_len = len(subcategory)
                    if subcategory_len > best_len:
                        best_subcategory = subcategory
                        best_len = subcategory_len
                    break

        return [(category, best_subcategory)]

    def _get_best_language_model_for_text(self, text: str) -> List[Tuple[str, str]]:
        threshold = 8.0

        category_tree = self._category_tree
        scored_similarities: List[Tuple[float, (str, str)]] = []
        for category in category_tree:
            lm = self.lm_by_category[category]
            text_lm = Counter([t for t in text.split(SPACE) if t not in STOP_WORDS])

            score = self.compare_language_models(text_lm, lm)

            scored_similarities.append((score, (category, self.DEFAULT_SUBCATEGORY)))
        # log.info(f"'{text}' {sorted(scored_similarities)}")

        sorted_scored_similarities = sorted(scored_similarities)
        if sorted_scored_similarities[0][0] > threshold:
            return [self.DEFAULT_PAIR]
        return [
            (c, self.DEFAULT_SUBCATEGORY) for score, c in scored_similarities if score < threshold
        ]

    def categorize_text(self, text: str) -> List[Tuple[str, str]]:
        text = self._text_processor.cleanse_text(text)
        text = text.lower().strip()

        if not text:
            return [self.DEFAULT_PAIR]

        category_tree = self._category_tree

        assignments = []
        for category in category_tree:
            result = self._get_best_category_for_text(category, text)
            if result:
                assignments.extend(result)

        if not assignments:
            result = self._get_best_language_model_for_text(text)
            if result:
                assignments.extend(result)
        return assignments

    def categorize_by_entry_id(self, entry_id: EntryId) -> List[Tuple[str, str]]:
        if entry_id not in self.text_by_id:
            return [self.DEFAULT_PAIR]
        entry_text = self.text_by_id[entry_id].strip()
        if not entry_text:
            return [self.DEFAULT_PAIR]

        return self.categorize_text(entry_text)

    def categorize_by_pkey(self, pkey: str) -> List[Tuple[str, str]]:
        entry_id = self._id_by_pkey.get(pkey, None)
        if not entry_id:
            return [self.DEFAULT_PAIR]

        return self.categorize_by_entry_id(entry_id)


class AutoCatHandler:
    KEY_AGE = "autocat1_age"

    def __init__(self):
        self.exclude_words = {"information", "info", "question", "answer", "help"}
        self.text_processor = None
        self.corpus = None

    def load_corpus(
        self,
        df: DataFrame,
        pkey_column_name: str,
        text_column_name: str,
        date_column_name: str,
    ):
        self.text_processor = TextProcessor(parser=spacy_parser)

        max_timestamp = df[date_column_name].max()
        df[self.KEY_AGE] = (max_timestamp - df[date_column_name]).dt.days

        self.corpus = Corpus.from_df(
            df=df,
            pkey_column_name=pkey_column_name,
            text_column_name=text_column_name,
            age_column_name=self.KEY_AGE,
            text_processor=self.text_processor,
        )

    def build_model(self, entry_ids: Optional[List[EntryId]] = None) -> CorpusProcessor:
        if not self.corpus:
            raise ValueError("Corpus has not been loaded")

        corpus_processor = CorpusProcessor(self.corpus, self.exclude_words)
        corpus_processor.build_model(entry_ids)
        return corpus_processor

    def pkeys_to_entry_ids(self, pkeys: List[str]) -> List[EntryId]:
        if not self.corpus:
            log.info(f"Corpus is {self.corpus}")
            raise ValueError()

        id_by_pkey = self.corpus.id_by_pkey
        entry_ids = []
        missing = present = 0
        for pkey in pkeys:

            try:
                entry_ids.append(id_by_pkey[pkey])
                present += 1

            except KeyError:
                log.info(f"pkeys were missing: {pkey}")
                missing += 1

        return entry_ids


autocat_handler = AutoCatHandler()
