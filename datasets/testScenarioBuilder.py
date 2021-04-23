
import argparse
from typing import List, Optional, Iterator, Dict
from dataclasses import dataclass
import dataclasses
from pathlib import Path
from collections import defaultdict, OrderedDict
import gzip
import itertools
import re
import json

NOT_ALPHANUM = re.compile("[^A-Za-z0-9]")


def tokenize(tokens: str) -> List[str]:
    return [token.lower() for token in NOT_ALPHANUM.split(tokens) if token]

@dataclass
class Entry():
    doc_id: int
    product_id: str
    helpfulness_numerator: int
    helpfulness_denominator: int
    score: int
    tokens: List[str]




def entry_to_typed(entry: Dict, doc_id: int) -> Entry:
    helpfulness = entry["review/helpfulness"]
    helpfulnessSlash = helpfulness.index("/")
    helpfulness_numerator = int(helpfulness[:helpfulnessSlash])
    helpfulness_denominator = int(helpfulness[helpfulnessSlash+1:])

    product_id = entry["product/productId"]
    score = int(float(entry["review/score"]))

    review_text = entry["review/text"]

    return Entry(
        doc_id, product_id, helpfulness_numerator, helpfulness_denominator, score, 
        tokenize(review_text))

@dataclass
class Scenario:
    total_tokens: int
    unique_tokens: int
    num_reviews: int
    product_id_to_entry: OrderedDict[str, List[Entry]]
    term_to_postings: OrderedDict[str, List[int]]
    term_to_collection_frequency: Dict[str, int]
    product_id_to_review_ids: Dict[str, List[int]]
    

def scenario_builder(entries: Iterator[Entry]) -> Scenario:
    total_tokens = 0
    product_id_to_entry = defaultdict(list)
    num_reviews = 0
    term_to_postings = defaultdict(list)
    product_id_to_review_ids = defaultdict(OrderedDict)
    term_to_collection_frequency = defaultdict(lambda: 0)

    for entry in sorted(entries, key=lambda entry: entry.product_id):
        product_id_to_entry[entry.product_id].append(entry)

        product_id_to_review_ids[entry.product_id][entry.doc_id] = True

        num_reviews += 1

        token_to_freq_in_doc = defaultdict(lambda: 0)
        for token in entry.tokens:
            token_to_freq_in_doc[token] += 1

        for token, freq_in_doc in token_to_freq_in_doc.items():
            term_to_postings[token].append((entry.doc_id, freq_in_doc))
            term_to_collection_frequency[token] += freq_in_doc
    
        total_tokens += len(entry.tokens)

    unique_tokens = len(term_to_postings)

    # sort stuff
    product_id_to_entry = OrderedDict(sorted(product_id_to_entry.items()))
    for term, postings in term_to_postings.items():
        flat_postings = []
        for doc_id, freq in sorted(postings, key=lambda tup: tup[0]):
            flat_postings.extend([doc_id, freq])
        term_to_postings[term] = flat_postings
    term_to_postings = OrderedDict(sorted(term_to_postings.items()))
    product_id_to_review_ids = { product_id: list(dic.keys()) for product_id, dic in product_id_to_review_ids.items()}
    term_to_collection_frequency = dict(term_to_collection_frequency)

    return Scenario(
        total_tokens=total_tokens,
        unique_tokens=unique_tokens,
        num_reviews=num_reviews,
        product_id_to_entry=product_id_to_entry,
        term_to_postings=term_to_postings,
        term_to_collection_frequency=term_to_collection_frequency,
        product_id_to_review_ids=product_id_to_review_ids
    )

def parse(filename: Path) -> Iterator[Entry]:
    if filename.suffix == ".gz":
        f = gzip.open(filename, 'rt')
    else:
        f = open(filename, 'r')
    entry = {}
    doc_id = 1
    for l in f:
        l = l.strip()
        colonPos = l.find(':')
        if colonPos == -1:
            if entry:
                yield entry_to_typed(entry, doc_id)
                doc_id += 1
            entry = {}
            continue
        eName = l[:colonPos]
        rest = l[colonPos+2:]
        entry[eName] = rest
    if entry:
        yield entry_to_typed(entry, doc_id)
        doc_id += 1

    f.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Creates .json files for testing IndexReader")
    parser.add_argument("file_path", metavar='file', type=Path, help="Either a .txt or a .tar.gz file")
    args = parser.parse_args()

    filename: Path = vars(args)['file_path']

    scenario = scenario_builder(parse(filename))


    out_path = filename.with_suffix(".json")
    with open(out_path, 'w', encoding='utf-8') as out_file:
        json.dump(dataclasses.asdict(scenario),  out_file, ensure_ascii=False, indent=4)
