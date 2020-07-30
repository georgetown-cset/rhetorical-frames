"""
Prepare content from Foreign Affairs, Reuters, and Defense One for annotation.

Input data is in `data/source`; output is written to `data/for_annotation`.
"""
import json
import re
from pathlib import Path
from random import shuffle

import pandas as pd
import spacy

from matcher import create_matcher


def process_foreign_affairs():
    records = split_factiva('data/source/foreign-affairs')
    hits = list(keyword_filter(records))
    print(f"{len(hits)} docs containing keywords")
    write_jsonl(hits, 'foreign_affairs_20200114.jsonl')


def process_reuters():
    records = split_factiva('data/source/reuters', author_date=False)
    write_jsonl(records, 'reuters_20200114.jsonl')


def process_defense_one():
    keep_keys = ['id', 'title', 'text', 'author', 'date']
    records = []
    with open('data/source/defense-one/defense_one_20200108.jsonl', 'rt') as f:
        for line in f:
            record = json.loads(line)
            record['id'] = record['link']
            record['author'] = '\n'.join(record['bio'])
            record['text'] = record['full_text']
            date_match = re.search(
                r"((Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec) \d{1,2}, \d{4})", record['snippet'])
            if date_match:
                record['date'] = date_match.group()
            else:
                record['date'] = None
            record = {k: v for k, v in record.items() if k in keep_keys}
            records.append(record)
    # Deduplicate
    records = {r["id"]: r for r in records}
    records = list(records.values())
    write_jsonl(records, 'defense_one_20200114.jsonl')


def split_factiva(path, author_date=True):
    """Split Factiva results into records.
    """
    text_paths = list(Path(path).glob('*.txt'))
    assert text_paths
    print(f'{len(text_paths)} export files')
    records = []
    for path in text_paths:
        text = path.read_text()
        # Articles in each file are split by Factiva document ID numbers. The parentheses capture this document ID,
        # so the resulting list is like [text 1, id 1, text 2, id 2, ...]. Note that each export item ends with
        # search metadata. We omit this implicitly because it isn't followed by a document ID, so the 'i' iterator
        # below exhausts first.
        matches = re.split(r'\nDocument ([\d\w]+)\n', text)
        for a, i in pairwise(matches):
            # print(f'{len(articles)} articles in export')
            # for a in articles:
            grafs = a.split('\n')
            grafs = [p.strip() for p in grafs if p.strip()]
            try:
                record = {
                    "id": i,
                    "title": grafs.pop(0),
                }
            except IndexError:
                print(f'Skipping empty result {i}: {a}')
                continue
            if author_date:
                # If true (for Foreign Affairs), the first two metadata lines are author and publication date
                record["author"] = grafs.pop(0)
                record["date"] = grafs.pop(1)
            else:
                # Otherwise (for Reuters), the author appears below the metadata block, before the full text,
                # and we look for it with regex
                for j, graf_text in enumerate(grafs):
                    author_match = re.search(r"^By ([\w\s.]+)$", graf_text)
                    if author_match:
                        record["author"] = author_match.group(1)
                        # Exclude from text
                        grafs.pop(j)
                    if j > 14:
                        continue
                # Author takes a null value if we were unsuccessful
                if "author" not in record:
                    record["author"] = None
                # Similarly, we look for the a date pattern in the first 15 lines
                for j, graf_text in enumerate(grafs):
                    date_match = re.search(
                        r"(^\d+ (January|February|March|April|May|June|July|August|September|October|November|December) \d{4})$",
                        graf_text)
                    if date_match:
                        record["date"] = date_match.group()
                        grafs.pop(j)
                    if j > 14:
                        continue
                # If unsuccessful, date will be null
                if "date" not in record:
                    record["date"] = None
            record["text"] = grafs
            records.append(record)
    print(f'{len(records)} extracted docs')
    # Deduplicate by id
    records = {r["id"]: r for r in records}
    print(f'{len(records)} uniquely-id\'d docs')
    records = list(records.values())
    return records


def pairwise(iterable):
    """Iterate over a list pairwise.

    s -> (s0, s1), (s2, s3), (s4, s5), ...
    https://stackoverflow.com/questions/5389507/iterating-over-every-two-elements-in-a-list
    """
    a = iter(iterable)
    return zip(a, a)


def write_jsonl(records, filename):
    """Write out records to JSONL in shuffled order."""
    shuffle(records)
    with open(Path('data', 'for_annotation', filename), 'wt') as f:
        for r in records:
            f.write(json.dumps(r) + '\n')


def keyword_filter(records):
    """Filter articles to those with AI keywords in title or text."""
    matcher = create_matcher()
    for r in records:
        search_text = r["title"] + ". " + " ".join(r["text"])
        matches = matcher.match(search_text)
        if matches:
            print(matches)
            yield r


def validate_annotation_inputs():
    """Check shape, missingness, and types in resulting annotation inputs."""
    input_paths = Path('data', 'for_annotation').glob('*.jsonl')
    for path in input_paths:
        with open(path, 'rt') as f:
            for line in f:
                record = json.loads(line)
                assert isinstance(record, dict)
                # We require an id string
                assert record['id'] is not None and record['id'].strip() != ''
                keys = ['id', 'title', 'author', 'date']
                # Some of these fields are nullable, but all must exist and they should be strings
                for k in keys:
                    if record[k] is not None:
                        assert isinstance(record[k], str)
                # text should give a non-empty list
                assert isinstance(record['text'], list)
                assert len(record['text'])


def summarize_annotation_inputs():
    """Summarize missingness and other counts in the resulting annotation inputs."""
    input_paths = Path('data', 'for_annotation').glob('*.jsonl')
    en = spacy.load('en_core_web_sm', disable=['parser', 'tagger', 'ner'])
    for path in input_paths:
        df = pd.read_json(path, lines=True)
        print(f'Read {df.shape[0]} records from {path.name}')
        print(f"Null titles: {count_null(df['title'])}")
        print(f"Null authors: {count_null(df['author'])}")
        print(f"Null dates: {count_null(df['date'])}")
        full_text = df['text'].apply(lambda x: '\n'.join(x))
        docs = list(en.pipe(text for text in full_text.values))
        words = pd.Series([len(doc) for doc in docs])
        print('Characters:')
        print(full_text.apply(len).describe().astype(int))
        print('Words:')
        print(words.describe().astype(int))
        print(f'Total words: {words.sum()}')
        print('Paragraphs:')
        print(df['text'].apply(len).describe().astype(int))
        print('')


def count_null(series):
    return (series.apply(lambda x: pd.isnull(x) or x == '')).sum()


if __name__ == '__main__':
    process_foreign_affairs()
    process_reuters()
    process_defense_one()
    validate_annotation_inputs()
    summarize_annotation_inputs()
