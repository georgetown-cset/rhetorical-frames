"""Find matches for keyphrases."""
import argparse
import json
import logging
from multiprocessing import current_process, Pool
from pathlib import Path
from typing import List, Dict, Union

import spacy
from srsly import read_jsonl
from srsly.util import force_path


class Matcher(object):
    regex_symbols = r'+*[]\\^$.'
    wrap_regex = True

    def __init__(self, patterns=None, model='en_core_web_sm', label='KEYWORD', **kw):
        # By convention, the spaCy Lang instance is called nlp
        self.nlp = self._create_pipeline(model=model, **kw)
        self.label = label
        if patterns is not None:
            self.add_patterns(patterns)
        self.log = logging.getLogger(__name__)

    def add_patterns(self, patterns: List[Dict]):
        ruler = self.nlp.get_pipe('entity_ruler')
        ruler.add_patterns(patterns)

    def load_patterns(self, path: Union[str, Path]):
        path = force_path(path)
        patterns = []
        for line in path.open('rt').readlines():
            if not line.startswith('#'):
                patterns.append({'label': self.label, 'pattern': self._parse_pattern(line.strip())})
        self.add_patterns(patterns)

    def match(self, text: str):
        doc = self.nlp(text)
        matches = [ent.text for ent in doc.ents if ent.label_ == self.label]
        return matches

    def create_doc(self, text: str):
        return self.nlp(text)

    @staticmethod
    def _create_pipeline(model: str, **kw):
        pipeline = spacy.load(model, **kw)
        ruler = pipeline.create_pipe('entity_ruler')
        pipeline.add_pipe(ruler, last=True)
        return pipeline

    def _parse_pattern(self, text: str):
        pattern = []
        for token in text.split(' '):
            if any((s in token for s in self.regex_symbols)):
                if self.wrap_regex:
                    rule = {'LOWER': {'REGEX': '^' + token.lower() + '$'}}
                else:
                    rule = {'LOWER': {'REGEX': token.lower()}}
            else:
                rule = {'LOWER': token.lower()}
            pattern.append(rule)
        return pattern


def create_matcher():
    matcher = Matcher()
    keyword_path = Path(__file__).parent / 'data' / 'keywords.txt'
    matcher.load_patterns(keyword_path)
    return matcher


def match(matcher, record, key):
    matches = matcher.match(record.get(key))
    output = json.dumps({
        'id': record.get('id'),
        'matches': matches if matches else None,
        'cats': record['cats'],
        'binary_cats': record['binary_cats']
    })
    pid = current_process().pid
    with open(f'output_{pid}.jsonl', 'at') as f:
        f.write(output + '\n')


def main(path, output_path='data/matcher_output.jsonl', key='text'):
    matcher = create_matcher()
    records = read_jsonl(path)
    for path in Path().glob('output_*.jsonl'):
        path.unlink()
    with Pool(processes=12) as pool:
        params = ((matcher, r, key) for r in records)
        pool.starmap(match, params)
    with Path(output_path).open('wt') as outfile:
        for pid_file in Path().glob('output_*.jsonl'):
            for record in pid_file.open('rt'):
                outfile.write(record)
            pid_file.unlink()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Find keyphrase matches')
    parser.add_argument('path', help='Path to text file with one keyphrase pattern per line)')
    parser.add_argument('output', help='Output file')
    parser.add_argument('--workers', '-w', default=12)
    parser.add_argument('--key', '-k', default='text')
    args = parser.parse_args()
    main(path=args.path, output_path=args.output, key=args.key)
