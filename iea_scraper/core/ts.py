from collections import defaultdict
import json
import re

from iea_scraper.core.utils import get_dimension_db_data


def auto_mapping(mapped_dimension, text):
    mapping = defaultdict(list)
    norma = Normalizer()
    for dimension in mapped_dimension:
        data = get_dimension_db_data(dimension)
        names = {x['code']: get_vocab_list(x, norma) for x in data
                 if x['code'] != 'None'}
        for code, vocabs in names.items():
            for vocab in vocabs:
                if vocab in norma.normalize(text):
                    mapping[dimension] = code
    return mapping


def mapping(texts, mapped_dimension):
    normalizer = Normalizer()
    texts = [normalizer.normalize(text) for text in texts]

    maps = {}
    for dimension in mapped_dimension:
        data = get_dimension_db_data(dimension)
        maps[dimension] = {x['code']: get_vocab_list(x, normalizer)
                           for x in data if x['code'] != 'None'}
    mappings = [get_one_mapping(text, maps) for text in texts]
    return mappings


def get_one_mapping(text, maps):
    mapping = {}
    for dimension, dim_maps in maps.items():
        first_word = float('inf')
        for code, vocab_list in dim_maps.items():
            for vocab in vocab_list:
                position = text.find(vocab)
                if (position > -1) and (position < first_word):
                    mapping[dimension] = code
                    first_word = position
    return mapping


class Normalizer:
    def __init__(self):
        pass

    def normalize(self, text):
        text = str(text)
        text = text.replace('$', 'dollars')
        pattern = r'[^a-zA-z0-9\s]'
        text = re.sub(pattern, ' ', text)
        return text.lower()


def get_vocab_list(x, norma):
    vocab = [x['code'], x['long_name']]
    try:
        vocab.extend(json.loads((x['meta_data']))['keywords'])
    except Exception:
        pass
    vocab = [norma.normalize(x) for x in vocab]
    return vocab
