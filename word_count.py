from mrjob.job import MRJob
from vocabularies import get_words_from_string
import sqlite3
import time

class MRWordCount(MRJob):

    def mapper(self, key, comment):
        for word in get_words_from_string(comment):
            yield (word.lower(), 1)

    def reducer(self, key, values):
        if (sum(values) == 1):
            yield key, None

if __name__ == '__main__':
    MRWordCount.run()