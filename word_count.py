from mrjob.job import MRJob
from vocabularies import get_words_from_string
import sqlite3
import time

class MRWordCount(MRJob):

    def configure_args(self):
        super(MRWordCount, self).configure_args()
        self.add_file_arg('--database')

    def mapper_init(self):
        self.sqlite_conn = sqlite3.connect(self.options.database)
        subreddits_iterator = self.sqlite_conn.cursor()


    def mapper(self, key, value):
        for word in get_words_from_string(value):
            yield (word.lower(), 1)

    def reducer(self, key, values):
        yield (key, sum(values))

if __name__ == '__main__':
    MRWordCount.run()