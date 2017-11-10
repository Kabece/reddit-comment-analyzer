from mrjob.job import MRJob
from vocabularies import get_words_from_string
import sqlite3
import time

class SqliteJob(MRJob):

    def configure_args(self):
        super(SqliteJob, self).configure_args()
        self.add_file_arg('--database')

    def mapper_init(self):
        # make sqlite3 database available to mapper
        self.sqlite_conn = sqlite3.connect(self.options.database)
        self.subreddits_iterator = self.sqlite_conn.cursor()

    def mapper(self, key, _):
        self.subreddits_iterator.execute("SELECT sub.id, comm.body FROM subreddits sub INNER JOIN "
                                    "comments com ON sub.id == comm.subreddit_id LIMIT 1")


    def reducer(self, key, values):
        if (sum(values) == 1):
            yield key, None

if __name__ == '__main__':
    MRWordCount.run()