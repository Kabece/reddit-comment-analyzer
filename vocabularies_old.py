import sqlite3
import time
from multiprocessing import Pool

conn = sqlite3.connect('C:/BigData/reddit.db', check_same_thread=False)
subreddits_iterator = conn.cursor()
cursor = conn.cursor()
vocabularies_list = []

# Copyright to David Kofoed Wind
def get_words_from_string(s):
    symbols = ['\n', '`', '~', '!', '@', '#', '$', '%', '^', '&', '*', '(', ')', '_', '-', '+', '=', '{', '[', ']', '}',
               '|', '\\', ':', ';', '"', "'", '<', '>', '.', '?', '/', ',']
    s = s.lower()
    for sym in symbols:
        s = s.replace(sym, " ")
    words = set()
    for w in s.split(" "):
        if len(w.replace(" ", "")) > 0:
            words.add(w)
    return words

def iterate_over_subreddits():
    subreddits_iterator.execute("SELECT id FROM subreddits LIMIT 1")
    for row in subreddits_iterator:
        size = calculate_subreddit_vocabulary_size(row[0])
        vocabularies_list.append((row, size))
    print(vocabularies_list)

def calculate_subreddit_vocabulary_size(subreddit_id):
    pool = Pool(processes=4)
    unique_words = set()
    cursor.execute("SELECT comm.body "
                   "FROM subreddits sub INNER JOIN comments comm "
                   "WHERE sub.id = ?", (subreddit_id,))
    while(True):
        rows = cursor.fetchmany(4)
        if len(rows) > 0:
              for row in rows:
                unique_words.update(pool.apply_async(get_words_from_string, (row[0],)).get())
        else:
            break
    return len(unique_words)

if __name__ == '__main__':
    start_time = time.time()
    iterate_over_subreddits()
    print(time.time() - start_time)