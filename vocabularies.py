import sqlite3
import time
import multiprocessing

conn = sqlite3.connect('C:/BigData/reddit.db')
subreddits_iterator = conn.cursor()
vocabularies_dict = dict()

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

def task(row):
    return {row[0]: get_words_from_string(row[1])}

def iterate_over_subreddits():
    global vocabularies_dict
    pool = multiprocessing.Pool(processes=multiprocessing.cpu_count())
    subreddits_iterator.execute("SELECT subreddit_id, body from comments LIMIT 1000");
    while(True):
        rows = subreddits_iterator.fetchmany(100)
        if len(rows) > 0:
            result = pool.map_async(task, rows)
            for dict in result.get():
                vocabularies_dict.update(dict)
        else:
            break
    pool.close()
    pool.join()

def print_sorted_vocabularies():
    global vocabularies_dict
    for vocabulary in vocabularies_dict.keys():
        print("[%s : %s]" % (vocabulary, len(vocabularies_dict.get(vocabulary))))

if __name__ == '__main__':
    start_time = time.time()
    iterate_over_subreddits()
    print_sorted_vocabularies()
    print(time.time() - start_time)