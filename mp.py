from multiprocessing import Pool
import time
import sqlite3

conn = sqlite3.connect('C:/BigData/reddit.db')
subreddits_iterator = conn.cursor()
results = []

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

def task():
    subreddits_iterator.execute("SELECT sub.id, comm.body FROM subreddits sub INNER JOIN "
                                "comments comm ON sub.id == comm.subreddit_id LIMIT 1000")
    for row in subreddits_iterator:
        return row[0], get_words_from_string(row[1])

if __name__ == '__main__':
    start_time = time.time()
    pool = Pool(processes=4)
    subreddits_iterator.execute("SELECT sub.id, comm.body FROM subreddits sub INNER JOIN "
                                "comments comm ON sub.id == comm.subreddit_id LIMIT 10000")
    for row in subreddits_iterator:
        results.append((row[0], pool.apply_async(get_words_from_string, (row[1],)).get(timeout=10)))
    print(results)
    print(time.time() - start_time)