import sqlite3
import time
import multiprocessing
import operator

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
    return (row[0], get_words_from_string(row[1]))

def iterate_over_comments():
    global vocabularies_dict
    pool = multiprocessing.Pool(processes=multiprocessing.cpu_count())
    subreddits_iterator.execute("SELECT subreddit_id, body from comments");
    while(True):
        rows = subreddits_iterator.fetchmany(10000)
        if len(rows) > 0:
            result = pool.map_async(task, rows)
            for tuple in result.get():
                voc_ser = vocabularies_dict.get(tuple[0])
                if voc_ser is None:
                    voc_ser = set(tuple[1])
                else:
                    voc_ser.update(tuple[1])
                vocabularies_dict.update({tuple[0] : voc_ser})
        else:
            break
    pool.close()
    pool.join()

def print_sorted_vocabularies():
    global vocabularies_dict
    for key, value in vocabularies_dict.items():
        vocabularies_dict.update({key : len(value)})
    sortedv = sorted(vocabularies_dict.items(), key=operator.itemgetter(1), reverse=True)
    i = 0
    cursor = conn.cursor()
    print ("Ten subreddits with largest vocabularies: ")
    for key, value in sortedv:
        if i < 10:
            name = cursor.execute("SELECT name FROM subreddits WHERE id = ?", (key,))
            print("[Subreddit ID: %s | Subreddit name: %s | Vocabulary size: %s]" % (key, name.fetchone()[0], value))
            i = i + 1
        else:
            break


if __name__ == '__main__':
    start_time = time.time()
    iterate_over_comments()
    print_sorted_vocabularies()
    print("Execution time: %s seconds." % (time.time() - start_time))