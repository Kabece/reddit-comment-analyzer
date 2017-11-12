import sqlite3
import time
import multiprocessing
import operator
import itertools
import sys


vocabularies_dict = dict()
size_dict = dict()

def iterate_over_comments():
    global vocabularies_dict
    subreddits_iterator.execute("SELECT subreddit_id, author_id FROM comments");
    for row in subreddits_iterator:
        subs_set = vocabularies_dict.get(row[0])
        if subs_set is None:
            subs_set = {row[1]}
        else:
            subs_set.update([row[1]])
        vocabularies_dict.update({row[0] : subs_set})
    return vocabularies_dict

def task(tup):
    return len(tup[0].intersection(tup[1]))

def find_common_authors():
    global vocabularies_dict
    global size_dict
    pairs = itertools.combinations(vocabularies_dict.keys(), 2)
    for pair in pairs:
        size = len(vocabularies_dict[pair[0]].intersection(vocabularies_dict[pair[1]]))
        if size > 0:
            size_dict.update({(pair[0], pair[1]) : size})

def print_sorted_vocabularies():
    global size_dict
    sortedv = sorted(size_dict.items(), key=operator.itemgetter(1), reverse=True)
    i = 0
    cursor = conn.cursor()
    cursor2 = conn.cursor()
    print ("Ten subreddits with largest number of common authors: ")
    for key, value in sortedv:
        if i < 10:
            name = cursor.execute("SELECT name FROM subreddits WHERE id = ?", (key[0],))
            name2 = cursor2.execute("SELECT name FROM subreddits WHERE id = ?", (key[1],))
            print("[Subreddit 1: %s | Subreddit 2: %s | Common authors: %s]" % (name.fetchone()[0], name2.fetchone()[0], value))
            i = i + 1
        else:
            break

if __name__ == '__main__':
    start_time = time.time()
    conn = sqlite3.connect(sys.argv[1])
    subreddits_iterator = conn.cursor()
    iterate_over_comments()
    find_common_authors()
    print_sorted_vocabularies()
    print("Execution time: %s seconds." % (time.time() - start_time))