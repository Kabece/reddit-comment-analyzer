import operator
import sqlite3
import sys
import time

threads_dict = dict()
size_dict = dict()

def task(row):
    cursor = conn.cursor()
    depth = 0
    cursor.execute("SELECT id FROM comments WHERE parent_id = ?", (row[0],))
    new_row = cursor.fetchone()
    while (new_row is not None):
        depth += 1
        cursor.execute("SELECT id FROM comments WHERE parent_id = ?", (new_row[0],))
        new_row = cursor.fetchone()
    return (row[1], depth)

# def iterate_over_comments():
#     global threads_dict
#     pool = multiprocessing.Pool(processes=multiprocessing.cpu_count())
#     comments_iterator.execute("SELECT id, subreddit_id, parent_id FROM comments"
#                               " WHERE parent_id LIKE 't3%' LIMIT 100000")
#     while(True):
#         rows = comments_iterator.fetchmany(10000)
#         if len(rows) > 0:
#             results = pool.map_async(task, rows)
#             for result in results.get():
#                 thread_sizes_list = threads_dict.get(result[0])
#                 if thread_sizes_list is None:
#                     thread_sizes_list = [result[1]]
#                 else:
#                     thread_sizes_list.append(result[1])
#                 threads_dict.update({result[0] : thread_sizes_list})
#         else:
#             break
#     pool.close()
#     pool.join()

def iterate_over_comments():
    global threads_dict
    comments_iterator.execute("SELECT id, subreddit_id, parent_id FROM comments"
                              " WHERE parent_id LIKE 't3%'")
    while(True):
        rows = comments_iterator.fetchmany(10000)
        if len(rows) > 0:
            for row in rows:
                result = task(row)
                thread_sizes_list = threads_dict.get(result[0])
                if thread_sizes_list is None:
                    thread_sizes_list = [result[1]]
                else:
                    thread_sizes_list.append(result[1])
                threads_dict.update({result[0] : thread_sizes_list})
        else:
            break

def calculate_average():
    for key, value in threads_dict.items():
        threads_dict.update({key: (sum(value) / len(value))})

def print_sorted_vocabularies():
    global threads_dict
    sortedv = sorted(threads_dict.items(), key=operator.itemgetter(1), reverse=True)
    i = 0
    cursor = conn.cursor()
    print ("Ten subreddits with deepest average comment threads: ")
    for key, value in sortedv:
        if i < 10:
            name = cursor.execute("SELECT name FROM subreddits WHERE id = ?", (key,))
            print("[Subreddit ID: %s | Subreddit name: %s | Average thread depth: %s]" % (key, name.fetchone()[0], value))
            i = i + 1
        else:
            break

if __name__ == '__main__':
    start_time = time.time()
    conn = sqlite3.connect(sys.argv[1])
    comments_iterator = conn.cursor()
    iterate_over_comments()
    calculate_average()
    print_sorted_vocabularies()
    print("Execution time: %s seconds." % (time.time() - start_time))