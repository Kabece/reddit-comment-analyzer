import sqlite3

conn = sqlite3.connect('D:/reddit.db')
cursor = conn.cursor()

def get_comment_bodies():
    cursor.execute('SELECT * FROM subreddits LIMIT 10')
    for row in cursor:
        print(row)

get_comment_bodies()