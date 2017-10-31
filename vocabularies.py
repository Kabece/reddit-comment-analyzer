import sqlite3

conn = sqlite3.connect('D:/reddit.db')
cursor = conn.cursor()

# Copyright to David Kofoed Wind
def get_words_from_string():
    symbols = ['\n', '`', '~', '!', '@', '#', '$', '%', '^', '&', '*', '(', ')', '_', '-', '+', '=', '{', '[', ']', '}',
               '|', '\\', ':', ';', '"', "'", '<', '>', '.', '?', '/', ',']

    s = """**Most Popular Comments**   \n\n---\n|Score|Author|Post Title|Link to comment|\n|:-|-|-|-|\n|4186|/u/DrowningDream|[WP] A Man gets to paradise. Unfortunately, Lucifer won the War"""

    s = s.lower()
    for sym in symbols:
        s = s.replace(sym, " ")

    words = set()
    for w in s.split(" "):
        if len(w.replace(" ", "")) > 0:
            words.add(w)

    print(words)

def get_comment_bodies():
    cursor.execute('SELECT * FROM subreddits LIMIT 10')
    for row in cursor:
        print(row)

# get_comment_bodies()
get_words_from_string()