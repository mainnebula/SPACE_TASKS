"""
usage:

print(gen_username())
"""
from random import randint

# create a wordlist from the keywords file
wlist = open("words.txt", "r").read()
names = wlist.split("\n")

def gen_username():
    words = []

    for word in names:
        words.append(word)

    n_words = len(words) # there's a small joke in here haha

    first = words[randint(0, n_words)]
    second = words[randint(0, n_words)]
    
    # generate
    username = first+second
    return username

