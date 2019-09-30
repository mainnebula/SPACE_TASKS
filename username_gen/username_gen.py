import random
import re

regex = re.compile('[^a-zA-Z]') # to remove non-letter characters

# create list of all keywords from text file
f = open("keywords.txt", "r").read()
_keywords = f.split("\n")
keywords = []
for k in _keywords:
    sample = regex.sub('', k)
    if sample != '':
        keywords.append(sample)

# generate unique username
total_keywords = len(keywords) - 1
_username = []
while True:
    x = random.randint(0,total_keywords)
    if keywords[x] not in _username:
        _username.append(keywords[x])
    if len(_username) == 2:
        break

username = "".join(_username)
print(username)
