#!/usr/bin/env python
"""
Reducer for count words with Hadoop streaming.
"""

from __future__ import print_function
import sys

def main(stdin):
    """
    Aggregate the wordcount pairs.
    """
    (word, count) = (None, 0)
    for line in stdin:
        (new_word, new_count) = line.split()
        # If we've seen this word before, continue tally,...
        if new_word == word:
            count += new_count
        # ...otherwise...
        else:
            # ...print the current tally if not at start...
            if word != None:
                print(("{word}\t{count}").format(word=word, count=count))
            # ...and reset the tally.
            count = new_count
        # Track the last word seen.
        word = new_word
    # At end, print the last tally.
    print(("{word}\t{count}").format(word=word, count=count))
    return None

if __name__ == '__main__':
    main(stdin=sys.stdin)
