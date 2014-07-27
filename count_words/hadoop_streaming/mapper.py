#!/usr/bin/env python
"""
Mapper for count words with Hadoop streaming.
"""

from __future__ import print_function
import sys

def main(stdin):
    """
    Read in line. Parse line into list of words.
    Sort and tally words. Print tallies.
    """
    for line in stdin:
        words = line.split()
        counts = map(words.count, words)
        tallies = sorted(set(zip(words, counts)))
        for (word, count) in tallies:
            print(("{word}\t{count}").format(word=word, count=count))
    return None

if __name__ == '__main__':
    main(stdin=sys.stdin)
