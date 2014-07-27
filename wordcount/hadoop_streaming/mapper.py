#!/usr/bin/env python
"""
Mapper for count words with Hadoop streaming.
"""

from __future__ import print_function
import sys

def main(stdin):
    """
    Read in line. Parse line into list of words.
    Print word as a single count.
    """
    for line in stdin:
        words = line.split()
        for word in words:
            print(("{word}\t{count}").format(word=word, count=1))
    return None

if __name__ == '__main__':
    main(stdin=sys.stdin)
