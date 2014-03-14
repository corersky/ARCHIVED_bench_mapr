#! /usr/bin/env python
"""
count_words.py

Count words from a .txt file. Output to .csv.
Example usage: $ python count_words.py ./input.txt output.csv

Adapted from disco/examples/utils/count_words.py, wordcount.py, simple_innerjoin.py.
"""

import sys
import csv
from disco.core import Job, result_iterator
from disco.util import kvgroup

class CountWords(Job):
    # 5 partitions for 5 slave nodes: scout02-06
    partitions = 5
    sort = True

    def map(self, line, params):
        for word in line.split():
            yield word, 1

    def reduce(self, iter, params):
        for word, counts in kvgroup(sorted(iter)):
            yield word, sum(counts)

if __name__ == '__main__':
    input_filename = "input.txt"
    output_filename = "output.csv"
    if len(sys.argv) > 1:
        input_filename = sys.argv[1]
        if len(sys.argv) > 2:
            output_filename = sys.argv[2]

    from count_words import CountWords
    job = CountWords().run(input=[input_filename])

    with open(output_filename, 'w') as fp:
        writer = csv.writer(fp)
        for word, counts in result_iterator(job.wait(show=True)):
            writer.writerow(word, count)
