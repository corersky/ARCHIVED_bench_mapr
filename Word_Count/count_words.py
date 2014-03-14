#! /usr/bin/env python
"""
count_words.py

Count words from a .txt file. Output to .csv.

Example usage:
$ ddfs chunk data:inputtxt ./input.txt
$ python count_words.py data:inputtxt output.csv

Adapted from disco/examples/utils/count_words.py, wordcount.py, simple_innerjoin.py
and http://disco.readthedocs.org/en/latest/start/tutorial_2.html

TODO:
Add help text.
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

    def reduce(self, rows_iter, out, params):
        # Is sorted necessary if sort = True?
        # for word, count in kvgroup(sorted(rows_iter)):
        for word, count in kvgroup(rows_iter):
            out.add(word, sum(count))

if __name__ == '__main__':
    input_filename = "input.txt"
    output_filename = "output.csv"
    if len(sys.argv) > 1:
        input_filename = sys.argv[1]
        if len(sys.argv) > 2:
            output_filename = sys.argv[2]

    # Necessary to import since slave noes do not have
    # same namespace as master.
    from count_words import CountWords
    job = CountWords().run(input=[input_filename])

    with open(output_filename, 'w') as fp:
        writer = csv.writer(fp)
        for word, count in result_iterator(job.wait(show=True)):
            writer.writerow([word] + count)
