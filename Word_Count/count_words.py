#!/usr/bin/env python
"""
count_words.py

Count words from a .txt file. Output to .csv.

Example usage:
Assuming input.txt is a 5G files and you have 5 slave nodes:
$ split --line-bytes=1G input.txt
$ ddfs push data:inputtxt ./xa?
$ python count_words.py data:inputtxt output.csv

Adapted from
disco/examples/utils/count_words.py, wordcount.py, simple_innerjoin.py
http://disco.readthedocs.org/en/latest/start/tutorial_2.html

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
        for word, count in kvgroup(rows_iter):
            out.add(word, sum(count))

if __name__ == '__main__':

    # # TODO: allow running without argucments
    if len(sys.argv) != 3:
        print "ERROR: Wrong number of arguments."
        print "  Example usage:"
        print "  Assuming input.txt is a 5G file and you have 5 slave nodes:"
        print "  $ split --line-bytes=1G input.txt"
        print "  $ ddfs push data:inputtxt ./xa?"
        print "  $ python count_words.py data:inputtxt output.csv"
        sys.exit()
    else:
        # TODO: check input is tag
        input_tag = sys.argv[1]
        output_filename = sys.argv[2]

    # Necessary to import since slave nodes do not have
    # same namespace as master.
    from count_words import CountWords
    job = CountWords().run(input=[input_tag])

    with open(output_filename, 'w') as fp:
        writer = csv.writer(fp, quoting=csv.QUOTE_NONNUMERIC)
        for word, count in result_iterator(job.wait(show=True)):
            writer.writerow([word, count])
