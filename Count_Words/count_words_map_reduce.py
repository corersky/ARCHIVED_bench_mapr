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
Automatically determine partitions from config file and automatically chunk data
"""

import sys
import csv
from disco.ddfs import DDFS
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

    if len(sys.argv) != 3:
        sys.stderr.write(
            """ERROR: Wrong number of arguments.
  Example usage:
  Assuming input.txt is a 5G file and you have 5 slave nodes:
  $ split --line-bytes=1G input.txt"
  $ ddfs push data:inputtxt ./xa?"
  $ python count_words.py data:inputtxt ouput.txt
""")
        sys.exit(1)

    input_tag = sys.argv[1]
    output_filename = sys.argv[2]
    if not DDFS().exists(input_tag):
        sys.stderr.write(
            "ERROR: " + input_tag + """ is not a tag in Disco Distributed File System.
  Example usage:
  Assuming input.txt is a 5G file and you have 5 slave nodes:
  $ split --line-bytes=1G input.txt"
  $ ddfs push data:inputtxt ./xa?"
  $ python count_words.py data:inputtxt ouput.txt
""")
        sys.exit(1)

    # Necessary to import since slave nodes do not have
    # same namespace as master.
    from count_words import CountWords
    job = CountWords().run(input=[input_tag])

    with open(output_filename, 'w') as fp:
        writer = csv.writer(fp, quoting=csv.QUOTE_NONNUMERIC)
        for word, count in result_iterator(job.wait(show=True)):
            writer.writerow([word, count])
