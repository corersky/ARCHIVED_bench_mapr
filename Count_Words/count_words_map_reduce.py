#!/usr/bin/env python
"""
Count words from a .txt file. Output to .csv.

TODO:
- check ddfs tag exists
- check disco v0.4.4
"""

import argparse
import csv
from disco.ddfs import DDFS
from disco.core import Job, result_iterator
from disco.util import kvgroup
from disco.func import chain_reader

def main(file_in="input.txt", file_out="output.csv"):

    # TODO: Rename tag data:count_words1 if tag exists.
    # Disco v0.4.4 requrires that ./ prefix the file to identify as a local file.
    # http://disco.readthedocs.org/en/0.4.4/howto/chunk.html#chunking
    tag = "data:count_words"
    DDFS().chunk(tag=tag, urls=["./"+file_in])
    try:
        # Import since slave nodes do not have same namespace as master.
        from count_words_map_reduce import CountWords
        job = CountWords().run(input=[tag], map_reader=chain_reader)
        with open(file_out, 'w') as f_out:
            writer = csv.writer(f_out, quoting=csv.QUOTE_NONNUMERIC)
            for word, count in result_iterator(job.wait(show=True)):
                writer.writerow([word, count])
    finally:
        DDFS().delete(tag=tag)
    return None

class CountWords(Job):

    def map(self, line, params):
        for word in line.split():
            yield word, 1

    def reduce(self, rows_iter, out, params):
        for word, count in kvgroup(sorted(rows_iter)):
            out.add(word, sum(count))
        return None

if __name__ == '__main__':

    parser = argparse.ArgumentParser(description="Count words in a file with map-reduce.")
    parser.add_argument("--file_in", default="input.txt", help="Input file. Default: input.txt")
    parser.add_argument("--file_out", default="output.csv", help="Output file. Default: output.csv")
    args = parser.parse_args()
    print args

    main(file_in=args.file_in, file_out=args.file_out)
