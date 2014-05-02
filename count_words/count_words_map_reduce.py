#!/usr/bin/env python
"""
Count words from a Disco tag. Output to .csv.

TODO:
- check disco v0.4.4
"""

from __future__ import print_function
import argparse
import csv
from disco.core import Job, result_iterator
from disco.util import kvgroup
from disco.func import chain_reader

class CountWords(Job):

    def map(self, line, params):
        for word in line.split():
            yield word, 1

    def reduce(self, rows_iter, out, params):
        for word, count in kvgroup(sorted(rows_iter)):
            out.add(word, sum(count))
        return None

def main(tag, file_out):
    # Import since slave nodes do not have same namespace as master.
    from count_words_map_reduce import CountWords
    job = CountWords().run(input=[tag], map_reader=chain_reader)
    with open(file_out, 'w') as f_out:
        writer = csv.writer(f_out, quoting=csv.QUOTE_NONNUMERIC)
        for word, count in result_iterator(job.wait(show=False)):
            writer.writerow([word, count])
    return None

if __name__ == '__main__':

    tag_default = "data:count_words"
    file_out_default = "output.csv"

    parser = argparse.ArgumentParser(description="Count words from a tagged Disco data set using map-reduce.")
    parser.add_argument("--tag",
                        default=tag_default,
                        help="Input tag. Default: {default}".format(default=tag_default))
    parser.add_argument("--file_out",
                        default=file_out_default,
                        help="Output file. Default: {default}".format(default=file_out_default))
    args = parser.parse_args()
    print(args)
    
    main(tag=args.tag, file_out=args.file_out)
