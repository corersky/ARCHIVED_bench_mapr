#!/usr/bin/env python
"""
Sort records from a Disco tag. Output .csv.

TODO:
- make master scrpt to run load then sort
- check disco v0.4.4
"""

from __future__ import print_function
import argparse
import csv
from disco.core import Job, result_iterator
from disco.util import kvgroup
from disco.func import chain_reader
        
class Sort(Job):
    
    def map(self, line, params):
        yield line, 1

    def reduce(self, rows_iter, out, params):
        for line, count in kvgroup(sorted(rows_iter)):
            out.add(line, sum(count))
        return None

def main(tag, file_out):
    # Import since slave nodes do not have same namespace as master
    from sort_map_reduce import Sort
    job = Sort().run(input=[tag], map_reader=chain_reader)
    with open(file_out, 'w') as f_out:
        writer = csv.writer(f_out, quoting=csv.QUOTE_NONNUMERIC)
        for string, count in result_iterator(job.wait(show=True)):
            writer.writerow([string, count])
    return None

if __name__ == '__main__':

    tag_default = "data:sort"
    file_out_default = "output.csv"
    
    parser = argparse.ArgumentParser(description="Sort a file with map-reduce.")
    parser.add_argument("--tag",
                        default=tag_default,
                        help="Input tag. Default: {default}".format(default=tag_default))
    parser.add_argument("--file_out",
                        default=file_out_default,
                        help="Output file. Default: {default}".format(default=file_out_default))
    args = parser.parse_args()
    print args

    main(tag=args.tag, file_out=args.file_out)
