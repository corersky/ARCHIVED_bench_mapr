#!/usr/bin/env python
"""
sort.py

Sort a file and tally sorted lines. Output .csv.

"""

import argparse
import csv
from disco.ddfs import DDFS
from disco.core import Job, result_iterator
from disco.util import kvgroup
from disco.worker.task_io import chain_reader

def main(file_in="input.txt", file_out="output.csv"):
    # with open(file_in, 'r') as f_in:
    #     lines = [line for line in f_in]
    # counts = map(lines.count, lines)
    # tallies = sorted(list(set(zip(lines, counts))))
    # with open(file_out, 'w') as f_out:
    #     writer = csv.writer(f_out, quoting=csv.QUOTE_NONNUMERIC)
    #     for (line, count) in tallies:
    #         writer. writerow([line, count])

    tag = "data:sort"
    DDFS().chunk(tag=tag, urls=["./"+file_in])

    try:
        # Import since slave nodes do not have same namespace as master
        from sort import Sort
        # TODO: disco is reading its chunked data as binary
        # how follow http://disco.readthedocs.org/en/latest/howto/chunk.html ?
        job = Sort().run(map_reader=chain_reader, input=[tag])
        with open(file_out, 'w') as f_out:
            writer = csv.writer(f_out, quoting=csv.QUOTE_NONNUMERIC)
            for string, count in result_iterator(job.wait(show=True)):
                writer.writerow([string, count])
    finally:
        DDFS().delete(tag=tag)
        
class Sort(Job):
    
    def map(self, line, params):
        yield line, 1

    def reduce(self, rows_iter, out, params):
        for line, count in kvgroup(sorted(rows_iter)):
            out.add(line, sum(count))

if __name__ == '__main__':
    
    parser = argparse.ArgumentParser(description="Sort a file with map-reduce.")
    parser.add_argument("--file_in", default="input.txt", help="input file (default: input.txt)")
    parser.add_argument("--file_out", default="output.csv", help="output file (default: output.csv)")
    args = parser.parse_args()
    print args

    main(file_in=args.file_in, file_out=args.file_out)
