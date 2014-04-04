#!/usr/bin/env python
"""
Sort a file to check the map-reduce implentation.
"""

import argparse
import csv

def main(file_in="input.txt", file_out="output.txt"):
    with open(file_in, 'r') as f_in:
        lines = [line for line in f_in]
    counts = map(lines.count, lines)
    # TODO: tallies has O ~ N^2 performance
    tallies = sorted(set(zip(lines, counts)))
    with open(file_out, 'w') as f_out:
        writer = csv.writer(f_out, quoting=csv.QUOTE_NONNUMERIC)
        for (line, count) in tallies:
            writer.writerow([line, count])
    return None

if __name__ == '__main__':

    parser = argparse.ArgumentParser(description="Sort a file without map-reduce.")
    parser.add_argument("--file_in", default="input.txt", help="Input file. Default: input.txt")
    parser.add_argument("--file_out", default="output.csv", help="Output file. Default: output.csv")
    args = parser.parse_args()
    print args

    main(file_in=args.file_in, file_out=args.file_out)
