#!/usr/bin/env python
"""
Do k-means clustering without map-reduce.
See https://groups.google.com/forum/#!topic/disco-dev/u3EsnGgLOPM
"""

import argparse
import csv

def main(file_in="input.txt", file_out="output.csv"):
    pass

if __name__ == '__main__':
    
    parser = argparse.ArgumentParser(description="Do k-means clustering from a file without map-reduce.")
    parser.add_argument("--file_in", default="input.txt", help="Input file. Default: input.txt")
    parser.add_argument("--file_out", default="output.csv", help="Output file. Default: output.csv")
    args = parser.parse_args()
    print args

    main(file_in=args.file_in, file_out=args.file_out)
