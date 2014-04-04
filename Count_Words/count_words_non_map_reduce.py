#!/usr/bin/env python
"""
Count words from a .txt file and output .csv
as check for map reduce implentation.
"""

import argparse
import csv

def main(file_in="input.txt", file_out="output.csv"):
    with open(file_in, 'r') as f_in:
        word_lists = [line.split() for line in f_in]
    # Flatten the list
    # http://stackoverflow.com/questions/952914/\
    # making-a-flat-list-out-of-list-of-lists-in-python
    words = [word for word_list in word_lists for word in word_list]
    counts = map(words.count, words)
    # TODO: tallies has O ~ N^2 performance
    tallies = sorted(set(zip(words, counts)))
    with open(file_out, 'w') as f_out:
        writer = csv.writer(f_out, quoting=csv.QUOTE_NONNUMERIC)
        for (word, count) in tallies:
            writer.writerow([word, count])
    return None

if __name__ == '__main__':
    
    parser = argparse.ArgumentParser(description="Count words in a file without map-reduce.")
    parser.add_argument("--file_in", default="input.txt", help="Input file. Default: input.txt")
    parser.add_argument("--file_out", default="output.csv", help="Output file. Default: output.csv")
    args = parser.parse_args()
    print args

    main(file_in=args.file_in, file_out=args.file_out)
