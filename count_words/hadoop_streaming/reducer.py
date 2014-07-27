#!/usr/bin/env python
"""
Count words without map-reduce from a .txt file and output to .csv
as check for map-reduce implentation.
"""

from __future__ import print_function
import argparse
import csv

def main(file_in, file_out):
    """
    Read in lines, flatten list of lines to list of words,
    sort and tally words, write out tallies.
    """

    with open(file_in, 'r') as f_in:
        word_lists = [line.split() for line in f_in]

    # Flatten the list
    # http://stackoverflow.com/questions/952914/\
    # making-a-flat-list-out-of-list-of-lists-in-python
    words = [word for word_list in word_lists for word in word_list]
    counts = map(words.count, words)
    tallies = sorted(set(zip(words, counts)))

    with open(file_out, 'w') as f_out:
        writer = csv.writer(f_out, quoting=csv.QUOTE_NONNUMERIC)
        for (word, count) in tallies:
            writer.writerow([word, count])

    return None

if __name__ == '__main__':
    
    file_in_default = "input.txt"
    file_out_default = "output.csv"

    parser = argparse.ArgumentParser(description="Count words in a file without map-reduce.")
    parser.add_argument("--file_in",
                        default=file_in_default,
                        help="Input file. Default: {default}".format(default=file_in_default))
    parser.add_argument("--file_out",
                        default=file_out_default,
                        help="Output file. Default: {default}".format(default=file_out_default))
    args = parser.parse_args()
    print(args)
    
    main(file_in=args.file_in, file_out=args.file_out)
