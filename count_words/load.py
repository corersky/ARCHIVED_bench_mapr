#!/usr/bin/env python
"""
Load data from a .txt file to Disco.

TODO:
- check disco v0.4.4
- make module load shared between count_words and sort.
"""

from __future__ import print_function
import sys
import os
import argparse
from disco.ddfs import DDFS

def load(file_in, tag):
    # TODO: Rename tag data:count_words1 if tag exists.
    if DDFS().exists(tag=tag):
        print("WARNING: Overwriting Disco tag {tag}.".format(tag=tag), file=sys.stderr)
        DDFS().delete(tag=tag)
    # Disco v0.4.4 requrires that ./ prefix the file to identify as a local file.
    # http://disco.readthedocs.org/en/0.4.4/howto/chunk.html#chunking
    print("Loading into Disco:\n{file_in}\nas\n{tag}".format(file_in=file_in, tag=tag))
    try:
        DDFS().chunk(tag=tag, urls=[os.path.join('./', file_in)])
    except ValueError as err:
        print("ValueError: "+err.message, file=sys.stderr)
    return None

def main(file_in, tag):
    load(file_in, tag)
    return None

if __name__ == '__main__':

    file_in_default  = "input.txt"
    tag_default = "data:count_words"

    parser = argparse.ArgumentParser(description="Load data from a file into Disco and tag.")
    parser.add_argument("--file_in",
                        default=file_in_default,
                        help="Input file. Default: {default}".format(default=file_in_default))
    parser.add_argument("--tag",
                        default=tag_default,
                        help="Disco tag for input file. Default: {default}".format(default=tag_default))
    args = parser.parse_args()
    print(args)

    main(file_in=args.file_in, tag=args.tag)
