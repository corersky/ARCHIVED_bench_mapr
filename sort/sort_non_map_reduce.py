#!/usr/bin/env python
"""
Sort a file to check the map-reduce implentation.
"""

from __future__ import print_function
import argparse

def main(file_in, file_out):
    """
    Read in lines, sort lines, write out sorted lines.
    """

    with open(file_in, 'r') as f_in:
        lines = [line for line in f_in]

    with open(file_out, 'w') as f_out:
        for string in sorted(lines):
            f_out.write(string)

    return None

if __name__ == '__main__':

    file_in_default = "input.txt"
    file_out_default = "output.txt"

    parser = argparse.ArgumentParser(description="Sort a file without map-reduce.")
    parser.add_argument("--file_in",
                        default=file_in_default,
                        help="Input file. Default: {default}".format(default=file_in_default))
    parser.add_argument("--file_out",
                        default=file_out_default,
                        help="Output file. Default: {default}".format(default=file_out_default))
    args = parser.parse_args()
    print(args)

    main(file_in=args.file_in, file_out=args.file_out)
