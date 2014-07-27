#!/usr/bin/env python
"""
Mapper for sort with Hadoop streaming.
"""

from __future__ import print_function
import sys

def main(stdin):
    """
    Read in line. Parse into list of words.
    """

    with open(file_in, 'r') as f_in:
        lines = [line for line in f_in]

    with open(file_out, 'w') as f_out:
        for string in sorted(lines):
            f_out.write(string)

    return None

if __name__ == '__main__':
    main(stdin=sys.stdin)
