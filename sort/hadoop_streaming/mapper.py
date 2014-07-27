#!/usr/bin/env python
"""
Mapper for sort with Hadoop streaming.
"""

from __future__ import print_function
import sys

def main(stdin):
    """
    Print lines from standard in.
    Value is just a place holder.
    """
    for line in stdin:
        # Remove trailing newlines.
        line = line.rstrip()
        # Omit empty lines.
        if line != '':
            print(("{line}\t{num}").format(line=line, num=1))
    return None

if __name__ == '__main__':
    main(stdin=sys.stdin)
