#!/usr/bin/env python
"""
Reducer for sort with Hadoop streaming.
"""

from __future__ import print_function
import sys

def main(stdin):
    """
    Sort standard input and return sorted lines.
    Value is just a place holder.
    """
    for line_num in sorted(stdin):
        # Remove trailing newlines.
        line_num = line_num.rstrip()
        # Sort only lines containing text.
        # Omit empty lines containing only newlines.
        try:
            (line, num) = line_num.rsplit('\t', 1)
            print(("{line}\t{num}").format(line=line, num=num))
        except ValueError:
            pass
    return None

if __name__ == '__main__':
    main(stdin=sys.stdin)
