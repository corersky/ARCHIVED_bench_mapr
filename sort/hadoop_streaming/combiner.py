#!/usr/bin/env python
"""
Combiner for sort with Hadoop streaming.
"""

from __future__ import print_function
import sys

def main(stdin):
    """
    Take unsorted standard input from mapper and return sorted block.
    Value is just a place holder.
    """
    for line_num in sorted(stdin):
        # Remove trailing newlines.
        line_num = line_num.rstrip()
        # Omit empty lines.
        try:
            (line, num) = line_num.rsplit('\t', 1)
            print(("{line}\t{num}").format(line=line, num=num))
        except ValueError:
            pass
    return None

if __name__ == '__main__':
    main(stdin=sys.stdin)
