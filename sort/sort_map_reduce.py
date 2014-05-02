#!/usr/bin/env python
"""
Sort records from a Disco tag. Output .txt.

TODO:
- check disco v0.4.4
"""

from __future__ import print_function
import argparse
from disco.core import Job, result_iterator
from disco.util import kvgroup
from disco.func import chain_reader
        
class Sort(Job):
    """
    Map each line to a '1'.
    Reduce the lines by sorting.
    """

    def map(self, line, params):
        """
        Map:
        Pair each line with '1' in a tuple: (line, 1)
        """
        
        yield (line, 1)

    def reduce(self, rows_iter, out, params):
        """
        Reduce:
        Sort all (line, 1) tuples and tally duplicate lines.
        """

        for (line, count) in kvgroup(sorted(rows_iter)):
            out.add(line, sum(count))
        return None

def main(tag, file_out):
    """
    Run Sort map-reduce job and write output, including duplicate lines.
    """

    # Import since slave nodes do not have same namespace as master.
    from sort_map_reduce import Sort
    job = Sort().run(input=[tag], map_reader=chain_reader)
    
    with open(file_out, 'w') as f_out:
        for (line, total) in result_iterator(job.wait(show=False)):
            # Write out duplicates.
            line_list = [line]*total
            for string in line_list:
                f_out.write(string)
    return None

if __name__ == '__main__':

    tag_default = "data:sort"
    file_out_default = "output.txt"
    
    parser = argparse.ArgumentParser(description="Sort lines from a tagged Disco data set using map-reduce.")
    parser.add_argument("--tag",
                        default=tag_default,
                        help="Input tag. Default: {default}".format(default=tag_default))
    parser.add_argument("--file_out",
                        default=file_out_default,
                        help="Output file. Default: {default}".format(default=file_out_default))
    args = parser.parse_args()
    print(args)

    main(tag=args.tag, file_out=args.file_out)
