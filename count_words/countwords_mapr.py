#!/usr/bin/env python
"""
Count words from a Disco tag. Output to .csv.
"""

from __future__ import print_function
import argparse
import csv
from disco.core import Job, result_iterator
from disco.util import kvgroup
from disco.func import chain_reader

class CountWords(Job):
    """
    Map each word to a '1'.
    Reduce the tallies to count the words.
    """

    def map(self, line, params):
        """
        Map:
        Split each line into words and 
        pair every word with '1' in a tuple: (word, 1)
        """
        
        for word in line.split():
            yield (word, 1)

    def reduce(self, rows_iter, out, params):
        """
        Reduce:
        Sort all (word, 1) tuples then tally.
        """
        
        # kvgroup requires consecutive keys that compare as equal
        # in order to combine values.
        for (word, count) in kvgroup(sorted(rows_iter)):
            out.add(word, sum(count))
        return None

def main(tag, file_out):
    """
    Run CountWords map-reduce job and write output.
    """

    # Import since slave nodes do not have same namespace as master.
    from count_words_map_reduce import CountWords
    job = CountWords().run(input=[tag], map_reader=chain_reader)
    with open(file_out, 'w') as f_out:
        writer = csv.writer(f_out, quoting=csv.QUOTE_NONNUMERIC)
        for (word, total) in result_iterator(job.wait(show=False)):
            writer.writerow([word, total])
    return None

if __name__ == '__main__':

    tag_default = "data:count_words"
    file_out_default = "output.csv"

    parser = argparse.ArgumentParser(description="Count words from a tagged Disco data set using map-reduce.")
    parser.add_argument("--tag",
                        default=tag_default,
                        help="Input tag. Default: {default}".format(default=tag_default))
    parser.add_argument("--file_out",
                        default=file_out_default,
                        help="Output file. Default: {default}".format(default=file_out_default))
    args = parser.parse_args()
    print(args)
    
    main(tag=args.tag, file_out=args.file_out)
