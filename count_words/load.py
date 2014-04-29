#!/usr/bin/env python
"""
Load data from a .txt file to Disco.

TODO:
- check disco v0.4.4
"""

import argparse
from disco.ddfs import DDFS



def main(tag_in, file_out):
    # Import since slave nodes do not have same namespace as master.
    from count_words_map_reduce import CountWords
    job = CountWords().run(input=[tag_in], map_reader=chain_reader)
    with open(file_out, 'w') as f_out:
        writer = csv.writer(f_out, quoting=csv.QUOTE_NONNUMERIC)
        for word, count in result_iterator(job.wait(show=True)):
            writer.writerow([word, count])
    return None

if __name__ == '__main__':

    file_in_default  = "input.txt"
    tag_in_default   = "data:count_words"
    file_out_default = "output.csv"

    parser = argparse.ArgumentParser(description="Count words in a file using disco with map-reduce.")
    parser.add_argument("--file_in",
                        default=file_in_default,
                        help="Input file. Default: {default}".format(default=file_in_default))
    parser.add_argument("--tag_in",
                        default=tag_in_default,
                        help="Input tag. Default: {default}".format(default=tag_in_default))
    parser.add_argument("--file_out",
                        default=file_out_default,
                        help="Output file. Default: {default}".format(default=file_out_default))
    args = parser.parse_args()
    print args
    
    if args.file_in:
        # TODO: Rename tag data:count_words1 if tag exists.
        # Disco v0.4.4 requrires that ./ prefix the file to identify as a local file.
        # http://disco.readthedocs.org/en/0.4.4/howto/chunk.html#chunking
        try:
            tag_in = "data:count_words"
            DDFS().chunk(tag=tag_in, urls=["./"+file_in])
            main(tag_in=args.tag_in, file_out=args.file_out)
        finally:
            DDFS().delete(tag=tag_in)
    if args.tag_in:
        main(tag_in=args.tag_in, file_out=args.file_out)
