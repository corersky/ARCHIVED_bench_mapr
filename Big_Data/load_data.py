#!/usr/bin/env python
"""
Download bz2 files from list and upload to Disco Distributed File System.
"""

import argparse
import wget

def main(file_in="bz2_url_list.txt"):
    with open(file_in, 'r') as f_in:
        for url in f_in:
            # Skip commented lines.
            if url.startswith('#'):
                continue
            bz2_file = url.strip()
            wget(bz2_file)
            
    return None

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Download bz2 files from list and upload to Disco Distributed File System.")
    parser.add_argument("--file_in", default="bz2_url_list.txt", 
                        help="Input file list of URLs to bz2 files for download. Default: bz2_url_list.txt")
    args = parser.parse_args()
    print args
    main(file_in=args.file_in)
