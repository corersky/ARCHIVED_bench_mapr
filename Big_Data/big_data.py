#!/usr/bin/env python
"""
Download bz2 files from list and upload to Disco Distributed File System.
"""

import argparse
import urllib2
import os
from disco.ddfs import DDFS

def main(file_in="bz2_url_list.txt"):
    with open(file_in, 'r') as f_in:
        for url in f_in:
            print "Downloading {url}".format(url=url)
            # Skip commented lines.
            if url.startswith('#'):
                continue
            # from http://stackoverflow.com/questions/4028697/how-do-i-download-a-zip-file-in-python-using-urllib2
            try:
                url = url.strip()
                file_bz2 = os.path.basename(url)
                f_url = urllib2.urlopen(url)
                with open(file_bz2, 'wb') as f_bz2:
                    f_bz2.write(f_url.read())
            except HTTPError, e:
                print "HTTP Error:", e.code, url
            except URLError, e:
                print "URL Error:", e.reason, url
    return None

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Download bz2 files from list and upload to Disco Distributed File System.")
    parser.add_argument("--file_in", default="bz2_url_list.txt", 
                        help="Input file list of URLs to bz2 files for download. Default: bz2_url_list.txt")
    args = parser.parse_args()
    print args
    main(file_in=args.file_in)
