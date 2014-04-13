#!/usr/bin/env python
"""
Download bz2 files from list and upload to Disco Distributed File System.

TODO:
- check ddfs tag exists
- check disco v0.4.4
- less IO if bz2 in map reader
- less IO if push to ddfs w/o writing files
"""

import argparse
import urllib2
import os
import bz2
from disco.ddfs import DDFS

def download(url, file_out=os.path.basename(url)):
    """
    Download a file from a URL.
    """
    # From http://stackoverflow.com/questions/4028697
    # /how-do-i-download-a-zip-file-in-python-using-urllib2
    try:
        print "Downloading {url}".format(url=url)
        f_url = urllib2.urlopen(url)
        with open(file_out, 'wb') as f_out:
            f_out.write(f_url.read())
    except HTTPError, e:
        print "HTTP Error:", e.code, url
    except URLError, e:
        print "URL Error:", e.reason, url
    return None

def decompress(file_bz2, file_out=os.path.splitext(file_bz2)[0]):
    """
    Decompress bz2 files.
    """
    base, ext = os.path.splitext(file_bz2)
    if ext != 'bz2':
        print file_bz2
        raise NameError("File extension not 'bz2'.")
    with bz2.BZ2File(file_bz2, 'rb') as f_bz2:
        with open(file_out, 'wb') as f_out:
            f_out.write(f_bz2.read())
    return None

def main(file_in="bz2_url_list.txt"):
    """
    Download bz2 files from list and upload to Disco Distributed File System.
    """
    with open(file_in, 'r') as f_in:
        for url in f_in:
            # Skip commented lines.
            if url.startswith('#'):
                continue
            # Remove newlines and name file from URL.
            url = url.strip()
            file_bz2 = os.path.basename(url)
            download(url=url, file_out=file_bz2)
            file_decom = os.path.splitext(file_bz2)[0]
            decompress(file_bz2=file_bz2, file_out=file_decom)
            # # Load data to Disco Distributed File System.
            # # If tag exists, delete it.
            # tag="data:big"
            # if DDFS().exists(tag=tag):
            # DDFS().delete(tag=tag)
            # DDFS().chunk(tag=tag, urls=['./'+file_bz2])
            # delete bz2 file
            # delete decompressed file
    return None

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Download bz2 files from list and upload to Disco Distributed File System.")
    parser.add_argument("--file_in", default="bz2_url_list.txt", 
                        help="Input file list of URLs to bz2 files for download. Default: bz2_url_list.txt")
    args = parser.parse_args()
    print args
    main(file_in=args.file_in)
