#!/usr/bin/env python
"""
Download bz2 files from list and upload to Disco Distributed File System.

TODO:
- check ddfs tag exists
- check disco v0.4.4
- less IO if bz2 in map reader
- less IO if push to ddfs w/o writing files
- decompress bz2 w/o subprocess
"""

from __future__ import print_function
import argparse
import urllib2
import os
import bz2
import sys
from disco.ddfs import DDFS

def download(url, file_out="download.out"):
    """
    Download a file from a URL.
    """
    # From http://stackoverflow.com/questions/4028697
    # /how-do-i-download-a-zip-file-in-python-using-urllib2
    try:
        print("Downloading\n{url}\nto\n{file_out}".format(url=url, file_out=file_out))
        f_url = urllib2.urlopen(url)
        with open(file_out, 'wb') as f_out:
            f_out.write(f_url.read())
    except urllib2.HTTPError, e:
        print("HTTP Error:", e.code, url)
    except urllib2.URLError, e:
        print("URL Error:", e.reason, url)
    return None

def decompress_and_partition(file_bz2, file_out="decompress.out"):
    """
    Decompress bz2 files and partition with newlines every 100 KB.
    Due to a bug in disco v0.4.4 for uploading, pushing a long record as a blob corrupts the existing tag.
    Chunk does not work on records over 1MB.
    See: https://groups.google.com/forum/#!searchin/disco-dev/push$20chunk/disco-dev/i9LiNiLEQ7k/95fX2sC-dtQJ
    """
    (base, ext) = os.path.splitext(file_bz2)
    if ext != '.bz2':
        print(file_bz2)
        raise NameError("File extension not '.bz2'.")
    print(
        "Decompressing and partitioning\n"
        +"{file_bz2}\nto\n{file_out}".format(file_bz2=file_bz2, file_out=file_out))
    # From http://stackoverflow.com/questions/16963352/decompress-bz2-files
    # and http://bob.ippoli.to/archives/2005/06/14/python-iterators-and-sentinel-values/
    with open(file_out, 'wb') as f_out, bz2.BZ2File(file_bz2, 'rb') as f_bz2:
        for data in iter(lambda : f_bz2.read(100*1024), b''):
            f_out.write(data)
            f_out.write(b"\n")
    return None

def main(file_in="bz2_url_list.txt", tmp="/scratch/sth499"):
    """
    Download bz2 files from list and upload to Disco Distributed File System.
    """
    with open(file_in, 'r') as f_in:
        # If Disco tag exists, delete it.
        tag="data:big"

        if DDFS().exists(tag=tag):
            print("Deleting Disco tag {tag}.".format(tag=tag))
            DDFS().delete(tag=tag)

        for line in f_in:

            # Skip commented lines.
            if line.startswith('#'):
                continue

            # Remove newlines and name file from URL.
            url = line.strip()
            file_bz2 = os.path.join(tmp, os.path.basename(url))
            # Download bz2 file.
            download(url=url, file_out=file_bz2)

            # Decompress and partition bz2 file.
            file_decom = os.path.splitext(file_bz2)[0]
            decompress_and_partition(file_bz2=file_bz2, file_out=file_decom)

            # Load data into Disco Distributed File System.
            print("Loading into Disco:\n{file_in}".format(file_in=file_decom))
            try:
                DDFS().chunk(tag=tag, urls=[os.path.join('./', file_decom)])
            except ValueError:
                print("ValueError:\n{file_decom}".format(file_decom=file_decom), file=sys.stderr)
            # Delete bz2 and decompressed files.
            print("Deleting:\n{file_bz2}\n{file_decom}".format(file_bz2=file_bz2, file_decom=file_decom))
            os.remove(file_bz2)
            os.remove(file_decom)
    return None

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Download bz2 files from list and upload to Disco Distributed File System.")
    parser.add_argument("--file_in", default="bz2_url_list.txt", 
                        help="Input file list of URLs to bz2 files for download. Default: bz2_url_list.txt")
    parser.add_argument("--tmp", default="/scratch/sth499",
                        help="Path where to temporarily save bz2 files for extraction and loading.")
    args = parser.parse_args()
    print(args)
    main(file_in=args.file_in, tmp=args.tmp)
