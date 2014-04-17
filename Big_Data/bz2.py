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

import argparse
import urllib2
import os
import subprocess
from disco.ddfs import DDFS

def download(url, file_out="download.out"):
    """
    Download a file from a URL.
    """
    # From http://stackoverflow.com/questions/4028697
    # /how-do-i-download-a-zip-file-in-python-using-urllib2
    try:
        print "Downloading\n{url}\nto\n{file_out}".format(url=url, file_out=file_out)
        f_url = urllib2.urlopen(url)
        with open(file_out, 'wb') as f_out:
            f_out.write(f_url.read())
    except urllib2.HTTPError, e:
        print "HTTP Error:", e.code, url
    except urllib2.URLError, e:
        print "URL Error:", e.reason, url
    return None

def decompress(file_bz2, file_out="decompress.out"):
    """
    Decompress bz2 files.
    """
    (base, ext) = os.path.splitext(file_bz2)
    if ext != '.bz2':
        print file_bz2
        raise NameError("File extension not '.bz2'.")
    print "Decompressing\n{file_bz2}\nto\n{file_out}".format(file_bz2=file_bz2, file_out=file_out)
    with bz2.BZ2File(file_bz2, 'rb') as f_bz2:
        with open(file_out, 'wb') as f_out:
            f_out.write(f_bz2.read())
    # TODO: delete following if above works
    #    subprocess.call(['bzip2', '-dk', file_bz2])
    return None

def main(file_in="bz2_url_list.txt", tmp="/scratch/sth499"):
    """
    Download bz2 files from list and upload to Disco Distributed File System.
    """
    with open(file_in, 'r') as f_in:
        # If Disco tag exists, delete it.
        tag="data:big"
        if DDFS().exists(tag=tag):
            print "Deleting Disco tag {tag}.".format(tag=tag)
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
            # Decompress bz2 file.
            file_decom = os.path.splitext(file_bz2)[0]
            decompress(file_bz2=file_bz2, file_out=file_decom)
            # Load data into Disco Distributed File System.
            # Files must be prefixed with './'
            print "Loading into Disco: {file_decom}.".format(file_decom=file_decom)
            DDFS().chunk(tag=tag, urls=['./'+file_decom])
            # Delete bz2 and decompressed files.
            print "Deleting:\n{file_bz2}\n{file_decom}".format(file_bz2=file_bz2, file_decom=file_decom)
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
    print args
    main(file_in=args.file_in, tmp=args.tmp)
