#!/usr/bin/env python
"""
Download bz2 files from list and upload to Disco Distributed File System.
"""

from __future__ import print_function
import argparse
import os
import glob
import sys
import urllib2
import bz2
import pandas as pd
from disco.ddfs import DDFS

def create_df_concat(fcsv_list):
    """
    Read in a list of CSV files and combine into a single dataframe.
    Basename of CSV files are used as top level of heirarchical index.
    """
    df_dict = {}
    for fcsv in fcsv_list:
        df = csv_to_df(fcsv=fcsv)
        fcsv_basename = os.path.basename(fcsv)
        (fcsv_base, fcsv_ext) = os.path.splitext(fcsv_basename)
        df_dict[fcsv_base] = df
    df_concat = pd.concat(df_dict)
    return df_concat

def csv_to_df(fcsv):
    """
    Read a CSV file and return as a dataframe.
    Ignore comments starting with #.
    """
    if not os.path.isfile(fcsv):
        raise IOError("File does not exist: {fname}".format(fname=fcsv))
    (fcsv_base, ext) = os.path.splitext(fcsv)
    fcsv_nocmts = fcsv_base + '_temp' + ext
    if not ext == '.csv':
        raise IOError("File extension not '.csv': {fname}".format(fname=fcsv))
    with open(fcsv, 'r') as fcmts:
        # Make a temporary file without comments.
        with open(fcsv_nocmts, 'w') as fnocmts:
            for line in fcmts:
                if line.startswith('#'):
                    continue
                else:
                    fnocmts.write(line)
    df = pd.read_csv(fcsv_nocmts, sep=',')
    os.remove(fcsv_nocmts)
    return df

def download(url, fout="download.out"):
    """
    Download a file from a URL.
    """
    # From http://stackoverflow.com/questions/4028697
    # /how-do-i-download-a-zip-file-in-python-using-urllib2
    try:
        f_url = urllib2.urlopen(url)
        with open(fout, 'wb') as fo:
            fo.write(f_url.read())
    except urllib2.HTTPError, e:
        print("HTTP Error:", e.code, url, file=sys.stderr)
    except urllib2.URLError, e:
        print("URL Error:", e.reason, url, file=sys.stderr)
    return None

def decompress_and_partition(fbz2, fout="decompress.out"):
    """
    Decompress bz2 files and insert newlines every 100 KB.
    Due to a bug in disco v0.4.4 for uploading, pushing a long record as a blob corrupts the existing tag.
    Chunk does not work on records over 1MB.
    See: https://groups.google.com/forum/#!searchin/disco-dev/push$20chunk/disco-dev/i9LiNiLEQ7k/95fX2sC-dtQJ
    """
    (base, ext) = os.path.splitext(fbz2)
    if ext != '.bz2':
        raise IOError("File extension not '.bz2': {fname}".format(fname=fbz2))
    # Read large file incrementally and insert newlines every 100 KB. From:
    # http://stackoverflow.com/questions/16963352/decompress-bz2-files
    # http://bob.ippoli.to/archives/2005/06/14/python-iterators-and-sentinel-values/
    with open(fout, 'wb') as fo, bz2.BZ2File(fbz2, 'rb') as fb:
        for data in iter(lambda : fb.read(100*1024), b''):
            fo.write(data)
            fo.write(b"\n")
    return None

def upload_to_ddfs(fin, tag):
    """
    Upload to Disco Distributed File System.
    """
    try:
        DDFS().chunk(tag=tag, urls=[os.path.join('./', fin)])
    except ValueError as err:
        print("ValueError: "+err.message
              +"File: {fin}".format(fin=fin), file=sys.stderr)
    return None

def main(fcsv_list, tmp_dir, delete, no_upload):
    """
    Download bz2 files and upload to Disco.
    """
    # Read in CSV files to dataframes to manage tags of bz2 files.
    df_concat = create_df_concat(fcsv_list=fcsv_list)
    select_filetags = df_concat['filetag'].notnull()
    df_filetags = df_concat[select_filetags]
    for (idx, bz2url, filetag) in df_filetags[['bz2url', 'filetag']].itertuples():
        # Download bz2 file if it doesn't exist.
        fbz2 = os.path.join(tmp_dir, os.path.basename(bz2url))
        if os.path.isfile(fbz2):
            print("INFO: Skipping download. File already exists:\n{fbz2}".format(fbz2=fbz2))
        else:
            print("INFO: Downloading:\n{url}\nto\n{fout}".format(url=bz2url, fout=fbz2))
            download(url=bz2url, fout=fbz2)
    #     # Decompress and partition bz2 file if it doesn't exist.
    #     fdecom = os.path.splitext(fbz2)[0]
    #     if os.path.isfile(fdecom):
    #         print("INFO: Skipping decompress and partition. "
    #               +"File already exists:\n{fdecom}".format(fdecom=fdecom))
    #     else:
    #         print("INFO: Decompressing and partitioning:\n"
    #               +"{fbz2}\nto\n{fout}".format(fbz2=fbz2, fout=fdecom))
    #         decompress_and_partition(fbz2=fbz2, fout=fdecom)
    #     # Load data into Disco Distributed File System if it doesn't exist.
    #     if DDFS().exists(tag=filetag):
    #         print("INFO: Skipping Disco upload. "
    #               +"Tag already exists:\n{tag}.".format(tag=filetag))
    #     else:
    #         print("INFO: Loading into Disco:"
    #               +"\n{fdecom}\nunder tag\n{tag}".format(fdecom=fdecom, tag=filetag))
    #         upload_to_ddfs(fdecom=fdecom, tag=filetag)
    #     # Delete bz2 and decompressed files.
    #     if delete:
    #         print("INFO: Deleting:\n{fbz2}\n{fdecom}".format(fbz2=fbz2, fdecom=fdecom))
    #         os.remove(fbz2)
    #         os.remove(fdecom)
    return None

if __name__ == '__main__':
    tmp_dir_default = "/tmp"
    parser = argparse.ArgumentParser(description="Download bz2 files then upload to Disco and tag.")
    parser.add_argument("--fcsvs",
                        nargs='*',
                        default=glob.glob(os.path.join(os.getcwd(), "*.csv")),
                        help=("Input .csv files with URLs of bz2 files for download and DDFS tags for upload. "
                              +"Default: [all .csv in CWD]"))
    parser.add_argument("--tmp_dir",
                        default=tmp_dir_default,
                        help=("Path to save bz2 files for extraction and loading. "
                              +"Default: {default}".format(default=tmp_dir_default)))
    parser.add_argument("--delete",
                        action="store_true",
                        help=("Delete files after uploading to Disco."))
    parser.add_argument("--no_upload",
                        action="store_true",
                        help=("Only download and decompress files, not upload to Disco."))
    args = parser.parse_args()
    print(args)
    main(fcsv_list=args.fcsvs,
         tmp_dir=args.tmp_dir,
         delete=args.delete,
         no_upload=args.no_upload)
