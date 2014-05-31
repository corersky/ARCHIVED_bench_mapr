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
    CSV file names are used as top level of heirarchical index.
    """
    df_dict = {}
    for fcsv in fcsv_list:
        df = csv_to_df(fcsv=fcsv)
        fcsv_basename = os.path.basename(fcsv)
        df_dict[fcsv_basename] = df
    df_concat = pd.concat(df_dict)
    return df_concat

def csv_to_df(fcsv):
    """
    Read a CSV file and return as a dataframe.
    Ignore comments starting with #.
    """
    if not os.path.isfile(fcsv):
        raise IOError(("File does not exist: {fname}").format(fname=fcsv))
    (fcsv_base, ext) = os.path.splitext(fcsv)
    fcsv_nocmts = fcsv_base + '_temp' + ext
    if ext != '.csv':
        raise IOError(("File extension not '.csv': {fname}").format(fname=fcsv))
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
    f_url = urllib2.urlopen(url)
    with open(fout, 'wb') as fo:
        fo.write(f_url.read())
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
        raise IOError(("File extension not '.bz2': {fname}").format(fname=fbz2))
    # Read large file incrementally and insert newlines every 100 KB. From:
    # http://stackoverflow.com/questions/16963352/decompress-bz2-files
    # http://bob.ippoli.to/archives/2005/06/14/python-iterators-and-sentinel-values/
    with open(fout, 'wb') as fo, bz2.BZ2File(fbz2, 'rb') as fb:
        for data in iter(lambda : fb.read(100*1024), b''):
            fo.write(data)
            fo.write(b"\n")
    return None

class ErrMsg(object):
    """
    Print custom error messages.
    """
    _err_count = 0

    def __init__(self):
        """
        Initialize.
        """
        # TODO: for future
        return None

    def __del__(self):
        """
        Deconstruct.
        """
        # TODO: for future
        return None

    def eprint(self, err):
        """
        Print custom error message.
        """
        print(("ERROR: {err}").format(err=err), file=sys.stderr)
        ErrMsg._err_count += 1
        return None

    def esum(self):
        """
        Print tally of errors.
        """
        if ErrMsg._err_count == 0:
            print("INFO: No errors occured.")
        else:
            print(("ERROR: {num} errors occurred. "
                   +"Search output above for 'ERROR:'").format(num=ErrMsg._err_count), file=sys.stderr)
        return None

def main(args):
    """
    Input .csv files with URLs of bz2 files for download
    and DDFS tags for upload.
    """
    # Read in CSV files to dataframes to manage tags of bz2 files.
    df_concat = create_df_concat(fcsv_list=args.fcsvs)
    # Load all bz2 files into Disco.
    df_bz2urls_filetags = df_concat.dropna(subset=['bz2url', 'filetag'])
    for (idx, bz2url, filetag) in df_bz2urls_filetags[['bz2url', 'filetag']].itertuples():
        # Download bz2 file if it doesn't exist.
        fbz2 = os.path.join(args.data_dir, os.path.basename(bz2url))
        if os.path.isfile(fbz2):
            if args.verbose: print(("INFO: Skipping download. File already exists:\n {fbz2}").format(fbz2=fbz2))
        else:
            if args.verbose: print(("INFO: Downloading:\n {url}\n to:\n {fout}").format(url=bz2url, fout=fbz2))
            try: download(url=bz2url, fout=fbz2)
            except: ErrMsg().eprint(err=sys.exc_info())
        # Decompress and partition bz2 file if it doesn't exist.
        fdecom = os.path.splitext(fbz2)[0]
        if os.path.isfile(fdecom):
            if args.verbose: print(("INFO: Skipping decompress and partition. "
                                    +"File already exists:\n {fdecom}").format(fdecom=fdecom))
        else:
            if args.verbose: print(("INFO: Decompressing and partitioning:\n"
                                    +" {fbz2}\n to:\n {fout}").format(fbz2=fbz2, fout=fdecom))
            try: decompress_and_partition(fbz2=fbz2, fout=fdecom)
            except: ErrMsg().eprint(err=sys.exc_info())
        # Load data into Disco Distributed File System if it doesn't exist.
        if DDFS().exists(tag=filetag):
            if args.verbose: print(("INFO: Skipping Disco upload. "
                                    +"Tag already exists:\n {tag}.").format(tag=filetag))
        else:
            if args.verbose: print(("INFO: Loading into Disco:\n"
                                    +" {fdecom}\n under tag:\n {tag}").format(fdecom=fdecom, tag=filetag))
            try: DDFS().chunk(tag=filetag, urls=[os.path.join('./', fdecom)])
            except: ErrMsg().eprint(err=sys.exc_info())
        # Delete bz2 and decompressed files if wanted.
        if args.delete:
            if args.verbose: print(("INFO: Deleting:\n {fbz2}\n {fdecom}").format(fbz2=fbz2, fdecom=fdecom))
            try:
                os.remove(fbz2)
                os.remove(fdecom)
            except: ErrMsg().eprint(err=sys.exc_info())
    # Match 'settag' to 'filetag'. Use 'bz2url' to match.
    # One 'settag' can match many 'filetag'. Must have all 'filetag' loaded.
    # Append matched 'bz2url' data to 'settag' into Disco.
    df_bz2urls_settags = df_concat.dropna(subset=['bz2url', 'settag'])
    for (idx, bz2url, settag) in df_bz2urls_settags[['bz2url', 'settag']].itertuples():
        # Reset variables.
        criteria = ''
        matched_filetag = ''
        criteria = (df_bz2urls_filetags['bz2url'] == bz2url)
        matched_filetag = df_bz2urls_filetags[criteria]['filetag'].values[0]
        if args.verbose: print(("INFO: Appending data from:\n {bz2url}\n"
                                +" under tag:\n {filetag}\n to tag:\n"
                                +" {settag}").format(bz2url=bz2url, filetag=matched_filetag, settag=settag))
        try:
            matched_filetag_urls = DDFS().urls(matched_filetag)
            DDFS().tag(settag, matched_filetag_urls)
        except: ErrMsg().eprint(err=sys.exc_info())
    # Report error count.
    if args.verbose: ErrMsg().esum()
    return None

if __name__ == '__main__':
    data_dir_default = "/tmp"
    parser = argparse.ArgumentParser(formatter_class=argparse.RawTextHelpFormatter,
                                     description="Download bz2 files then upload to Disco and tag.")
    parser.add_argument("--fcsvs",
                        nargs='*',
                        default=glob.glob(os.path.join(os.getcwd(), "*.csv")),
                        help=("Input .csv files with URLs of bz2 files for download and DDFS tags for upload. "
                              +"Default: [all .csv in CWD]"))
    parser.add_argument("--data_dir",
                        default=data_dir_default,
                        help=(("Path to save bz2 files for extraction and loading. "
                               +"Default: {default}").format(default=data_dir_default)))
    parser.add_argument("--verbose",
                        "-v",
                        action="store_true",
                        help=("Print 'INFO:' messages to stdout."))
    parser.add_argument("--delete",
                        "-d",
                        action="store_true",
                        help=("Delete files after uploading to Disco."))
    args = parser.parse_args()
    if args.verbose:
        print("INFO: Arguments:")
        for arg in args.__dict__:
            print(arg, args.__dict__[arg])
    main(args)
