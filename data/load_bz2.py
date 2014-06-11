#!/usr/bin/env python
"""
Download bz2 files from list and upload to Disco Distributed File System.
"""

from __future__ import print_function, division
import argparse
import os
import sys
import glob
import urllib2
import bz2
import subprocess32 as sub
import pandas as pd
from disco.ddfs import DDFS

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
            print(("ERROR: {num} errors occurred."
                   +" Search output above for 'ERROR:'").format(num=ErrMsg._err_count), file=sys.stderr)
        return None

def csv2df(fcsv):
    """
    Read a CSV file and return as a dataframe.
    Ignore comments starting with #.
    """
    if not os.path.isfile(fcsv):
        raise IOError(("File does not exist: {fname}").format(fname=fcsv))
    (fcsv_base, ext) = os.path.splitext(fcsv)
    if ext != '.csv':
        raise IOError(("File extension not '.csv': {fname}").format(fname=fcsv))
    fcsv_nocmts = fcsv_base + '_temp' + ext
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

def create_df_concat(fcsv_list):
    """
    Read in a list of CSV files and combine into a single dataframe.
    CSV file names are used as top level of heirarchical index.
    """
    df_dict = {}
    for fcsv in fcsv_list:
        df = csv2df(fcsv=fcsv)
        fcsv_basename = os.path.basename(fcsv)
        df_dict[fcsv_basename] = df
    df_concat = pd.concat(df_dict)
    return df_concat

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

def decom_part(fbz2, fout="decompress.out"):
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
            fo.write(b'\n')
    return None

def main_load(args):
    """
    Stage of main function for loading individual files:
    - Download bz2 file if it doesn't exist.
    - Decompress and partition bz2 file if it doesn't exist.
    - Load data into Disco Distributed File System if it doesn't exist.
    """
    df_bz2urls_filetags = args.df_concat.dropna(subset=['bz2url', 'filetag'])
    for (idx, bz2url, filetag) in df_bz2urls_filetags[['bz2url', 'filetag']].itertuples():
        # Download bz2 file if it doesn't exist.
        fbz2 = os.path.join(args.data_dir, os.path.basename(bz2url))
        if os.path.isfile(fbz2):
            if args.verbose >= 2: print(("INFO: Skipping download. File already exists:\n {fbz2}").format(fbz2=fbz2))
        else:
            if args.verbose >= 1: print(("INFO: Downloading:\n {url}\n to:\n {fout}").format(url=bz2url, fout=fbz2))
            try: download(url=bz2url, fout=fbz2)
            except: ErrMsg().eprint(err=sys.exc_info())
        # Decompress and partition bz2 file if it doesn't exist.
        fdecom = os.path.splitext(fbz2)[0]
        if os.path.isfile(fdecom):
            if args.verbose >= 2: print(("INFO: Skipping decompress and partition."
                                    +" File already exists:\n {fdecom}").format(fdecom=fdecom))
        else:
            if args.verbose >= 1: print(("INFO: Decompressing and partitioning:\n"
                                    +" {fbz2}\n to:\n {fout}").format(fbz2=fbz2, fout=fdecom))
            try: decom_part(fbz2=fbz2, fout=fdecom)
            except: ErrMsg().eprint(err=sys.exc_info())
        # Load data into Disco Distributed File System if it doesn't exist.
        if DDFS().exists(tag=filetag):
            if args.verbose >= 2: print(("INFO: Skipping Disco upload."
                                    +" Tag already exists:\n {tag}.").format(tag=filetag))
        else:
            if args.verbose >= 1: print(("INFO: Loading into Disco:\n"
                                    +" {fdecom}\n under tag:\n {tag}").format(fdecom=fdecom, tag=filetag))
            try: DDFS().chunk(tag=filetag, urls=[os.path.join('./', fdecom)])
            except: ErrMsg().eprint(err=sys.exc_info())
    return None

def main_check_filetags(args):
    """
    Stage of main function for checking filetags:
    - Download files from Disco Distributed File System.
    - Compare file size from Disco with original file size.
    """
    df_bz2urls_filetags = args.df_concat.dropna(subset=['bz2url', 'filetag'])
    for (idx, bz2url, filetag) in df_bz2urls_filetags[['bz2url', 'filetag']].itertuples():
        # Download files from Disco Distributed File System.
        ftag = os.path.join(args.data_dir, filetag + '.txt')
        cmd = ""
        if os.path.isfile(ftag):
            if args.verbose >= 2: print(("INFO: Skipping Disco download."
                                         +" File already exists:\n {ftag}").format(ftag=ftag))
        else:
            if args.verbose >= 1: print(("INFO: Downloading Disco tag:\n"
                                         +" {tag}\n into:\n {ftag}").format(tag=filetag, ftag=ftag))
            cmd = "ddfs xcat "+filetag+" > "+ftag
            sub.check_call(cmd, shell=True)
        # Compare file size from Disco with original file size.
        bytes_per_gb = 10**9
        fbz2 = os.path.join(args.data_dir, os.path.basename(bz2url))
        fdecom = os.path.splitext(fbz2)[0]
        fdecom_size_gb = os.path.getsize(fdecom) / bytes_per_gb
        ftag_size_gb = os.path.getsize(ftag) / bytes_per_gb
        frac_diff = (ftag_size_gb - fdecom_size_gb) / fdecom_size_gb
        if abs(frac_diff) > 0.1:
            print(("WARNING: Fractional difference in files sizes is greater than 10%:\n"
                   +" Decompressed file name and size (GB):\n"
                   +"  {fdecom}\n"
                   +"  {fdecom_size}\n"
                   +" Disco tag file name and size (GB):\n"
                   +"  {ftag}\n"
                   +"  {ftag_size}\n"
                   +" Fractional difference in file sizes, ((ftag - fdecom) / fdecom) :\n"
                   +"  {frac_diff}").format(fdecom=fdecom, fdecom_size=fdecom_size_gb,
                                                 ftag=ftag, ftag_size=ftag_size_gb,
                                                 frac_diff=frac_diff))
        elif args.verbose >= 1:
            print(("INFO: Fractional difference in file sizes:\n"
                   +" Decompressed file name and size (GB):\n"
                   +"  {fdecom}\n"
                   +"  {fdecom_size}\n"
                   +" Disco tag file name and size (GB):\n"
                   +"  {ftag}\n"
                   +"  {ftag_size}\n"
                   +" Fractional difference in file sizes, ((ftag - fdecom) / fdecom) :\n"
                   +"  {frac_diff}").format(fdecom=fdecom, fdecom_size=fdecom_size_gb,
                                                 ftag=ftag, ftag_size=ftag_size_gb,
                                                 frac_diff=frac_diff))
    return None

def main_sets(args):
    """
    Stage of main function for matching individual files to data sets
    - Match 'settag' to 'filetag'. Use 'bz2url' to match.
    - One 'settag' can match many 'filetag'.
    - Append matched 'bz2url' data to 'settag' into Disco.
    - Note: Must have all 'filetag' loaded.
    """
    # idx variables are unused.
    # TODO: Don't load settag if it already exists.
    df_bz2urls_filetags = args.df_concat.dropna(subset=['bz2url', 'filetag'])
    df_bz2urls_settags = args.df_concat.dropna(subset=['bz2url', 'settag'])
    for (idx, bz2url, settag) in df_bz2urls_settags[['bz2url', 'settag']].itertuples():
        # Reset variables.
        criteria = ''
        matched_filetag = ''
        criteria = (df_bz2urls_filetags['bz2url'] == bz2url)
        matched_filetag = df_bz2urls_filetags[criteria]['filetag'].values[0]
        if args.verbose >= 1: print(("INFO: Appending data from:\n {bz2url}\n"
                                     +" under tag:\n {filetag}\n to tag:\n"
                                     +" {settag}").format(bz2url=bz2url,
                                                          filetag=matched_filetag,
                                                          settag=settag))
        try:
            matched_filetag_urls = DDFS().urls(matched_filetag)
            DDFS().tag(settag, matched_filetag_urls)
        except: ErrMsg().eprint(err=sys.exc_info())
    return None

def main(args):
    """
    Load data in stages:
    - Read in CSV files to dataframes to manage tags of bz2 files.
    - Load individual files.
    - Check that files were loaded correctly, if wanted.
    - Match individual files to data sets, if wanted.
    - Report error count.
    """
    # Read in CSV files to dataframes to manage tags of bz2 files.
    df_concat = create_df_concat(fcsv_list=args.fcsvs)
    args.df_concat = df_concat
    # Load individual files.
    main_load(args)
    # Check filetags, if wanted.
    if args.check_filetags:
        main_check_filetags(args)
    # Match individual files to data sets, if wanted.
    if not args.no_sets:
        main_sets(args)
    # At end, report error count.
    if args.verbose >= 1: ErrMsg().esum()
    return None

if __name__ == '__main__':
    data_dir_default = '/tmp'
    parser = argparse.ArgumentParser(formatter_class=argparse.RawTextHelpFormatter,
                                     description="Download bz2 files then upload to Disco and tag.")
    parser.add_argument('--fcsvs',
                        nargs='*',
                        default=glob.glob(os.path.join(os.getcwd(), '*.csv')),
                        help=("Input .csv files with URLs of bz2 files for download and DDFS tags for upload.\n"
                              +"Default: [all .csv in CWD]"))
    parser.add_argument('--data_dir',
                        default=data_dir_default,
                        help=(("Path to save bz2 files for extraction and loading.\n"
                               +"Default: {default}").format(default=data_dir_default)))
    parser.add_argument('--check_filetags',
                        action='store_true',
                        help=("Check that DDFS tags of files match sizes of"
                              +" unzipped, partitioned .bz2 files.\n"
                              +"Writes 'filetag.txt' files to --data_dir"))
    # TODO: remove no_sets option when have create filesets option
    parser.add_argument('--no_sets',
                        action='store_true',
                        help=("Do not associate files with data sets."))
    parser.add_argument('--verbose',
                        '-v',
                        action='count',
                        help=("Print 'INFO:' messages to stdout. -vv for more verbosity."))
    args = parser.parse_args()
    if args.verbose >= 1:
        print("INFO: Arguments:")
        for arg in args.__dict__:
            print('', arg, args.__dict__[arg])
    if not os.path.isdir(args.data_dir):
        raise IOError(("Directory does not exist: data_dir = {data_dir}").format(data_dir=args.data_dir))
    main(args)
