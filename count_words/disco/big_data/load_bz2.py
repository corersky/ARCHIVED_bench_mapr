#!/usr/bin/env python
"""
Download files from a list and upload to distributed file systems.
"""

from __future__ import print_function, division
import argparse
import os
import sys
import glob
import urllib2
import bz2
import operator
import pandas as pd
from subprocess32 import Popen
from disco.ddfs import DDFS

# TODO: use logging for error messages.

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

def csv_to_df(fcsv):
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
        df = csv_to_df(fcsv=fcsv)
        fcsv_basename = os.path.basename(fcsv)
        df_dict[fcsv_basename] = df
    df_concat = pd.concat(df_dict)
    return df_concat

def download(url, fout="download.out"):
    """
    Download a file from a URL.
    """
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
    # Read large file incrementally and insert newlines every 100 KB.
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
    # Download bz2 file if it doesn't exist.
    # TODO: parallelize, see "programming python" on threads
    # quick hack: use Popen with wget to download
    for (idx, bz2url, filetag) in df_bz2urls_filetags[['bz2url', 'filetag']].itertuples():
        fbz2 = os.path.join(args.data_dir, os.path.basename(bz2url))
        if os.path.isfile(fbz2):
            if args.verbose >= 2: print(("INFO: Skipping download. File already exists:\n {fbz2}").format(fbz2=fbz2))
        else:
            if args.verbose >= 1: print(("INFO: Downloading:\n {url}\n to:\n {fout}").format(url=bz2url, fout=fbz2))
            
            try: download(url=bz2url, fout=fbz2)
            except: ErrMsg().eprint(err=sys.exc_info())
    # Decompress and partition bz2 file if it doesn't exist.
    # TODO: parallelize, see "programing python" on threads
    # quick hack: use Popen with "bunzip2 --keep" and "grep -oE '.{1,1000}' fname" to partition
    for (idx, bz2url, filetag) in df_bz2urls_filetags[['bz2url', 'filetag']].itertuples():
        fbz2 = os.path.join(args.data_dir, os.path.basename(bz2url))
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
    cmds = []
    for (idx, bz2url, filetag) in df_bz2urls_filetags[['bz2url', 'filetag']].itertuples():
        fbz2 = os.path.join(args.data_dir, os.path.basename(bz2url))
        fdecom = os.path.splitext(fbz2)[0]
        if DDFS().exists(tag=filetag):
            if args.verbose >= 2: print(("INFO: Skipping Disco upload."
                                         +" Tag already exists:\n {tag}.").format(tag=filetag))
        else:
            if args.verbose >= 1: print(("INFO: Loading into Disco:\n"
                                         +" {fdecom}\n under tag:\n {tag}").format(fdecom=fdecom, tag=filetag))
            cmd = ("ddfs chunk {tag} {url}").format(tag=filetag, url=os.path.join('./', fdecom))
            cmds.append(cmd)
            # TODO: parallelize using Python API rather than system, see "programming python" on threads
            # try: DDFS().chunk(tag=filetag, urls=[os.path.join('./', fdecom)])
    try:
        processes = [Popen(cmd, shell=True) for cmd in cmds]
        for proc in processes: proc.wait()
    except: ErrMsg().eprint(err=sys.exc_info())
    return None

def main_check_filetags(args):
    """
    Stage of main function for checking filetags:
    - Download files from Disco Distributed File System.
    - Compare file size from Disco with original file size.
    """
    df_bz2urls_filetags = args.df_concat.dropna(subset=['bz2url', 'filetag'])
    # TODO: use DDFS().pull to get data, then parallelize, see "programing python" on threads
    cmds = []
    for (idx, bz2url, filetag) in df_bz2urls_filetags[['bz2url', 'filetag']].itertuples():
        # Download files from Disco Distributed File System.
        ftag = os.path.join(args.data_dir, filetag+'.txt')
        if os.path.isfile(ftag):
            if args.verbose >= 2: print(("INFO: Skipping Disco download."
                                         +" File already exists:\n {ftag}").format(ftag=ftag))
        else:
            if args.verbose >= 1: print(("INFO: Downloading Disco tag:\n"
                                         +" {tag}\n into:\n {ftag}").format(tag=filetag, ftag=ftag))
            cmd = ("ddfs xcat {tag} > {fname}").format(tag=filetag, fname=ftag)
            cmds.append(cmd)
    try:
        processes = [Popen(cmd, shell=True) for cmd in cmds]
        for proc in processes: proc.wait()
    except: ErrMsg().eprint(err=sys.exc_info())
    bytes_per_gb = 10**9
    for (idx, bz2url, filetag) in df_bz2urls_filetags[['bz2url', 'filetag']].itertuples():
        # Compare file size from Disco with original file size.
        fbz2 = os.path.join(args.data_dir, os.path.basename(bz2url))
        fdecom = os.path.splitext(fbz2)[0]
        fdecom_sizegb = os.path.getsize(fdecom) / bytes_per_gb
        ftag = os.path.join(args.data_dir, filetag+'.txt')
        ftag_sizegb = os.path.getsize(ftag) / bytes_per_gb
        frac_diff = (ftag_sizegb - fdecom_sizegb) / fdecom_sizegb
        if abs(frac_diff) > 0.1:
            print(("WARNING: Fractional difference in files sizes is larger than +/- 10%:\n"
                   +" Decompressed file name and size (GB):\n"
                   +"  {fdecom}\n"
                   +"  {fdecom_size}\n"
                   +" Disco tag file name and size (GB):\n"
                   +"  {ftag}\n"
                   +"  {ftag_size}\n"
                   +" Fractional difference in file sizes, ((ftag - fdecom) / fdecom) :\n"
                   +"  {frac_diff}").format(fdecom=fdecom, fdecom_size=fdecom_sizegb,
                                            ftag=ftag, ftag_size=ftag_sizegb,
                                            frac_diff=frac_diff),
                  file=sys.stderr)
        elif args.verbose >= 1:
            print(("INFO: Fractional difference in file sizes:\n"
                   +" Decompressed file name and size (GB):\n"
                   +"  {fdecom}\n"
                   +"  {fdecom_size}\n"
                   +" Disco tag file name and size (GB):\n"
                   +"  {ftag}\n"
                   +"  {ftag_size}\n"
                   +" Fractional difference in file sizes, ((ftag - fdecom) / fdecom) :\n"
                   +"  {frac_diff}").format(fdecom=fdecom, fdecom_size=fdecom_sizegb,
                                            ftag=ftag, ftag_size=ftag_sizegb,
                                            frac_diff=frac_diff))
    return None

def main_sets(args):
    """
    Stage of main function for packing individual files into data sets.
    - Sort filetags by size in descending order.
    - Add filetags to a dataset as long as they can fit.
    - Label the dataset with the actual dataset size.
    - Append data to settag from filetags in DDFS.
    - Note: Must have all 'filetag' loaded.
    """
    df_bz2urls_filetags = args.df_concat.dropna(subset=['bz2url', 'filetag'])
    bytes_per_gb = 10**9
    filetag_sizegb_map = {}
    # If it exists, use checked, downloaded data from Disco to verify dataset sizes,
    # otherwise use decompressed files prior to Disco upload.
    if args.check_filetags:
        # idx variables are unused.
        for (idx, bz2url, filetag) in df_bz2urls_filetags[['bz2url', 'filetag']].itertuples():
            ftag = os.path.join(args.data_dir, filetag+'.txt')
            ftag_sizegb = os.path.getsize(ftag) / bytes_per_gb
            filetag_sizegb_map[filetag] = ftag_sizegb
    else:
        # idx variables are unused.
        for (idx, bz2url, filetag) in df_bz2urls_filetags[['bz2url', 'filetag']].itertuples():
            fbz2 = os.path.join(args.data_dir, os.path.basename(bz2url))
            fdecom = os.path.splitext(fbz2)[0]
            fbz2_sizegb = os.path.getsize(fbz2) / bytes_per_gb
            filetag_sizegb_map[filetag] = fbz2_sizegb
    # Sort filetags by size in descending order.
    # Add filetags to a dataset as long as they can fit. Nest the data sets.
    filetag_sizegb_sorted = sorted(filetag_sizegb_map.iteritems(), key=operator.itemgetter(1), reverse=True)
    settag_filetags_map = {}
    is_first = True
    for size in sorted(args.sets_gb):
        filetags = []
        tot = 0.
        res = size
        # Include smaller data sets in the next larger dataset.
        if not is_first:
            filetags.extend(settag_filetags_map[prev_settag])
            tot += prev_tot
            res -= prev_tot
        for (filetag, sizegb) in filetag_sizegb_sorted:
            if (sizegb <= res) and (filetag not in filetags):
                filetags.append(filetag)
                tot += sizegb
                res -= sizegb
        # Label the dataset with the actual dataset size.
        # Note: Disco tags must have character class [A-Za-z0-9_\-@:]+ else get CommError.
        settag = ("{tot:.2f}GB".format(tot=tot)).replace('.', '-')
        settag_filetags_map[settag] = filetags
        # Include the smaller data set in the next larger dataset.
        prev_tot = tot
        prev_settag = settag
        is_first = False
    # Append data to settag from filetags in DDFS.
    # TODO: use logging.
    for settag in sorted(settag_filetags_map):
        if DDFS().exists(tag=settag):
            if args.verbose >= 2:
                print(("INFO: Skipping Disco upload."
                       +" Tag already exists:\n {tag}.").format(tag=settag))
        else:
            if args.verbose >= 1:
                print(("INFO: Appending data to settag from filetags:\n"
                       +" {settag}\n"
                       +" {filetags}").format(settag=settag,
                                              filetags=settag_filetags_map[settag]))
            for filetag in settag_filetags_map[settag]:
                try:
                    filetag_urls = DDFS().urls(filetag)
                    DDFS().tag(settag, filetag_urls)
                except: ErrMsg().eprint(err=sys.exc_info())
    return None

def main(args):
    """
    Load data in stages:
    - Read in CSV files to dataframes to manage tags of bz2 files.
    - Load individual files.
    - Check that files were loaded correctly, if wanted.
    - Pack individual files to data sets, if provided.
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
    # Pack individual files to data sets, if provided.
    # Use checked filetas if available, otherwise use source files.
    if len(args.sets_gb) > 0:
        main_sets(args)
    # At end, report error count.
    if args.verbose >= 1: ErrMsg().esum()
    return None

if __name__ == '__main__':
    # TODO: use config file instead.
    arg_default_map = {}
    arg_default_map['fcsvs'] = glob.glob(os.path.join(os.getcwd(), '*.csv'))
    arg_default_map['data_dir'] = '/tmp'
    arg_default_map['sets_gb'] = []
    parser = argparse.ArgumentParser(formatter_class=argparse.RawTextHelpFormatter,
                                     description="Download bz2 files then upload to Disco and tag.")
    parser.add_argument('--fcsvs',
                        nargs='*',
                        default=arg_default_map['fcsvs'],
                        help=("Input .csv files with URLs of bz2 files for download and DDFS tags for upload.\n"
                              +"Example: --fcsvs /path/to/files/*.csv\n"
                              +"Default: [all .csv in CWD]"))
    parser.add_argument('--data_dir',
                        default=arg_default_map['data_dir'],
                        help=(("Path to save bz2 files for extraction and loading.\n"
                               +"Example: --data_dir /path/to/data/dir\n"
                               +"Default: {default}").format(default=arg_default_map['data_dir'])))
    parser.add_argument('--check_filetags',
                        action='store_true',
                        help=("Check that DDFS tags of files match sizes of"
                              +" unzipped, partitioned .bz2 files.\n"
                              +"Writes 'filetag.txt' files to --data_dir"))
    parser.add_argument('--sets_gb',
                        nargs='+',
                        type=float,
                        default=arg_default_map['sets_gb'],
                        help=(("Sizes of data sets in GB. Data sets will be formed from URLs and given DDFS tags.\n"
                               +"Example: --sets_gb 1 3 10 30 100 300 1000")))
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
