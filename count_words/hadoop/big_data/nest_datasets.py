#!/usr/bin/env python
"""
Nest data sets in hadoop.
"""

from __future__ import print_function, division
import argparse
import glob
import os
import operator
import subprocess32 as sub

def main(args):
    """
    Main function for loading data into Hadoop
    and creating nested datasets.
    """
    # Load files into Hadoop directory.
    if args.files_in is not None:
        if args.verbose >= 1: print(("INFO: Uploading\n"
                                     +" {fins}\n into:\n {fdir}").format(fins=args.files_in, fdir=args.files_dir))
        cmd = ("hadoop fs -put {fins} {fdir}").format(fins=args.files_in, fdir=args.files_dir)
        proc = sub.Popen(cmd, shell=True)
        proc.wait()
    # Create nested datasets from data in Hadoop directory.
    bytes_per_gb = 10**9
    if args.sets_gb is not None:
        # Get file sizes and sort in descending order.
        cmd = ("hadoop fs -du {fdir}").format(fdir=args.files_dir)
        output = sub.check_output(cmd, shell=True)
        hfile_sizegb_map = dict([
                (hfile, int(size)/bytes_per_gb) for (size, hfile) in [
                    line.split() for line in output.splitlines()]])
        hfile_sizegb_sorted = sorted(hfile_sizegb_map.iteritems(), key=operator.itemgetter(1), reverse=True)
        # Add files to a dataset as long as they can fit and are not already included.
        hset_hfiles_map = {}
        is_first = True
        for size in sorted(args.sets_gb):
            hfiles = []
            tot = 0.
            res = size
            # Nest the datasets by including smaller datasets in the next larger dataset.
            if not is_first:
                hfiles.extend(hset_hfiles_map[prev_hset])
                tot += prev_tot
                res -= prev_tot
            for (hfile, sizegb) in hfile_sizegb_sorted:
                if (sizegb <= res) and (hfile not in hfiles):
                    hfiles.append(hfile)
                    tot += sizegb
                    res -= sizegb
            # Label the dataset with the actual dataset size.
            hset = ("{tot:.2f}GB").format(tot=tot)
            hset_hfiles_map[hset] = hfiles
            # Include smaller datasets in the next larger dataset.
            prev_tot = tot
            prev_hset = hset
            is_first = False
        # Create a separate HDFS collection for each dataset.
        for hset in sorted(hset_hfiles_map):
            sdir = os.path.join(args.sets_dir, hset)
            if args.verbose >= 1:
                print(("INFO: Appending to HDFS directory from files:\n"
                       +" {sdir}\n"
                       +" {hfiles}").format(sdir=sdir,
                                            hfiles=hset_hfiles_map[hset]))
            cmd = ("hadoop fs -mkdir -p {sdir}").format(sdir=sdir)
            sub.Popen(cmd, shell=True)
            cmd = ("hadoop fs -cp {hfiles} {sdir}").format(hfiles=hset_hfiles_map[hset],
                                                           sdir=sdir)
    return None

if __name__ == '__main__':
    arg_default_map = {}
    arg_default_map['files_in']  = None
    arg_default_map['files_dir'] = ''
    arg_default_map['sets_gb']   = None
    arg_default_map['sets_dir']  = ''
    parser = argparse.ArgumentParser(formatter_class=argparse.RawTextHelpFormatter,
                                     description="Load files into Hadoop and make nested data sets.")
    parser.add_argument('--files_in',
                        nargs='*',
                        type=str,
                        default=arg_default_map['files_in'],
                        help=("Files to load to Hadoop.\n"
                              +"Example: --files_in /path/to/files/*\n"
                              +"Default: {default}").format(default=arg_default_map['files_in']))
    parser.add_argument('--files_dir',
                        default=arg_default_map['files_dir'],
                        type=str,
                        help=(("HDFS path to load files.\n"
                               +"Example: --files_dir path/to/hadoop/dir\n"
                               +"Default: \'{default}\'").format(default=arg_default_map['files_dir'])))
    parser.add_argument('--sets_gb',
                        nargs='+',
                        type=float,
                        default=arg_default_map['sets_gb'],
                        help=("Sizes of data sets in GB. Data sets will be formed from files in Hadoop.\n"
                              +"Example: --sets_gb 1 3 10 30 100 300 1000"))
    parser.add_argument('--sets_dir',
                        default=arg_default_map['sets_dir'],
                        type=str,
                        help=(("HDFS path to copy sets.\n"
                               +"Example: --sets_dir path/to/hadoop/dir\n"
                               +"Default: \'{default}\'").format(default=arg_default_map['sets_dir'])))
    parser.add_argument('--verbose',
                        '-v',
                        action='count',
                        help=("Print 'INFO:' messages to stdout. -vv for more verbosity."))
    args = parser.parse_args()
    if args.verbose >= 1:
        print("INFO: Arguments:")
        for arg in args.__dict__:
            print('', arg, args.__dict__[arg])
    for hdir in (args.files_dir, args.sets_dir):
        cmd = "hadoop fs -test -d {hdir}".format(hdir=hdir)
        proc = sub.Popen(cmd, shell=True)
        rc = proc.returncode
        if rc != 0:
            raise IOError(("HDFS directory does not exist: {hdir}").format(hdir=hdir))
    main(args)
