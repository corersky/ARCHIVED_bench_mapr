#!/usr/bin/env python
"""
Plot elapsed times from map-reduce job.
"""

# TODO: use event logger to handle info message
# TODO: have test module
# TODO: rename module to main, move utilties to utils

from __future__ import print_function, division
import os
import argparse
import json
import utils

def main(args):
    """
    Plot job execution times.
    """
    with open(args.fconfig, 'rb') as fp:
        plot_args = json.load(fp)
    utils.plot(**plot_args)
    return None

if __name__ == '__main__':
    defaults = {}
    defaults['fconfig'] = "plot_config.json"
    parser = argparse.ArgumentParser(formatter_class=argparse.RawTextHelpFormatter,
                                     description="Read configuration file and plot.")
    parser.add_argument("--fconfig",
                        default=defaults['fconfig'],
                        help=(("Input configuration file as .json.\n"
                               +" Default: {default}").format(default=defaults['fconfig'])))
    parser.add_argument("--verbose",
                        "-v",
                        action="store_true",
                        help=("Print 'INFO:' messages to stdout."))
    args = parser.parse_args()
    if not os.path.isfile(args.fconfig):
        raise IOError(("Configuration file does not exist: {fconfig}").format(fconfig=args.fconfig))
    (fconfig_base, ext) = os.path.splitext(args.fconfig)
    if ext != '.json':
        raise IOError(("Configuration file extension not '.json': {fconfig}").format(fconfig=args.fconfig))
    if args.verbose:
        print("INFO: Arguments:")
        for arg in args.__dict__:
            print("", arg, args.__dict__[arg])
    main(args)
