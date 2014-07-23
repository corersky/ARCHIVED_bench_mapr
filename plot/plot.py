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
import matplotlib.pyplot as plt

def plot(fplot, suptitle, xtitle, ytitle, label1, xypairs1, label2, xypairs2):
    """
    Plot job execution times.
    """
    # Check inputs.
    (fplot_base, ext) = os.path.splitext(fplot)
    if ext != '.pdf':
        raise IOError(("File extension not '.pdf': {fplot}").format(fplot=fplot))
    # Create figure object.
    subplot_kw = {}
    subplot_kw['xscale'] = 'log'
    fig_kw = {}
    fig_kw['figsize'] = (4., 6.)
    (fig, ax) = plt.subplots(nrows=2, ncols=1, sharex='col', subplot_kw=subplot_kw, **fig_kw)
    # Plot data.
    (x1, y1) = zip(*xypairs1)
    if xypairs2 != None:
        (x2, y2) = zip(*xypairs2)
        has_2_xypairs = True
    else:
        # TODO: use event logger to handle INFO messages.
        print("INFO: No xypairs2.")
        has_2_xypairs = False
    plt1_kw = {}
    plt1_kw['color'] = 'blue'
    plt1_kw['linestyle'] = '-'
    plt1_kw['marker'] = 'o'
    plt1_kw['label'] = label1
    ax[0].semilogx(x1, y1, **plt1_kw)
    ax[1].loglog(x1, y1, **plt1_kw)
    if has_2_xypairs:
        plt2_kw = {}
        plt2_kw['color'] = 'green'
        plt2_kw['linestyle'] = '-'
        plt2_kw['marker'] = 'o'
        plt2_kw['label'] = label2
        ax[0].semilogx(x2, y2, **plt2_kw)
        ax[1].loglog(x2, y2, **plt2_kw)
    # Set figure text.
    fig.suptitle(suptitle)
    fig.text(x=0.5,
             y=0.085,
             s=xtitle,
             horizontalalignment='center',
             verticalalignment='center')
    fig.text(x=0.04,
             y=0.5,
             s=ytitle,
             horizontalalignment='center',
             verticalalignment='center',
             rotation='vertical')
    # Shift lower plot up to make room for legend at bottom.
    # Add metadata to legend labels.
    box0 = ax[0].get_position()
    ax[0].set_position([box0.x0 + 0.03,
                        box0.y0,
                        box0.width,
                        box0.height])
    box1 = ax[1].get_position()
    ax[1].set_position([box1.x0 + 0.03,
                        (box1.y0 + 0.04),
                        box1.width,
                        box1.height])
    (handles, labels) = ax[0].get_legend_handles_labels()
    if not has_2_xypairs:
        ncol = 1
    else:
        ncol = 2
    fig.legend(handles,
               labels,
               ncol=ncol,
               loc='lower center',
               bbox_to_anchor=(0.05, -0.01, 0.9, 1.),
               mode='expand')
    # Save figure as .pdf.
    fig.savefig(fplot)
    return None

def main(args):
    """
    Plot job execution times.
    """
    with open(args.fconfig, 'rb') as fp:
        plot_args = json.load(fp)
    plot(**plot_args)
    return None

if __name__ == '__main__':
    defaults = {}
    defaults['fconfig'] = "plot_config.json"
    parser = argparse.ArgumentParser(formatter_class=argparse.RawTextHelpFormatter,
                                     description="Read Disco event file and plot performance metrics.")
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
