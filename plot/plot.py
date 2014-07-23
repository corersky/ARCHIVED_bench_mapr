#!/usr/bin/env python
"""
Plot elapsed times from map-reduce job.
"""

# TODO: have common module for import
# TODO: have test module

from __future__ import print_function, division
import os
import ast
import argparse
import json
import datetime as dt
import matplotlib.pyplot as plt

def create_plot_config(fjson='plot_config.json'):
    """
    Create default plot configuration file.
    """
    setting_value = {}
    setting_value['fplot']    = 'plot.pdf'
    setting_value['suptitle'] = ("Platform, job_type, N nodes\n"
                                 +"exec. time vs data size")
    setting_value['xtitle']   = "Data size (GB)"
    setting_value['ytitle']   = "Elapsed time (min)"
    setting_value['label1']   = "Map"
    setting_value['xypairs1'] = [(10,1), (30,2), (100,3), (300,4)]
    setting_value['label2']   = "Reduce"
    setting_value['xypairs2'] = [(10,1), (30,2), (100,4), (300,8)]
    # setting_value['label2']   = None
    # setting_value['xypairs2'] = None
    # Use binary read-write for cross-platform compatibility.
    # Use indent for human readability.
    with open(fjson, 'wb') as fp:
        json.dump(setting_value, fp, sort_keys=True, indent=4)

# def plot(suptitle, xtitle, xvalues, times_map, times_reduce, fplot):
def plot(fplot, suptitle, xtitle, ytitle, label1, xypairs1, label2, xypairs2):
    """
    Plot job execution times.
    """
    # Check inputs.
    (fplot_base, ext) = os.path.splitext(fplot)
    if ext != '.pdf':
        raise IOError(("File extension not '.pdf': {fname}").format(fname=fplot))
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
    Read Disco event file and plot performance metrics.
    """

    plot_args = {}
    plot_args['suptitle'] = ("Hadoop, wordcount, 9 nodes\n"
                             +"exec time vs data set size")
    plot_args['xtitle'] = "Data set size (GB)"
    plot_args['xvalues'] = [0.94, 2.7, 9.3, 27.9, 93.1, 279.4, 1052.5]
    # plot_args['times_map'] = [6.16, 13.78, 36.04]
    # plot_args['times_reduce'] = [0, 0, 0]
    plot_args['yvalues'] = [1.867, 1.917, 4.517, 9.2, 7.883, 12.3, 68.77]
    plot_args['fplot'] = args.fplot

    plot(**plot_args)

    return None

if __name__ == '__main__':
    defaults = {}
    defaults['fconfig'] = ""
    parser = argparse.ArgumentParser(formatter_class=argparse.RawTextHelpFormatter,
                                     description="Read Disco event file and plot performance metrics.")
    # parser.add_argument("--fconfig",
    #                     default=defaults['fconfig'],
    #                     help=(("Input configuration file as .csv.\n"
    #                            +" Default: {default}").format(default=defaults['fconfig'])))
    parser.add_argument("--fplot",
                        default=defaults['fplot'],
                        help=(("Output plot file as pdf.\n"
                               +" Default: {default}").format(default=defaults['fplot'])))
    parser.add_argument("--verbose",
                        "-v",
                        action="store_true",
                        help=("Print 'INFO:' messages to stdout."))
    args = parser.parse_args()
    if args.verbose:
        print("INFO: Arguments:")
        for arg in args.__dict__:
            print("", arg, args.__dict__[arg])
    main(args)
