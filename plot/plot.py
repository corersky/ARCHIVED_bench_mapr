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
    setting_value['xvalues']  = [1, 3, 10, 30, 100, 300, 1000]
    setting_value['ytitle']   = "Elapsed time (min)"
    setting_value['yvalues']  = [1, 2, 4, 8, 16, 32, 64]
    # Use binary read-write for cross-platform compatibility.
    # Use indent for human readability.
    with open(fjson, 'wb') as fp:
        json.dump(setting_value, fp, sort_keys=True, indent=4)

# def plot(suptitle, xtitle, xvalues, times_map, times_reduce, fplot):
def plot(fplot, suptitle, xtitle, xvalues, ytitle, yvalues):
    """
    Plot job execution times.
    """
    # TODO:
    #  - if ndim yvalues = 2, make double plots
    #  - check for labels
    # Check inputs
    num_xvalues = len(xvalues)
    # TODO: Raise assertion error
    # if len(times_map) != num_xvalues:
    #     raise IOError(("Number of map jobs != number of x-axis values.\n"
    #                    +" xvalues = {xvalues}\n"
    #                    +" times_map = {times_map}").format(xvalues=xvalues,
    #                                                        times_map=times_map))
    # if len(times_reduce) != num_xvalues:
    #     raise IOError(("Number of reduce jobs != number of x-axis values.\n"
    #                    +" xvalues = {xvalues}\n"
    #                    +" times_reduce = {times_reduce}").format(xvalues=xvalues,
    #                                                              times_reduce=times_reduce))
    if len(yvalues) != num_xvalues:
        raise IOError(("Number of map jobs != number of x-axis values.\n"
                       +" xvalues = {xvalues}\n"
                       +" yvalues = {yvalues}").format(xvalues=xvalues,
                                                           yvalues=yvalues))
    (fplot_base, ext) = os.path.splitext(fplot)
    if ext != '.pdf':
        raise IOError(("File extension not '.pdf': {fname}").format(fname=fplot))

    # Create figure object.
    subplot_kw = {'xscale': 'log'}
    fig_kw = {'figsize': (4., 6.)}
    (fig, axes) = plt.subplots(nrows=2, ncols=1, sharex='col', subplot_kw=subplot_kw, **fig_kw)
    # Add bar charts.
    widths = tuple(s*0.7 for s in xvalues)
    # TODO: redo with bars side-by-side
    # bars_map = axes[0].bar(left=xvalues,
    #                        height=times_map,
    #                        width=widths,
    #                        color='r',
    #                        label="Map")
    # bars_reduce = axes[0].bar(left=xvalues,
    #                           height=times_reduce,
    #                           width=widths,
    #                           color='y',
    #                           bottom=times_map,
    #                           label="Reduce")
    bars = axes[0].bar(left=xvalues,
                       height=yvalues,
                       width=widths,
                       color='r',
                       label="Total")
    # axes[1].bar(left=xvalues,
    #             height=times_map,
    #             width=widths,
    #             color='r',
    #             log=True)
    # axes[1].bar(left=xvalues,
    #             height=times_reduce,
    #             width=widths,
    #             color='y',
    #             bottom=times_map,
    #             log=True)
    axes[1].bar(left=xvalues,
                height=yvalues,
                width=widths,
                color='r',
                log=True)
    # Must manually set at least common ylabel manually.
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
    box0 = axes[0].get_position()
    axes[0].set_position([box0.x0 + 0.03,
                          box0.y0,
                          box0.width,
                          box0.height])
    box1 = axes[1].get_position()
    axes[1].set_position([box1.x0 + 0.03,
                          (box1.y0 + 0.04),
                          box1.width,
                          box1.height])
    # fig.legend(handles=(bars_map, bars_reduce),
    #            labels=(bars_map.get_label(), bars_reduce.get_label()),
    #            ncol=2,
    #            loc='lower center',
    #            bbox_to_anchor=(0.05, -0.01, 0.9, 1.),
    #            mode='expand')
    fig.legend(handles=(bars),
               labels=(bars.get_label(), ),
               ncol=2,
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
    # TODO: use configparser instead
    # defaults['fconfig'] = "config.csv"
    defaults['fplot'] = "plot.pdf"
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
