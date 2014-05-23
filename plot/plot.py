#!/usr/bin/env python
"""
Plot information from job events.
"""

from __future__ import print_function
from __future__ import division
import ast
import argparse
import matplotlib.pyplot as plt

"""
TODO:
use instead:
/home/sth499/anaconda/var/disco/data/_disco_8989/a2/CountWords@572:cc185:548e5/events
["2014/03/14 17:26:13","master","Starting job"]
["2014/03/14 17:26:13","master","Starting map phase"]
["2014/03/14 17:26:13","master","map:0 assigned to scout02"]
["2014/03/14 17:26:13","scout02","MSG: [map:0] Done: 2017 entries mapped"]
["2014/03/14 17:26:14","scout02","DONE: [map:0] Task finished in 0:00:00.716"]
["2014/03/14 17:26:14","master","Starting shuffle phase"]
["2014/03/14 17:26:14","master","Finished shuffle phase in 0:00:00.003"]
["2014/03/14 17:26:14","master","Finished map phase in 0:00:00.844"]
["2014/03/14 17:26:14","master","Starting reduce phase"]
["2014/03/14 17:26:14","master","reduce:0 assigned to scout02"]
["2014/03/14 17:26:14","scout02","MSG: [reduce:0] Done: 11843 entries reduced"]
["2014/03/14 17:26:15","scout02","DONE: [reduce:0] Task finished in 0:00:00.867"]
["2014/03/14 17:26:15","master","Starting shuffle phase"]
["2014/03/14 17:26:15","master","Finished shuffle phase in 0:00:00.001"]
["2014/03/14 17:26:15","master","Finished reduce phase in 0:00:01.080"]
["2014/03/14 17:26:15","master","READY: Job finished in 0:00:01.925"]
"""

def config_to_dict(fconfig):
    """
    Read Disco cluster configuration file. Return as dict.
    """
    # Disco config file is one line and formatted as a dict.
    with open(fconfig) as fr:
        line = fr.read()
        dconfig = ast.literal_eval(line)
    return dconfig

def events_to_dict(fevents):
    """
    Read Disco events file from map-reduce job. Return as dict.
    """
    # Log files from Disco can be > 100 MB.
    # Only read relevant portions into memory.
    
    # Need to query for:
    # x job.time_start, time_finish, time_elapsed, node_id
    # x job.map.x time_start, x time_finish, x time_elpased, x num_nodes, x num_maps, x num_entries, x node_id
    # job.map.map0.x time_start, x time_finish, x time_elapsed, x node_id, x num_entries
    # job.map.shuffle.time_start, time_finish, time_elapsed, node_id
    # job.reduce.time_start, time_finish, time_elapsed, num_nodes, num_reduces, num_entries, node_id
    # job.reduce.reduce0.time_start, time_finish, time_elapsed, node_id, num_entries
    # job.reduce.shuffle.time_start, time_finish, time_elapsed, node_id

    pass

def plot(fplot):
    """
    Plot job event data.
    """
    # TODO: take data from event dict, config dict
    # TODO: check len(te_map) == len(te_reduce)
    # plot
    # stacked bar chart breakdown:
    # job.time_elapsed
    # job.map.time_elapsed + job.reduce.time_elapsed + restof(job.time_elapsed)
    # (job.map.shuffle.time_elapsed + restof(job.map.time_elapsed)
    #   + job.reduce.shuffle.time_elapsed + restof(job.reduce.time_elapsed)
    #   + restof(job.time_elapsed))
    # 
    # metadata:
    # job.map.shuffle.time_elapsed    : job.map.shuffle.node_id
    # restof(job.map.time_elapsed)    : job.map.num_nodes, num_maps, num_entries
    # job.reduce.shuffle.time_elapsed : job.reduce.shuffle.node_id
    # restof(job.reduce.time_elapsed) : job.reduce.num_nodes, num reduces, num_entries
    set_sizes    = (0.18, 0.45, 0.89, 2.97, 10.35)
    times_map    = (3.07, 3.12, 3.33, 4.16, 4.33)
    times_reduce = (3.6, 9.66, 13.10, 73.14, 499.56)
    times_job    = (6.67, 12.78, 16.43, 77.30, 503.89)
    num_sets    = len(set_sizes)
    # Create figure object. 
    subplot_kw = {'xscale': 'log'}
    fig_kw = {'figsize': (4., 6.)}
    (fig, axes) = plt.subplots(nrows=2, ncols=1, sharex='col', subplot_kw=subplot_kw, **fig_kw)
    # Add bar charts.
    widths = tuple(s*0.7 for s in set_sizes)
    bars_map = axes[0].bar(left=set_sizes,
                           height=times_map,
                           width=widths,
                           color='r',
                           label="Map")
    bars_reduce = axes[0].bar(left=set_sizes,
                              height=times_reduce,
                              width=widths,
                              color='y',
                              bottom=times_map,
                              label="Reduce")
    axes[1].bar(left=set_sizes,
                height=times_map,
                width=widths,
                color='r',
                log=True)
    axes[1].bar(left=set_sizes,
                height=times_reduce,
                width=widths,
                color='y',
                bottom=times_map,
                log=True)
    # Must manually set at least common ylabel manually.
    fig.suptitle("Disco execution times for CountWords\nby data size and process")
    fig.text(x=0.5,
             y=0.085,
             s="Data set size (GB)",
             horizontalalignment='center',
             verticalalignment='center')
    fig.text(x=0.04,
             y=0.5,
             s="Elapsed time (min)",
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
    fig.legend(handles=(bars_map, bars_reduce),
               labels=(bars_map.get_label(), bars_reduce.get_label()),
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
    
    # TODO: try, except, move on if fail
    # events_to_dict(fevents)

    # TODO: try, except, move on if fail
    # plot(fplot, devents, *dconfig)
    # allow for metadata to be added to plot (e.g. data size, file name, etc)
    plot(args.fplot)

    pass

if __name__ == '__main__':
    fevents_default = "events"
    fconfig_default = "disco_8989.config"
    fplot_default   = "events.pdf"
    parser = argparse.ArgumentParser(description="Read Disco event file and plot performance metrics.")
    parser.add_argument("--fevents",
                        default=fevents_default,
                        help=(("Input event file from Disco job. "
                               +"Default: {default}").format(default=fevents_default)))
    parser.add_argument("--fconfig",
                        default=fconfig_default,
                        help=(("Input Disco cluster configuration file. "
                               +"Default: {default}").format(default=fconfig_default)))
    parser.add_argument("--fplot",
                        default=fplot_default,
                        help=(("Output plot file as pdf. "
                               +"Default: {default}").format(default=fplot_default)))
    parser.add_argument("--verbose",
                        "-v",
                        action="store_true",
                        help=("Print 'INFO:' messages to stdout."))
    args = parser.parse_args()
    if args.verbose:
        print("INFO: Arguments:")
        for arg in args.__dict__:
            print(arg, args.__dict__[arg])
    main(args)
