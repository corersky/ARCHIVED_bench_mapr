#!/usr/bin/env python
"""
Plot information from job events.
"""

from __future__ import print_function
from __future__ import division
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

def events_to_obj(fevents):
    """
    Read event file and return object with metadata attributes.
    """
    # Log files from Disco can be > 100 MB.
    # Only read relevant portions into memory.

    # create dict then make into object
    # from collections import namedtuple
    # http://stackoverflow.com/questions/1305532/convert-python-dict-to-object

    # Need to query for:
    # job.sys
    # job.time_start, time_finish, time_elapsed, node_id
    # job.map.time_start, time_finish, time_elpased, num_nodes, num_maps, num_entries, node_id
    # job.map.map0.time_start, time_finish, time_elapsed, node_id, num_entries
    # job.map.shuffle.time_start, time_finish, time_elapsed, node_id
    # job.reduce.time_start, time_finish, time_elapsed, num_nodes, num_reduces, num_entries, node_id
    # job.reduce.reduce0.time_start, time_finish, time_elapsed, node_id, num_entries
    # job.reduce.shuffle.time_start, time_finish, time_elapsed, node_id

    pass

def plot(times_job, times_map, times_reduce, num_sets, set_sizes, fplot):
    """
    Plot job event data.
    """
    # TODO: take data from event object
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
    times_job      = (0.3, 0.9, 3, 9, 30)
    times_map      = (0.1, 0.3, 1, 3, 10)
    times_reduce   = (0.2, 0.6, 2, 6, 20)
    num_sets    = len(times_map)
    set_sizes   = (0.1, 0.3, 1, 3, 10)
    # Create figure object. 
    subplot_kw = {'xscale': 'log'}
    fig_kw = {'figsize': (4., 6.)}
    (fig, axes) = plt.subplots(nrows=2, ncols=1, sharex='col', subplot_kw=subplot_kw, **fig_kw)
    # Add bar charts.
    bars_map = axes[0].bar(left=set_sizes,
                           height=times_map,
                           width=set_sizes,
                           color='r',
                           label="Map")
    bars_reduce = axes[0].bar(left=set_sizes,
                              height=times_reduce,
                              width=set_sizes,
                              color='y',
                              bottom=times_map,
                              label="Reduce")
    axes[1].bar(left=set_sizes,
                height=times_map,
                width=set_sizes,
                color='r',
                log=True)
    axes[1].bar(left=set_sizes,
                height=times_reduce,
                width=set_sizes,
                color='y',
                bottom=times_map,
                log=True)
    # Must manually set at least common ylabel manually.
    fig.suptitle("Disco execution times for CountWords\nby data size and process")
    fig.text(x=0.5,
             y=0.11,
             s="Data set size (GB)",
             horizontalalignment='center',
             verticalalignment='center')
    fig.text(x=0.02,
             y=0.5,
             s="Elapsed time (s)",
             horizontalalignment='center',
             verticalalignment='center',
             rotation='vertical')
    # Shift lower plot up to make room for legend at bottom.
    # Add metadata to legend labels.
    box = axes[1].get_position()
    axes[1].set_position([box.x0,
                          (box.y0 + 0.04),
                          box.width,
                          box.height])
    fig.legend(handles=(bars_map, bars_reduce),
               labels=(bars_map.get_label(), bars_reduce.get_label()),
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
    # events_to_obj(fevents)

    # TODO: try, except, move on if fail
    # plot(events, fplot)
    # allow for metadata to be added to plot (e.g. data size, file name, etc)
    pass

if __name__ == '__main__':
    fevents_default = "events"
    fplot_default  = "events.pdf"
    parser = argparse.ArgumentParser(description="Read Disco event file and plot performance metrics.")
    parser.add_argument("--fevents",
                        default=fevents_default,
                        help=(("Input event file from Disco job. "
                               +"Default: {default}").format(default=fevents_default)))
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
