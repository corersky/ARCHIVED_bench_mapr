#!/usr/bin/env python
"""
Plot elapsed times from map-reduce job.
"""

from __future__ import print_function, division
import os
import ast
import argparse
import datetime as dt
import matplotlib.pyplot as plt

def config_to_dict(fconfig):
    """
    Read Disco cluster configuration file. Return as dict.
    """
    # TODO: use configparser instead
    # Disco config file is one line and formatted as a dict.
    with open(fconfig) as fr:
        line = fr.read()
        dconfig = ast.literal_eval(line.strip())
    return dconfig

def duration_to_timedelta(duration):
    """
    Convert duration from HH:MM:SS to datetime timedelta object.
    """
    duration_arr = map(float, duration.split(':'))
    duration_td = dt.timedelta(hours=duration_arr[0],
                               minutes=duration_arr[1],
                               seconds=duration_arr[2])
    return duration_td

def events_to_dict(fevents):
    """
    Read Disco events file from map-reduce job. Return as dict.
    """
    # Log files from Disco can be > 100 MB.
    # Only read relevant portions into memory.
    # Example record format:
    # ['2014/03/14 17:26:14', 'scout02', 'DONE: [map:0] Task finished in 0:00:00.716']
    # TODO: allow for chained map-reduce jobs
    # TODO: move to disco-specific utils module
    # Events for a single job.
    events = {}
    timestamp_fmt = "%Y/%m/%d %H:%M:%S"
    with open(fevents) as fr:
        map_started = False
        map_finished = False
        reduce_started = False
        reduce_finished = False
        for line in fr:
            arr = ast.literal_eval(line.strip())
            # Start job
            if 'master' in arr[1]:
                # ["2014/03/14 17:26:13","master","Starting job"]
                if 'Starting job' in arr[2]:
                    events['node_id'] = arr[1]
                    events['time_start'] = dt.datetime.strptime(arr[0], timestamp_fmt)
                    continue
            # Map
            if 'master' in arr[1]:
                # ["2014/03/14 17:26:13","master","Starting map phase"]
                if 'Starting map phase' in arr[2]:
                    map_started = True
                    events['map'] = {}
                    events['map']['node_id'] = arr[1]
                    events['map']['time_start'] = dt.datetime.strptime(arr[0], timestamp_fmt)
                    continue
                # ["2014/03/14 17:26:13","master","map:0 assigned to scout02"]
                if (('map' in arr[2])
                    and ('assigned to' in arr[2])):
                    # map_id is 1st word in string, e.g. map:0
                    # node_id is last word in string, e.g. scout02
                    map_id = (arr[2].split(None, 1)[0]).replace(':', '_')
                    events['map'][map_id] = {}
                    events['map'][map_id]['node_id'] = arr[2].rsplit(None, 1)[-1]
                    events['map'][map_id]['time_start'] = dt.datetime.strptime(arr[0], timestamp_fmt)
                    continue
            # ["2014/03/14 17:26:13","scout02","MSG: [map:0] Done: 2017 entries mapped"]
            if 'master' not in arr[1]:
                if (('MSG: [map' in arr[2])
                    and ('Done:' in arr[2])
                    and ('entries mapped' in arr[2])):
                    # map_id is 2nd word in string and has [], e.g. [map:0]
                    # num_entries is 4th word in string.
                    arr_msg = arr[2].split()
                    map_id = (((arr_msg[1]).replace(':', '_')).replace('[', '')).replace(']', '')
                    num_entries = int(arr_msg[3])
                    events['map'][map_id]['num_entries'] = num_entries
                    continue
                # ["2014/03/14 17:26:14","scout02","DONE: [map:0] Task finished in 0:00:00.716"]
                if (('DONE: [map' in arr[2])
                    and ('Task finished in' in arr[2])):
                    # map_id is 2nd word in string and has [], e.g. [map:0]
                    # time_elapsed is last word in string, e.g. 0:00:00.716
                    arr_msg = arr[2].split()
                    map_id = (((arr_msg[1]).replace(':', '_')).replace('[', '')).replace(']', '')
                    events['map'][map_id]['time_finish'] = dt.datetime.strptime(arr[0], timestamp_fmt)
                    events['map'][map_id]['time_elapsed'] = duration_to_timedelta(arr_msg[-1])
                    continue
            if 'master' in arr[1]:
                if map_started and not map_finished:
                    # ["2014/03/14 17:26:14","master","Starting shuffle phase"]
                    if 'Starting shuffle phase' in arr[2]:
                        events['map']['shuffle'] = {}
                        events['map']['shuffle']['node_id'] = arr[1]
                        events['map']['shuffle']['time_start'] = dt.datetime.strptime(arr[0], timestamp_fmt)
                        continue
                    # ["2014/03/14 17:26:14","master","Finished shuffle phase in 0:00:00.003"]
                    if 'Finished shuffle phase' in arr[2]:
                        # time_elapsed is last word in string, e.g. 0:00:00.716
                        events['map']['shuffle']['time_finish'] = dt.datetime.strptime(arr[0], timestamp_fmt)
                        events['map']['shuffle']['time_elapsed'] = duration_to_timedelta(arr[2].rsplit(None, 1)[-1])
                        continue     
                    # ["2014/03/14 17:26:14","master","Finished map phase in 0:00:00.844"]
                    if 'Finished map phase' in arr[2]:
                        map_finished = True
                        # time_elapsed is last word in string, e.g. 0:00:00.716
                        events['map']['time_finish'] = dt.datetime.strptime(arr[0], timestamp_fmt)
                        events['map']['time_elapsed'] = duration_to_timedelta(arr[2].rsplit(None, 1)[-1])
                        # Tally nodes, entries, and maps.
                        list_nodes = []
                        events['map']['num_entries'] = 0
                        events['map']['num_maps'] = 0
                        for key in events['map']:
                            if 'map_' in key:
                                if events['map'][key]['node_id'] not in list_nodes:
                                    list_nodes.append(events['map'][key]['node_id'])
                                events['map']['num_entries'] += events['map'][key]['num_entries']
                                events['map']['num_maps'] += 1
                        events['map']['num_nodes'] = len(list_nodes)
                        continue
            # Reduce
            if 'master' in arr[1]:
                # ["2014/03/14 17:26:14","master","Starting reduce phase"]
                if 'Starting reduce phase' in arr[2]:
                    reduce_started = True
                    events['reduce'] = {}
                    events['reduce']['node_id'] = arr[1]
                    events['reduce']['time_start'] = dt.datetime.strptime(arr[0], timestamp_fmt)
                    continue
                # ["2014/03/14 17:26:14","master","reduce:0 assigned to scout02"]
                if (('reduce' in arr[2])
                    and ('assigned to' in arr[2])):
                    # reduce_id is 1st word in string, e.g. reduce:0
                    # node_id is last word in string, e.g. scout02
                    reduce_id = (arr[2].split(None, 1)[0]).replace(':', '_')
                    events['reduce'][reduce_id] = {}
                    events['reduce'][reduce_id]['node_id'] = arr[2].rsplit(None, 1)[-1]
                    events['reduce'][reduce_id]['time_start'] = dt.datetime.strptime(arr[0], timestamp_fmt)
                    continue
            if 'master' not in arr[1]:
                # ["2014/03/14 17:26:14","scout02","MSG: [reduce:0] Done: 11843 entries reduced"]
                if (('MSG: [reduce' in arr[2])
                    and ('Done:' in arr[2])
                    and ('entries reduced' in arr[2])):
                    # reduce_id is 2nd word in string and has [], e.g. [reduce:0]
                    # num_entries is 4th word in string.
                    arr_msg = arr[2].split()
                    reduce_id = (((arr_msg[1]).replace(':', '_')).replace('[', '')).replace(']', '')
                    num_entries = int(arr_msg[3])
                    events['reduce'][reduce_id]['num_entries'] = num_entries
                    continue
                # ["2014/03/14 17:26:15","scout02","DONE: [reduce:0] Task finished in 0:00:00.867"]
                if (('DONE: [reduce' in arr[2])
                    and ('Task finished in' in arr[2])):
                    # reduce_id is 2nd word in string and has [], e.g. [reduce:0]
                    # time_elapsed is last word in string, e.g. 0:00:00.716
                    arr_msg = arr[2].split()
                    reduce_id = (((arr_msg[1]).replace(':', '_')).replace('[', '')).replace(']', '')
                    events['reduce'][reduce_id]['time_finish'] = dt.datetime.strptime(arr[0], timestamp_fmt)
                    events['reduce'][reduce_id]['time_elapsed'] = duration_to_timedelta(arr_msg[-1])
                    continue
            if 'master' in arr[1]:
                if reduce_started and not reduce_finished:
                    # ["2014/03/14 17:26:15","master","Starting shuffle phase"]
                    if 'Starting shuffle phase' in arr[2]:
                        events['reduce']['shuffle'] = {}
                        events['reduce']['shuffle']['node_id'] = arr[1]
                        events['reduce']['shuffle']['time_start'] = dt.datetime.strptime(arr[0], timestamp_fmt)
                        continue
                    # ["2014/03/14 17:26:15","master","Finished shuffle phase in 0:00:00.001"]
                    if 'Finished shuffle phase' in arr[2]:
                        # time_elapsed is last word in string, e.g. 0:00:00.716
                        events['reduce']['shuffle']['time_finish'] = dt.datetime.strptime(arr[0], timestamp_fmt)
                        events['reduce']['shuffle']['time_elapsed'] = duration_to_timedelta(arr[2].rsplit(None, 1)[-1])
                        continue     
                    # ["2014/03/14 17:26:15","master","Finished reduce phase in 0:00:01.080"]
                    if 'Finished reduce phase' in arr[2]:
                        reduce_finished = True
                        # time_elapsed is last word in string, e.g. 0:00:00.716
                        events['reduce']['time_finish'] = dt.datetime.strptime(arr[0], timestamp_fmt)
                        events['reduce']['time_elapsed'] = duration_to_timedelta(arr[2].rsplit(None, 1)[-1])
                        # Tally nodes, entries, and reduces.
                        list_nodes = []
                        events['reduce']['num_entries'] = 0
                        events['reduce']['num_reduces'] = 0
                        for key in events['reduce']:
                            if 'reduce_' in key:
                                if events['reduce'][key]['node_id'] not in list_nodes:
                                    list_nodes.append(events['reduce'][key]['node_id'])
                                events['reduce']['num_entries'] += events['reduce'][key]['num_entries']
                                events['reduce']['num_reduces'] += 1
                        events['reduce']['num_nodes'] = len(list_nodes)
                        continue
            # Finish job
            # ["2014/03/14 17:26:15","master","READY: Job finished in 0:00:01.925"]
            if 'master' in arr[1]:
                if 'READY: Job finished' in arr[2]:
                    # time_elapsed is last word in string, e.g. 0:00:00.716
                    events['time_finish'] = dt.datetime.strptime(arr[0], timestamp_fmt)
                    events['time_elapsed'] = duration_to_timedelta(arr[2].rsplit(None, 1)[-1])
                    continue
    return events

# def plot(suptitle, xtitle, xvalues, times_map, times_reduce, fplot):
def plot(suptitle, xtitle, xvalues, times, fplot):
    """
    Plot job execution times.
    """
    # Check inputs
    num_xvalues = len(xvalues)
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
    if len(times) != num_xvalues:
        raise IOError(("Number of map jobs != number of x-axis values.\n"
                       +" xvalues = {xvalues}\n"
                       +" times = {times}").format(xvalues=xvalues,
                                                           times=times))
    (fplot_base, ext) = os.path.splitext(fplot)
    if ext != '.pdf':
        raise IOError(("File extension not '.pdf': {fname}").format(fname=fplot))

    # Create figure object.
    subplot_kw = {'xscale': 'log'}
    fig_kw = {'figsize': (4., 6.)}
    (fig, axes) = plt.subplots(nrows=2, ncols=1, sharex='col', subplot_kw=subplot_kw, **fig_kw)
    # Add bar charts.
    widths = tuple(s*0.7 for s in xvalues)
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
                       height=times,
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
                height=times,
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
    # TODO: allow for metadata to be added to plot (e.g. data size, file name, etc)
    # # TODO: use configparse for config files
    # (fconfig_base, ext) = os.path.splitext(args.fconfig)
    # if ext != '.csv':
    #     raise IOError(("File extension is not '.csv': {fname}").format(fname=args.fconfig))
    # fconfig_nocmts = fconfig_base + '_temp' + ext
    # with open(args.fconfig, 'r') as fcmts:
    #     with open(fconfig_nocmts, 'w') as fnocmts:
    #         for line in fcmts:
    #             if line.startswith('#'):
    #                 continue
    #             else:
    #                 fnocomts.write(line)
    # dfconfig = pd.read_csv(fconfig_nocmts)
    # fconfigargs.fconfig

    plot_args = {}
    plot_args['suptitle'] = ("Hadoop, terasort, 9 nodes\n"
                             +"elapsed time vs data set size, sets from teragen")
    plot_args['xtitle'] = "Data set size (GB)"
    plot_args['xvalues'] = [1, 3, 10, 30, 100, 300]
    # plot_args['times_map'] = [6.16, 13.78, 36.04]
    # plot_args['times_reduce'] = [0, 0, 0]
    plot_args['times'] = [1.6588, 2.5679, 5.9771, 18.16, 58.9792, 265.7049]
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
