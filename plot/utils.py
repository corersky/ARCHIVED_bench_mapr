#!/usr/bin/env python
"""
Utilities for plotting.
"""

# TODO: have common module for import
# TODO: have test module

from __future__ import print_function, division
import os
import ast
import json
import matplotlib.pyplot as plt
import datetime as dt

def plot(fplot='plot.pdf',
         suptitle='suptitle',
         xtitle='xtitle', ytitle='ytitle',
         label1='label1', xypairs1=[(10,1), (30,2), (100,3), (300,4)],
         label2='label2', xypairs2=[(10,1), (30,2), (100,4), (300,8)],
         xdivide=1, ydivide=1):
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
    (x1, y1) = [(x/xdivide, y/ydivide) for (x, y) in zip(*xypairs1)]
    if xypairs2 != None:
        (x2, y2) = [(x/xdivide, y/ydivide) for (x, y) in zip(*xypairs2)]
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
        plt2_kw['marker'] = 's'
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
               bbox_to_anchor=(0.13, -0.01, 0.82, 1.0),
               mode='expand')
    # Save figure as .pdf.
    fig.savefig(fplot)
    return None

def create_plot_config(fjson='plot_config.json'):
    """
    Create plot configuration file.
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
    setting_value['xdivide']  = 1
    setting_value['ydivide']  = 1
    # setting_value['label2']   = None
    # setting_value['xypairs2'] = None
    # Use binary read-write for cross-platform compatibility.
    # Use indent for human readability.
    with open(fjson, 'wb') as fp:
        json.dump(setting_value, fp, sort_keys=True, indent=4)
    return None

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
