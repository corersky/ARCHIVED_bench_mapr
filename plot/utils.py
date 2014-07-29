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
import collections
import datetime as dt
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages

def duration_to_timedelta(duration):
    """
    Convert duration from HH:MM:SS to datetime timedelta object.
    """
    duration_arr = map(float, duration.split(':'))
    duration_td = dt.timedelta(hours=duration_arr[0],
                               minutes=duration_arr[1],
                               seconds=duration_arr[2])
    return duration_td

def disco_events_to_dict(fevents):
    """
    Parse Disco events file from map-reduce job. Return as dict.
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
    with open(fevents, 'rb') as fp:
        map_started = False
        map_finished = False
        reduce_started = False
        reduce_finished = False
        for line in fp:
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

def hadoop_log_to_dict(flog):
    """
    Parse file with Hadoop standard output. Return as a dict.
    """
    # Outout from Hadoop is ~50 KB.
    # Only read relevant portions into memory, example:
    # http://blog.cloudera.com/blog/2013/01/a-guide-to-python-frameworks-for-hadoop/
    # Example records:
    # ```
    # 14/07/26 02:52:40 INFO mapreduce.Job: Running job: job_1405523452120_0002
    # 14/07/26 02:53:40 INFO mapreduce.Job:  map 75% reduce 8%
    # 14/07/26 02:54:25 INFO mapreduce.Job: Job job_1405523452120_0002 completed successfully
    # 14/07/26 02:54:25 INFO mapreduce.Job: Counters: 51
    # File System Counters
    # FILE: Number of bytes read=336130040
    # FILE: Number of bytes written=673071732
    # HDFS: Number of bytes read=980649565
    # ```
    log = {}
    timestamp_fmt = "%y/%m/%d %H:%M:%S"
    with open(flog, 'rb') as fp:
        # Initialize job tracking variables.
        job_started = False
        job_id = None
        job_completed = False
        for line in fp:
            line = line.strip()
            # TEST:
            print(line)
            # No job is started...
            if ((job_started == False) and
                (job_completed == False)):
                # TEST
                print("TEST: job started and completed both false")
                # Retain only 'INFO' messages...
                if 'INFO' in line:
                    # TEST
                    print("TEST: info in line")
                    # Hadoop log records will have only 4 fields: date, time, level, message...
                    arr = line.strip().split(' ', 3)
                    if len(arr) == 4:
                        # test
                        print("TEST: len is 4")
                        # Parse the timestamp...
                        try:
                            dt_event = dt.datetime.strptime(arr[0]+' '+arr[1], timestamp_fmt)
                            # A new job has started...
                            if 'mapreduce.Job: Running job:' in arr[3]:
                                job_id = arr[3].rsplit(' ', 1)[-1]
                                log[job_id] = {}
                                log[job_id]['progress'] = {}
                                log[job_id]['summary'] = {}
                                # test
                                print("TEST: ",log)
                                job_started = True
                                continue
                            # otherwise ignore Hadoop startup messages.
                            else:
                                continue
                        # otherwise ignore lines without timestamps.
                        except ValueError:
                            continue
                    # otherwise ignore lines without 4 fields.
                    else:
                        continue
                # otherwise ignore non-'INFO' messages.
                else:
                    continue
            # otherwise job is in progress...
            elif ((job_started == True) and
                  (job_completed == False)):
                # Retain only 'INFO' messages...
                if 'INFO' in line:
                    # Hadoop log records will have only 4 fields: date, time, level, message...
                    arr = line.strip().split(' ', 3)
                    if len(arr) == 4:
                        # Parse the timestamp...
                        try:
                            dt_event = dt.datetime.strptime(arr[0]+' '+arr[1], timestamp_fmt)
                            # Progress on a current job...
                            if 'mapreduce.Job:  map' in arr[3]:
                                progress = arr[3].split()[1:]
                                progress = [tuple(progress[idx: idx+2]) for idx in xrange(0, len(progress), 2)]
                                progress = [(task, float(pct.strip('%'))/100) for (task, pct) in progress]
                                log[job_id]['progress'][dt_event] = progress
                                continue
                            # otherwise job is complete...
                            elif arr[3] == 'mapreduce.Job: '+job_id+' completed successfully':
                                job_completed = True
                                continue
                            # otherwise ignore other messages.
                            else:
                                continue
                        # otherwise ignore lines without timestamps.
                        except ValueError:
                            continue
                    # otherwise ignore lines without 4 fields.
                    else:
                        continue
                # otherwise ignore non-'INFO' messages.
                else:
                    continue
            # otherwise job has just completed.
            elif ((job_started == True) and
                  (job_completed == True)):
                print(log)
                # parse = statements
                # # Reset job tracking variables.
                # job_started = False
                # job_id = None
                # job_completed = False
                continue
            # otherwise there was an error.
            else:
                raise AssertionError(("Hadoop job may have failed. Check log manually."))
                
                
        ## if the line's first 2 elements dont make a datetime, ignore
        ## if they do, parse into datetime, level, message
        ##     if INFO message contains "mapreduce.Job: Running job:" save job_id, job_started=True
        ##         if INFO message contains "mapreduce.Job: map",
        ##             save dict[progress][datetime]=[(key1, field1), ...]
        #         if INFO message contains "mapreduce.Job: Counters":
        #             if 'File System Counters', 'Job Counters', 'Map-Reduce Framework', 'Shuffle Errors',
        #                 'File Input Format Counters', File Output Format Counters' then dict['File...']
        #                 split on '=', else job_completed=True
    return None

def dict_to_class(dobj):
    """
    Convert keys of a dict into attributes of a class.
    """
    Dclass = collections.namedtuple('Dclass', dobj.keys())
    return Dclass(**dobj)

def create_plot_config(fjson='plot_config.json'):
    """
    Create plot configuration file.
    """
    setting_value = collections.OrderedDict()
    setting_value['fplot']     = 'plot.pdf'
    setting_value['infodict']  = collections.OrderedDict()
    setting_value['infodict']['Title']    = "PDF title"
    setting_value['infodict']['Author']   = "PDF author"
    setting_value['infodict']['Subject']  = "PDF subject"
    setting_value['infodict']['Keywords'] = "PDF keywords"
    setting_value['comments'] = ["Insert multiline",
                                  "comments here."]
    setting_value['suptitle'] = ("Platform, job_type, N nodes\n"
                                 +"exec. time vs data size")
    setting_value['xtitle']   = "Data size (GB)"
    setting_value['ytitle']   = "Elapsed time (min)"
    setting_value['label1']   = "Map (num)"
    setting_value['xypairs1'] = [(10,1), (30,2), (100,3), (300,4)]
    setting_value['xyantn1']  = [1, 2, 3, 4]
    setting_value['prefantn1']= "m"
    setting_value['label2']   = "Reduce (num)"
    setting_value['xypairs2'] = [(10,1), (30,2), (100,4), (300,8)]
    setting_value['xyantn2']  = [1, 1, 1, 1]
    setting_value['prefantn2']= "r"
    # setting_value['label2']   = None
    # setting_value['xypairs2'] = None
    # setting_value['xyantn2']  = None
    setting_value['xdivide']  = 1
    setting_value['ydivide']  = 1
    setting_value['ydivbyantn'] = True
    # Use binary read-write for cross-platform compatibility.
    # Use indent for human readability.
    with open(fjson, 'wb') as fp:
        json.dump(setting_value, fp, sort_keys=False, indent=4)
    return None

def plot(args):
    """
    Plot job execution times.
    """
    # TODO: combine labels, xypairs, xyannotations into dataframe and reference by df_label.
    # Check inputs, open PDF file, and print/save metadata.
    (fplot_base, ext) = os.path.splitext(args.fplot)
    if ext != '.pdf':
        raise IOError(("File extension not '.pdf': {fplot}").format(fplot=args.fplot))
    print(("INFO: Writing plot:\n"+
           "  {fplot}").format(fplot=args.fplot))
    pdf = PdfPages(args.fplot)
    pdf_infodict = pdf.infodict()
    if args.infodict != {}:
        for key in args.infodict:
            pdf_infodict[key] = args.infodict[key]
    if args.comments != []:
        fcomm = fplot_base+"_comments.txt"
        print(("INFO: Writing comments:\n"+
               "  {fcomm}").format(fcomm=fcomm))
        with open(fcomm, 'wb') as fp:
            fp.write(("\n".join(args.comments))+"\n")
    # Create figure object.
    subplot_kw = {'xscale': 'log'}
    fig_kw = {'figsize': (4, 6)}
    (fig, ax) = plt.subplots(nrows=2, ncols=1, sharex='col', subplot_kw=subplot_kw, **fig_kw)
    # Plot normalized data and annotations.
    (x1, y1) = zip(*[(x/args.xdivide, y/args.ydivide) for (x, y) in args.xypairs1])
    if args.ydivbyantn:
        y1 = [y/antn for (y, antn) in zip(y1, args.xyantn1)]
    plt1_kw = {'color'    : 'blue',
               'linestyle': '-',
               'marker'   : 'o',
               'label'    : args.label1}
    ax[0].semilogx(x1, y1, **plt1_kw)
    ax[1].loglog(x1, y1, **plt1_kw)
    xypairs1 = zip(x1, y1)
    for a in ax:
        for idx in xrange(len(args.xyantn1)):
            a.annotate(args.prefantn1+str(args.xyantn1[idx]), xy=xypairs1[idx], xycoords='data',
                       xytext=(+0, +0), textcoords='offset points')
    if args.xypairs2 != None:
        (x2, y2) = zip(*[(x/args.xdivide, y/args.ydivide) for (x, y) in args.xypairs2])
        if args.ydivbyantn:
            y2 = [y/antn for (y, antn) in zip(y2, args.xyantn2)]
        plt2_kw = {'color'    : 'green',
                   'linestyle': '-',
                   'marker'   : 's',
                   'label'    : args.label2}
        ax[0].semilogx(x2, y2, **plt2_kw)
        ax[1].loglog(x2, y2, **plt2_kw)
        xypairs2 = zip(x2, y2)
        for a in ax:
            for idx in xrange(len(args.xyantn2)):
                a.annotate(args.prefantn2+str(args.xyantn2[idx]), xy=xypairs2[idx], xycoords='data',
                           xytext=(+0, -12), textcoords='offset points')
    else:
        # TODO: use event logger to handle INFO messages.
        print("INFO: No xypairs2.")
    # Scale plot heights and shift lower plot up to make room for legend at bottom.
    # Reference corner for positions is lower right. Position tuples: (left, bottom, width, height)
    # Add metadata to legend labels.
    box0 = ax[0].get_position()
    wscale  = 1
    vscale  = 0.95
    xoffset = 0.03
    yoffset = (1-vscale)*box0.height
    ygap    = 0.04
    ax[0].set_position([box0.x0 + xoffset,
                        box0.y0 + yoffset,
                        wscale*box0.width,
                        vscale*box0.height])
    box1 = ax[1].get_position()
    ax[1].set_position([box1.x0 + xoffset,
                        box1.y0 + ygap + 2*yoffset,
                        wscale*box1.width,
                        vscale*box1.height])
    # Set figure titles and legend.
    # Reference corner for positions is lower right. Position tuples: (left, bottom, width, height)
    fig_suptitle = fig.suptitle(args.suptitle)
    fig_xtitle = fig.text(x=0.5,
                          y= 2*ygap + 2*(1-vscale)*box1.height,
                          s=args.xtitle,
                          horizontalalignment='center',
                          verticalalignment='center')
    fig_ytitle = fig.text(x=xoffset,
                          y=0.5,
                          s=args.ytitle,
                          horizontalalignment='center',
                          verticalalignment='center',
                          rotation='vertical')
    (handles, labels) = ax[0].get_legend_handles_labels()
    fig.legend(handles,
               labels,
               loc='lower center',
               bbox_to_anchor=(0.1 + xoffset,
                               -0.005,
                               # -0.06 + 3*(1-vscale)*box1.height,
                               0.825,
                               1.00),
               mode='expand',
               fontsize=fig_xtitle.get_fontsize())
    # Save figure and close PDF file.
    pdf.savefig()
    pdf.close()
    return None

