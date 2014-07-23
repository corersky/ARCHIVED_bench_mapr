#!/usr/bin/env python
"""
Utilities for plotting.
"""

import json

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
