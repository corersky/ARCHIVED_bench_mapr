#!/usr/bin/env python
"""
sort.py

Sort a .txt file. Ouput .csv.

Example usage:
Assuming input.txt is a 5G file and you have 5 slave nodes:
$ split --line-bytes=1G input.txt"
$ ddfs push data:inputtxt ./xa?"
$ python sort.py data:inputtxt output.csv

Adapted from
disco/tests/test_sort.py, test_pipeline_sort.py
disco/examples/utils/count_words.py, wordcount.py, simple_innerjoin.py
http://disco.readthedocs.org/en/0.4.4/howto/dataflow.html

TODO:
- Add help text.
- Automatically determine partitions from config file and automatically chunk data
- Can't get sorted across nodes.
"""

import sys
import csv
from disco.ddfs import DDFS
from disco.core import Job, result_iterator
from disco.util import kvgroup, shuffled
from disco.compat import bytes_to_str, str_to_bytes

class Sort(Job):
    # 5 partitions for 5 slave nodes: scout02-06
    partitions = 5
    merge_partitions = True
    sort = True

    def map(self, string, params):
        bytestring = base64.encodestring(str_to_bytes(string))
        bytevalue = b''
        yield shuffled((bytestring, bytevalue))
    
    def reduce(self, rows_iter, out, params):
        for bytestring, bytevalue in kvgroup(rows_iter):
            string = bytes_to_str(base64.decodestring(bytestring))
            count = len(list(bytevalue))
            out.add(string, count)

if __name__ == '__main__':
    
    if len(sys.argv) != 3:
        sys.stderr.write(
            """ERROR: Wrong number of arguments.
  Example usage:
  Assuming input.txt is a 5G file and you have 5 slave nodes:
  $ split --line-bytes=1G input.txt"
  $ ddfs push data:inputtxt ./xa?"
  $ python sort.py data:inputtxt output.csv
""")
        sys.exit(1)

    input_tag = sys.argv[1]
    output_filename = sys.argv[2]
    if not DDFS().exists(input_tag):
        sys.stderr.write(
            "ERROR: " + input_tag + """ is not a tag in Disco Distributed File System.
  Example usage:
  Assuming input.txt is a 5G file and you have 5 slave nodes:
  $ split --line-bytes=1G input.txt"
  $ ddfs push data:inputtxt ./xa?"
  $ python sort.py data:inputtxt output.csv
""")
        sys.exit(1)
    
    # Necesary to import since slave nodes do not have
    # same namespace as master.
    from sort import Sort
    job = Sort().run(input=[input_tag])
    
    with open(output_filename, 'w') as fp:
        writer = csv.writer(fp, quoting=csv.QUOTE_NONNUMERIC)
        for string, count in result_iterator(job.wait(show=True)):
            writer.writerow([string, count])
