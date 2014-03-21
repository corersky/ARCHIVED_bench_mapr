#!/usr/bin/env python
"""
sort.py

Sort a .txt file. Ouput .txt.

TODO: Example usage

Adapted from
disco/tests/test_sort.py, test_pipeline_sort.py
disco/examples/utils/count_words.py, wordcount.py, simple_innerjoin.py

TODO: Add help text.
"""

import sys
import base64
import string
from disco.ddfs import DDFS
from disco.core import Job, result_iterator
from disco.test import TestCase, TestJob
from disco.util import kvgroup, shuffled
from disco.compat import bytes_to_str, str_to_bytes

# alphanum = list(string.ascii_letters) + list(map(str, range(10)))

class SortJob(TestJob):
    sort = True

    @staticmethod
    def map(string, params):
        return shuffled((base64.encodestring(str_to_bytes(c)), b'') for c in bytes_to_str(string * 10))
    
    @staticmethod
    def reduce(iter, params):
        for k, vs in kvgroup(iter):
            yield bytes_to_str(base64.decodestring(k)), len(list(vs))

# class SortTestCase(TestCase):
#     def serve(self, path):
#         return ''.join(alphanum)

#     def runTest(self):
#         self.job = SortJob().run(input=self.test_server.urls([''] * 100))
#         self.assertResults(self.job, sorted((c, 1000) for c in alphanum))

if __name__ == '__main__':
    
    # TODO: allow running without arguments
    if len(sys.argv) != 3:
        sys.stderr.write(
            """ERROR: Wrong number of arguments.
  Example usage:
  Assuming input.txt is a 5G file and you have 5 slave nodes:
  $ split --line-bytes=1G input.txt"
  $ ddfs push data:inputtxt ./xa?"
  $ python sort.py data:inputtxt ouput.txt
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
  $ python sort.py data:inputtxt ouput.txt
""")
        sys.exit(1)
    
    # Necesary to import since slave nodes do not have
    # same namespace as master.
    from sort import SortJob
    job = SortJob().run(input=[input_tag])
    
    with open(output_filename, 'w') as fp:
        for word in result_iterator(job.wait(show=True)):
            fp.write(word)
