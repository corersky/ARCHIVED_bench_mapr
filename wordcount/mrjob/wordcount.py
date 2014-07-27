#!/usr/bin/env python
"""
Module for MRJob.
Following https://pythonhosted.org/mrjob/guides/quickstart.html
"""

from mrjob.job import MRJob

class MRWordFrequencyCount(MRJob):

    def mapper(self, _, line):
        yield "chars", len(line)
        yield 
