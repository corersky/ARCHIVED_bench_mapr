#!/usr/bin/env python
"""
Module for MRJob.
Following https://pythonhosted.org/mrjob/guides/quickstart.html
"""

from mrjob.job import MRJob

class MRWordcount(MRJob):

    def mapper(self, _, line):
        """
        Read in line. Parse line into list of words.
        Yield words with a single count.
        """
        words = line.split()
        for word in words:
            yield (word, 1)

    def combiner(self, word, counts):
        """
        For each word, aggregate over the counts.
        """
        yield (word, sum(counts))

    def reducer(self, word, counts):
        """
        For each word, aggregate over the counts.
        """
        yield (word, sum(counts))

if __name__ == '__main__':
    MRWordcount.run()
