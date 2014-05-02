#!/usr/bin/env python
"""
Do k-means clusting with map-reduce.
Adapted from disco/examples/datamining/kclustering.py
"""

import argparse
import os
import numpy as np
from disco.ddfs import DDFS
from disco.core import Job, result_iterator
from disco.util import kvgroup
from disco.func import chain_reader

def main(file_in="iris.csv", file_out="centers.csv", n_clusters=3):
    # TODO: Rename tag data:kcluster1 if tag exists.
    # Disco v0.4.4 requires that ./ prefix the file to idendify as local file.
    # http://disco.readthedocs.org/en/0.4.4/howto/chunk.html#chunking
    tag = "data:sort"
    DDFS().chunk(tag=tag, urls=['./'+file_in])
    try:
        # Import since slave nodes do not have same namespace as master
        from kcluster_map_reduce import KCluster
        job = KCluster().run(input=[tag], map_reader=chain_reader)
        with open(file_out, 'w') as f_out:
            writer = csv.writer(f_out, quoting=csv.QUOTE_NONNUMERIC)
            for center in result _iterator(job.wait(show=True)):
                writer.writerow([center])
    finally:
        DDFS().delete(tag=tag)
    return None

class KCluster(Job):

    # TODO: Fastest way forward is to update versions to most dev branch and use built-in map-reduce
    # Simplest, dumbest way.
    # One node per cluster
    # Assign a cluster center to first 3 data data points.
    # Compute distance of all points to each center.
    # Each point is assigned to cluster with minimum distance.
    # With new point assignments, each cluster center is recomputed.
    # new center - old center = diff
    # repeat distance calc.
    # repeat assignment
    # repeat center determination
    # repeat diff
    # if diff old - diff new / diff old > 0.05, repeat again

    def random_init_map(element, params):
        """Assign data element randomply to one of k clusters."""
        yield
    pass

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Do k-means clustering on file with map-reduce.")
    parser.add_argument("--file_in", default="iris.csv", help="Input file in CSV format. Default: iris.csv")
    parser.add_argument("--file_out", default="centers.csv", help="Output file. Default: centers.csv")
    parser.add_argument("--n_clusters", default=3, type=int, help="Number of clusters. Default: 3")
    args = parser.parse_args()
    print args
    if not os.path.isfile(args.file_in):
        create_iris_csv
    main(file_in=args.file_in, file_out=args.file_out, n_clusters=args.n_clusters)
