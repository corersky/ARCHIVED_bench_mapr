#!/usr/bin/env python
"""
Do k-means clustering in a simplistic way.
See https://groups.google.com/forum/#!topic/disco-dev/u3EsnGgLOPM

Adapted from
disco/examples/datamining/kclustering.py

TODO:
- check ddfs tag exists
- check disco v0.4.4
"""

import argparse
import os
import numpy as np
import csv
from disco.ddfs import DDFS
from disco.core import Job, result_iterator
from disco.util import kvgroup
from disco.func import chain_reader

# Comment from kclustering_original.py
# HACK: The following dictionary will be transformed into a class once
# class support in Params has been added to Disco.
mean_point_center = {
    'create':(lambda x,w: { '_x':x, 'w':w }),
    'update':(lambda p,q: { '_x':[ pxi+qxi for pxi,qxi in zip(p['_x'],q['_x']) ], 'w':p['w']+q['w'] }),
    'finalize':(lambda p: { 'x':[v/p['w'] for v in p['_x']],'_x':p['_x'], 'w':p['w'] }),
    'dist':(lambda p,x: sum((pxi-xi)**2 for pxi,xi in zip(p['x'],x)) )
    }

def main(file_in="iris.csv", file_out="centers.csv", n_clusters=3):
    data = np.genfromtxt(file_in, delimiter=",")
    # TODO: Rename tag data:sort1 if tag exists.
    # Disco v0.4.4 requires that ./ prefix the file to identify as local file.
    # http://disco.readthedocs.org/en/0.4.4/howto/chunk.html#chunking
    tag = "data:kcluster"
    DDFS().chunk(tag=tag, urls=["./"+file_in])
    try:
        # Import since slave nodes do not have same namespace as master
        from kcluster_map_reduce import KCluster
        job = KCluster().run(input=[tag], map_reader=chain_reader)
        with open(file_out, 'w') as f_out:
            writer = csv.writer(f_out, quoting=csv.QUOTE_NONNUMERIC)
            for (id, center) in result_iterator(job.wait(show=True)):
                writer.writerow([id, center])
    finally:
        DDFS().delete(tag=tag)
    return None

class KCluster(Job):
    pass

def map_init(iter, params):
    """Intialize random number generator with given seed `params.seed`."""
    random.seed(params.seed)

def random_init_map(e, params):
    """Assign datapoint `e` randomly to one of the `k` clusters."""
    yield (random.randint(0,params.k-1), params.create(e[1],1.0))

def estimate_map(e, params):
    """Find the cluster `i` that is closest to the datapoint `e`."""
    yield (min((params.dist(c, e[1]), i) for i,c in params.centers)[1], params.create(e[1],1.0))

def estimate_combiner(i, c, centers, done, params):
    """Aggregate the datapoints in each cluster."""
    if done:
        return centers.iteritems()
    centers[i] = c if i not in centers else params.update(centers[i], c)

def estimate_reduce(iter, out, params):
    """Estimate the cluster centers for each cluster."""
    centers = {}
    for i, c in iter:
        centers[i] = c if i not in centers else params.update(centers[i], c)
    for i, c in centers.iteritems():
        out.add(i, params.finalize(c))

def predict_map(e, params):
    """Determine the closest cluster for the datapoint `e`."""
    yield (e[0], min((params.dist(c,e[1]), i) for i,c in params.centers))

def estimate(master, input, center, k, iterations, map_reader = chain_reader):
    """
    Optimize k-clustering for `iterations` iterations with cluster
    center definitions as given in `center`.
    """
    job = master.new_job(name = 'k-clustering_init',
                         input = input,
                         map_reader = map_reader,
                         map_init = map_init,
                         map = random_init_map,
                         combiner = estimate_combiner,
                         reduce = estimate_reduce,
                         params = Params(k = k, seed = None, **center),
                         nr_reduces = k)
    centers = [(i,c) for i,c in result_iterator(job.wait())]
    job.purge()
    for  j in range(iterations):
        job = master.new_job(name = 'k-clustering_iteration_%s' %(j,),
                             input = input,
                             map_reader = map_reader,
                             map = estimate_map,
                             combiner = estimate_combiner,
                             reduce = estimate_reduce,
                             params = Params(centers = centers, **center),
                             nr_reduces = k)
        centers = [(i,c) for i,c in result_iterator(job.wait())]
        job.purge()
    return centers


def predict(master, input, center, centers, map_reader = chain_reader):
    """
    Predict the closest clusters for the datapoints in input.
    """
    job = master.new_job(name = 'kcluster_predict',
                         input = input,
                         map_reader = map_reader,
                         map = predict_map,
                         params = Params(centers = centers, **center),
                         nr_reduces = 0)
    return job.wait()

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Do K-means clustering with map reduce.")
    parser.add_argument("--file_in", default="iris.csv", help="Input file. Default: iris.csv")
    parser.add_argument("--file_out", default="centers.csv", help="Output file. Default: centers.csv")
    parser.add_argument("--n_clusters", default=3, type=int, help="Number of clusters. Default: 3")
    args = parser.parse_args()
    print args
    if not os.path.isfile(args.file_in):
        print "INFO: {file_in} does not exist.".format(file_in=args.file_in)
        print "Creating iris.csv"
        crate_iris_csv
    main(file_in=args.file_in, file_out=args.file_out, n_clusters=args.n_clusters)
