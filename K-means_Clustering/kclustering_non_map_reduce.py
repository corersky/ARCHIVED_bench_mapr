#!/usr/bin/env python
"""
kclustering.py

Do k-means clustering in a simplistic way.
See https://groups.google.com/forum/#!topic/disco-dev/u3EsnGgLOPM

TODO: update below
Example usage:
Assuming input.txt is a 5G file and you have 5 slave nodes:
$ split --line-bytes=1G input.txt"
$ ddfs push data:inputtxt ./xa?"
$ python sort.py data:inputtxt output.csv

Adapted from
disco/examples/datamining/kclustering.py
disco/examples/utils/count_words.py, wordcount.py, simple_innerjoin.py
http://disco.readthedocs.org/en/0.4.4/howto/dataflow.html

TODO:
- Add help text.
- Automatically determine partitions from config file and automatically chunk data
- Can't get sorted across nodes.
"""

import sys
import csv
from disco.core import Disco, Params, result_iterator
from disco.func import chain_reader
from optparse import OptionParser
from os import getenv


# Comment from kclustering_original.py
# HACK: The following dictionary will be transformed into a class once
# class support in Params has been added to Disco.
mean_point_center = {
    'create':(lambda x,w: { '_x':x, 'w':w }),
    'update':(lambda p,q: { '_x':[ pxi+qxi for pxi,qxi in zip(p['_x'],q['_x']) ], 'w':p['w']+q['w'] }),
    'finalize':(lambda p: { 'x':[v/p['w'] for v in p['_x']],'_x':p['_x'], 'w':p['w'] }),
    'dist':(lambda p,x: sum((pxi-xi)**2 for pxi,xi in zip(p['x'],x)) )
    }

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
                         params = Params(k = k, seed = None,
                                         **center),
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
                             params = Params(centers = centers,
                                             **center),
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
                         params = Params(centers = centers,
                                         **center),
                         nr_reduces = 0)

    return job.wait()


if __name__ == '__main__':
    parser = OptionParser(usage='%prog [options] inputs')
    parser.add_option('--disco-master',
                      default=getenv('DISCO_MASTER'),
                      help='Disco master')
    parser.add_option('--iterations',
                      default=10,
                      help='Numbers of iteration')
    parser.add_option('--clusters',
                      default=10,
                      help='Numbers of clusters')

    (options, input) = parser.parse_args()
    master = Disco(options.disco_master)

    centers = estimate(master, input, mean_point_center,
                       int(options.clusters), int(options.iterations))

    res = predict(master, input, mean_point_center, centers)

    print '\n'.join(res)

if __name__ == '__main__':
    
    if len(sys.argv) != 3:
        sys.stderr.write(
            """ERROR: Wrong number of arguments.
  Example usage:
  Assuming input.txt is a 5G file and you have 5 slave nodes:
  $ split --line-bytes=1G input.txt"
  $ ddfs push data:inputtxt ./xa?"
  $ python kclustering.py data:inputtxt output.csv
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
  $ python kclustering.py data:inputtxt output.csv
""")
        sys.exit(1)
    
    # Necesary to import since slave nodes do not have
    # same namespace as master.
    from kclustering import Kclustering
    job = Kclustering().run(input=[input_tag])

    with open(output_filename, 'w') as fp:
        writer = csv.writer(fp, quoting=csv.QUOTE_NONNUMERIC)
        for element, cluster_id in result_iterator(job.wait(show=True)):
            writer.writerow([element, cluster_id])

# OLD below

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
