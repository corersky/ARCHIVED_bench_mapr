#!/usr/bin/env python
"""
Do k-means clustering without map-reduce
from a .csv file and output centers to a .csv
as a check for map-reduce implentation.
Adapted from
http://scikit-learn.org/stable/
auto_examples/cluster/plot_cluster_iris.html
"""

from __future__ import print_function
import argparse
import os
import numpy as np
from sklearn.cluster import KMeans

def main(file_in, file_out, n_clusters):
    """
    Read in csv, compute centers, output centers to csv.
    """
    data = np.genfromtxt(file_in, delimiter=",")

    estimator = KMeans(n_clusters=n_clusters)
    print(estimator)
    estimator.fit(data)
    centers = estimator.cluster_centers_

    np.savetxt(file_out, centers, delimiter=",")

    return None

if __name__ == '__main__':

    file_in_default="iris.csv"
    file_out_default="centers.csv"
    n_clusters_default=3

    parser = argparse.ArgumentParser(description="Do k-means clustering on file without map-reduce.")
    parser.add_argument("--file_in",
                        default=file_in_default,
                        help="Input file in csv format. "
                        +"Default: {default}".format(default=file_in_default))
    parser.add_argument("--file_out",
                        default=file_out_default,
                        help="Output file with cluster centers. "
                        +"Default: {default}".format(default=file_out_default))
    parser.add_argument("--n_clusters",
                        default=n_clusters_default,
                        type=int,
                        help="Number of cluster centers to find. "
                        +"Default: {default}".format(default=n_clusters_default))
    args = parser.parse_args()
    print(args)

    main(file_in=args.file_in, file_out=args.file_out, n_clusters=args.n_clusters)
