#!/usr/bin/env python
"""
Do k-means clustering without map-reduce.
Adapted from http://scikit-learn.org/stable/auto_examples/cluster/plot_cluster_iris.html
"""

import argparse
import os
import numpy as np
from sklearn.cluster import KMeans

def main(file_in="iris.csv", file_out="centers.csv", n_clusters=3):
    #TODO: Rexume here
    iris = datasets.load_iris()
    X = iris.data
    est = KMeans(n_clusters=3)
    print est
    est.fit(X)
    centers = est.cluster_centers_
    print centers
    np.savetxt("output.csv", centers, delimiter=",")
    return None

if __name__ == '__main__':
    
    parser = argparse.ArgumentParser(description="Do k-means clustering on file without map-reduce.")
    parser.add_argument("--file_in", default="iris.csv", help="Input file. Default: iris.csv")
    parser.add_argument("--file_out", default="cluster_centers.csv", help="Output file. Default: cluster_centers.csv")
    parser.add_argument("--n_clusters", default=3, type=int, help="Number of clusters. Default: 3")
    args = parser.parse_args()
    print args

    if not os.path.isfile(args.file_in):
        create_iris_csv

    main(file_in=args.file_in, file_out=args.file_out, n_clusters=args.n_clusters)
