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
    data = np.genfromtxt(file_in, delimiter=",")
    estimator = KMeans(n_clusters=n_clusters)
    print estimator
    estimator.fit(data)
    centers = estimator.cluster_centers_
    np.savetxt(file_out, centers, delimiter=",")
    return None

if __name__ == '__main__':
    
    parser = argparse.ArgumentParser(description="Do k-means clustering on file without map-reduce.")
    parser.add_argument("--file_in", default="iris.csv", help="Input file. Default: iris.csv")
    parser.add_argument("--file_out", default="centers.csv", help="Output file. Default: centers.csv")
    parser.add_argument("--n_clusters", default=3, type=int, help="Number of clusters. Default: 3")
    args = parser.parse_args()
    print args

    if not os.path.isfile(args.file_in):
        create_iris_csv

    main(file_in=args.file_in, file_out=args.file_out, n_clusters=args.n_clusters)
