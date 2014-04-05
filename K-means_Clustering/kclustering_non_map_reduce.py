#!/usr/bin/env python
"""
Do k-means clustering without map-reduce.
Adapted from http://scikit-learn.org/stable/auto_examples/cluster/plot_cluster_iris.html
"""

import argparse
import csv
import numpy as np
from sklearn.cluster import KMeans
from sklearn import datasets

def main(file_in="input.txt", file_out="output.csv"):
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
    parser.add_argument("--file_in", default="input.txt", help="Input file. Default: input.txt")
    parser.add_argument("--file_out", default="output.csv", help="Output file. Default: output.csv")
    args = parser.parse_args()
    print args

    main(file_in=args.file_in, file_out=args.file_out)
