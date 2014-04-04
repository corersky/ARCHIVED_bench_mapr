#!/usr/bin/env python
"""
Do k-means clustering without map-reduce.
Adapted from http://scikit-learn.org/stable/auto_examples/cluster/plot_cluster_iris.html
"""

import argparse
import csv
import numpy as np
import pylab as pl
from sklearn.cluster import KMeans
from sklearn import datasets

def main(file_in="input.txt", file_out="output.csv"):
    np.random.seed(5)
    iris = datasets.load_iris()
    X = iris.data
    y = iris.target
    init = "todo"
    est = KMeans(n_clusters=3)
    print est.fit(X)
    print est.labels_
    print y
    return None

if __name__ == '__main__':
    
    parser = argparse.ArgumentParser(description="Do k-means clustering from a file without map-reduce.")
    parser.add_argument("--file_in", default="input.txt", help="Input file. Default: input.txt")
    parser.add_argument("--file_out", default="output.csv", help="Output file. Default: output.csv")
    args = parser.parse_args()
    print args

    main(file_in=args.file_in, file_out=args.file_out)
