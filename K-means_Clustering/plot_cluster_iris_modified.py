#!/usr/bin/evn python
"""
K-means clustering example from:
http://scikit-learn.org/stable/auto_examples/cluster/plot_cluster_iris.html
"""

import numpy as np
import pylab as pl
from sklearn.cluster import KMeans
from sklearn import datasets

np.random.seed(5)

iris = datasets.load_iris()
X = iris.data
y = iris.target
init = 

est = KMeans(n_clusters=3)
print est.fit(X)
print est.labels_
print y

