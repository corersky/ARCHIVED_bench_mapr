#!/usr/bin/env python
"""
Create iris.csv dataset.
"""

import argparse
import numpy as np
from sklearn import datasets

def main(file_out="iris.csv"):
    iris = datasets.load_iris()
    np.savetxt(file_out, iris.data, delimiter=",")
    return None

if __name__ == '__main__':
    
    parser = argparse.ArgumentParser(description="Download iris dataset as iris.csv..")
    parser.add_argument("--file_out", default="iris.csv", help="Output file. Default: iris.csv")
    args = parser.parse_args()
    print args

    main(file_out=args.file_out)
