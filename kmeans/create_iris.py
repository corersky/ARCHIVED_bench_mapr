#!/usr/bin/env python
"""
Create iris.csv dataset.
"""

import argparse
import numpy as np
from sklearn import datasets

def main(file_out):
    """
    Download and save iris data set.
    """

    iris = datasets.load_iris()
    np.savetxt(file_out, iris.data, delimiter=",")
    return None

if __name__ == '__main__':
    
    file_out_default="iris.csv"

    parser = argparse.ArgumentParser(description="Download iris dataset as iris.csv..")
    parser.add_argument("--file_out",
                        default=file_out_default,
                        help="Output file. Default: {default}".format(default=file_out_default))
    args = parser.parse_args()
    print args

    main(file_out=args.file_out)
