#!/usr/bin/env python
"""
sort_non-map-reduce.py

Sort a file to check the map-reduce implentation.

"""

import argparse

def main():
    pass
    
if __name__ == '__main__':

    parser = argparse.ArgumentParser(description="Sort a file without map-reduce.")
    parser.add_argument("fin", default="input.txt", help="input file (default: input.txt)")
    parser.add_argument("fout", default="output.csv", help="output file (default: output.csv)")
    args = parser.parse_args()
    print args

