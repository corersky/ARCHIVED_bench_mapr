# bench_mapr

Benchmark Python mapreduce implementations following [A Guide to Python Frameworks](http://blog.cloudera.com/blog/2013/01/a-guide-to-python-frameworks-for-hadoop/) and [HiBench Hadoop benchmark suite](https://github.com/intel-hadoop/HiBench).

## Current status

As of 20140814T030000Z:

Only wordcount and sort have been implemented.
Only Disco and Hadoop streaming Python examples have been completed.

This repository is follow-up to posting on Disco user group:
https://groups.google.com/forum/#!topic/disco-dev/u3EsnGgLOPM

Versions used:
- Python v2.7 from the ContinuumIO Anaconda Python distribution
- Disco v0.4.4 from the ContinuumIO Anaconda Python distribution
- Hadoop v2.3.0-cdh5.0.3 from the Cloudera distribution
