# Welcome to arrowdantic

Arrowdantic is a small Python library backed by a Rust implementation of Apache Arrow
to read Parquet and Arrow IPC files to Python.

Its main differences vs pyarrow are:
* it is quite small (3Mb vs 90Mb)
* faster
* likely safer (no segfaults, core dumps, buffer overflows, etc.)
* it is type-hinted
* it has a much smaller subset of its functionality
  * basic arrays (integers, floats, boolean, string, binary)
  * read Apache Arrow IPC file
  * read Apache Parquet
