This folder contains the source for replicating Case Study 1: A study
of c/c++ code weaknesses on stack overflow. The following files are included:
- `data_collection.py`: Python script for collecting code snippets from a version of the SOTorrent dataset
and using the Guesslang machine learning classifier to determine the language of the collected code snippets.
The script uses the `Posts` and `PostBlockVersion` to collect the code snippets dataset.
It is implemented so that data can be collected from either the December 2018 version of the SOTorrent dataset or the June
2022 version. 
The latter is used to reproduce the numbers reported in the original paper and the former is
used for the replication study i.e., repeating the authors experiments on a newer dataset version (i.e., June 2022). 
The June 2022 SOTorrent version is our own release. 
The collected data is stored in the `CodeBlockVersionHaoxiangZhang` database table. A dump of this table is provided in the `sql` directory.
This table has a `DataSetReleaseDate` column that indicates the dataset version the data was collected from.
- `download_snippets.py`: Python script for downloading all code snippets containing at least five lines of code. The code snippets are collected
using the `get_code_snippets(int)`  function, which collects code snippets from sotorrent using the approach describe in the original study.
The code snippets are stored locally in the `zhang_dataset/snippets` directory. A code snippet is stored twice: one with a `.cpp` extension and another with a `.c` extension.
The reason is that since we removed the Guesslang classifier from the pipeline, we rely on the CppCheck static analysis tool to determine the language of the code snippets.
In the case that CppCheck determines a snippet with both extension to be valid, we mark the snippet with a `.c` extension as duplicate.
- `mark_duplicates.py`: Python script for marking duplicate code snippets. A code snippet, say snippet1.c and snippet1.cpp is a duplicate if CppCheck reported the same CWE for both snippets.
- `weakness_detection.py`: Python script for detecting CWEs in the code snippets. The script uses the CppCheck static analysis tool to detect CWEs in the code snippets.
The CWEs are stored in the `CppCheckWeakness` database table. A dump of this table is provided in the `sql` directory.
- `analysis.py`: Python script for analyzing the CWEs detected in the code snippets. The script uses the `CppCheckWeakness` database table to analyze the CWEs detected in the code snippets.