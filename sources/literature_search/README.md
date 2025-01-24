### Instructions for Reproducing Section 3.1
To reproduce the studies presented in Table 1, run the `relevant_studies.py` Python script.
The authors already collected studies from multiple sources (), used GPT-4o to determine which studies should be included and stored all this
data in the `CrawledPapers` database table. If you're interested to re-create this table, then run the following source-specific Python script.
Each script will store the results into the database.
The structure of the xxx table is shown below:
```SQL
MariaDB [sotorrent22]> describe CrawledPapers;
+------------------------+------------------------------------------------------------------------+------+-----+---------+----------------+
| Field                  | Type                                                                   | Null | Key | Default | Extra          |
+------------------------+------------------------------------------------------------------------+------+-----+---------+----------------+
| Id                     | int(11)                                                                | NO   | PRI | NULL    | auto_increment |
| SemanticScholarPaperId | varchar(40)                                                            | YES  | MUL | NULL    |                |
| PaperTitle             | varchar(250)                                                           | YES  | MUL | NULL    |                |
| VenueName              | text                                                                   | YES  |     | NULL    |                |
| PublicationType        | enum('CONFERENCE','JOURNAL')                                           | YES  |     | NULL    |                |
| URL                    | text                                                                   | YES  |     | NULL    |                |
| PaperSource            | enum('DBPL','SOTorrentRef','StackExchangeSite','ACM DL','IEEE Xplore') | YES  |     | NULL    |                |
| PaperAbstract          | text                                                                   | NO   |     | NULL    |                |
| GPT4oLabel             | tinyint(1)                                                             | YES  |     | NULL    |                |
| Justification          | text                                                                   | YES  |     | NULL    |                |
+------------------------+------------------------------------------------------------------------+------+-----+---------+----------------+
```

This source folder contains all the source required to reproduce the findings describe in **Section 3.1** of our paper.
The following directories are included:

- `dblp`: This directory contains the `dblp_explore.py` python script used to search the conference proceedings and journals of the venues listed in Table 10 in the Appendix.
The retrieved studies are stored in the `CrawledPapers` database table. A dump of this table is provided in the `sql` directory and already contains the studies retrieved from the venues listed in Table 10.
- `acm_dl`: This directory contains the `acm_explore.py` python script used to process the studies downloaded from the ACM DL as a set of bibtex files.
The downloaded bibtex files are stored in the `bibtex_files`directory. The processed studies are stored in the `CrawledPapers` database table. A dump of this table is provided in the `sql` directory
and already contains the studies retrieved from the ACM DL.
- `ieee`: This directory contains the `ieee_explore.py` python script used to process the studies downloaded from the IEEE Xplore as a set of CSV files and 
the `csv_files` directory containing the studies downloaded from IEEE Xplore.
- `gpt4o_evaluation.py`: This python script evaluates the performance of the GPT-4-O model on the studies in the `abstracts` directory. The results of the evaluation is
shown in Appendix Table 11.
The processed studies are stored in the `CrawledPapers` database table and already contains the studies retrieved from IEEE Xplore.
- `semantic_scholar.py`: Contains code that makes calls to Semantic Scholar API.
- `relevant_studies.py`: Contains code that retrieves all the studies from the `CrawledPapers` database table that are relevant to the study. 
The relevant studies are shown in systematization listed in Table 1.