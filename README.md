This repository contains source code and data required to reproduce the findings presented in our USENIX 2025 paper entitled 
`Stack Overflow Meets Replication: Security Research Amid Evolving Code Snippets`

The repository structure mirrors the organization of the sections in the paper, making it easier to locate relevant source code and data.
Each section of the paper corresponds to a specific directory. Source files inside the `sources` parent directory contains code shared across multiple modules. 
Below is a description of how each directory is organized in relation to the sections of the paper.

### Section 3.1: Literature Search
This directory contains code and data needed to reproduce the findings presented in this Section.
All studies retrieved during our systematic literature search are stored in the `CrawlPapers` database table.
The `sources/literature_search` directory contains the source and data needed to reproduce the findings presented in this Section.
Specifically, the `relevant_studies.py` Python script (in the `sources/literature_search` directory) is used to reproduce the relevant studies presented in Table 1.


### Section 5: Evolution of Stack Overflow
The python scripts, Jupyter notebooks and data files in the `sources/evolution` directory are needed to reproduce the findings presented in Section 5 of our paper.

### Section 6: Replication Case Studies
This Section presents replicates four (out of six) case studies using a newer version of the Stack Overflow
dataset.
Each case study has its own dedicated folder, named `case_study_*`, where the specific source code and data needed to replicate 
that individual case study are stored. For example, the folder labeled `case_study_1` contains all the relevant files for 
replicating the findings of the study by Zhang et al. [87] presented in Section 6.1.
Similarly, `case_study_2` holds the materials required for replicating the findings from the DICOS study, as presented in Section 6.2 of the paper. 
Additional case study directories follow this naming convention.
Each case study directory has a `README.md` file that provides specific documentation, including instructions for running the code, 
a description of the datasets used, and any particular setup or configuration details required for replication. 

- `sql`: This directory contains a dump of the database tables containing the data needed for the reproduction. 
The tables were exported using the `mysqldump` command, as follows:

```bash
export packet_size="512M"
export sotorrent_db="sotorrent22"

mysqldump --max_allowed_packet=$packet_size -usotorrent -p --default-character-set=utf8mb4 $sotorrent_db CrawledPapers -r CrawledPapers.sql
```


###  Database Schema
The following shows the Database schema of the tables used in our replication study.
