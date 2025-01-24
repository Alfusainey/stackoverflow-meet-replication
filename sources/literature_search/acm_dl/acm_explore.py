import os
import re
from typing import Dict, List

import pandas as pd

from sources.literature_search.semantic_scholar import get_paper_info
from sources.literature_search.util import classify_paper, get_api_key, get_logger, \
    get_db_engine, is_none_or_empty, title_too_long
from sources.util import get_db_user, get_db_password


def keep_count_of_proceedings_and_journals(entry_type: str):
    if entry_type in ['proceedings', 'inproceedings']:
        proceedings.append(entry_type)
    else:
        journals.append(entry_type)


def write_to_file(entry: str):
    """
    Write a BibTeX entry to a file for manual inspection.
    """
    with open(bibtex_errors_file_path, 'a') as f:
        f.write(entry + '\n\n')


def parse_bibtex_entry(bibtex_entry: str) -> Dict[str, str]:
    """
    Parse a BibTeX entry into a Python dictionary.

    If there is an error parsing the entry, an empty dictionary is returned and save the entry into a file.
    """
    bibtex_dict = {}
    if is_none_or_empty(bibtex_entry):
        return bibtex_dict

    entry_lines = bibtex_entry.strip().split('\n')

    # Extract the entry type and citation key
    try:
        entry_type, citation_key = re.match(r'@(\w+)\{([^,]+),', entry_lines[0]).groups()
        keep_count_of_proceedings_and_journals(entry_type)
    except Exception as e:
        logger.error(f"Error parsing entry: {entry_lines[0]}")
        write_to_file(bibtex_entry)
        return bibtex_dict

    if citation_key in citation_key_cache:
        # ignore duplicate entries
        logger.info(f"DocumentExist: {citation_key}.")
        return {}

    bibtex_dict['entry_type'] = entry_type
    bibtex_dict['citation_key'] = citation_key
    citation_key_cache.add(citation_key)

    # Extract the fields
    for line in entry_lines[1:]:
        if '=' in line:
            key, value = line.split('=', 1)
            key = key.strip()
            value = value.replace('{', '').replace('}', '').strip().strip(',')
            bibtex_dict[key] = value
    return bibtex_dict


def read_bibtex_file(file_path: str) -> List[Dict[str, str]]:
    """
    Read multiple BibTeX entries from a file and parse each entry.
    """
    with open(file_path, 'r') as file:
        content = file.read()

    # Split the content into individual entries
    entries = content.split("\n\n") # re.split(r'@', content, maxsplit=1)
    parsed_entries = []

    for entry in entries:  # Skip the first split part as it will be empty
        # entry1 = '@' + entry.strip()  # Add the '@' back to the entry
        parsed_entry = parse_bibtex_entry(entry)
        if parsed_entry:
            parsed_entries.append(parsed_entry)

    return parsed_entries


def get_abstract(entry: Dict[str, str]) -> tuple:
    if 'abstract' in entry:
        return None, entry['abstract']

    # retrieve from semantic scholar
    # if 404 then set to empty string and log info
    logger.info(f"SemanticScholar: Retrieving abstract for {entry.get('title')} from Semantic Scholar...")
    paper_info = get_paper_info(entry.get('title'), logger)
    abstract = paper_info.get('abstract')
    semantic_scholar_paper_id = paper_info.get('SemanticScholarPaperId')
    return semantic_scholar_paper_id, abstract


def classify_entry(entry: Dict[str, str]) -> Dict[str, str]:
    semantic_scholar_paper_id, abstract = get_abstract(entry)
    if is_none_or_empty(abstract):
        logger.info(f"NoAbstract: {entry.get('title')}")
        return {}

    if title_too_long(entry.get('title')):
        logger.info(f"TitleTooLong: {entry.get('title')}")
        return {}

    venue_name = None
    publication_type = None
    if 'journal' in entry:
        publication_type = 'journal'.upper()
        venue_name = entry.get('journal')
    elif 'booktitle' in entry:
        publication_type = 'conference'.upper()
        venue_name = entry.get('booktitle')

    is_relevant, justification = classify_paper(get_api_key(), abstract)
    return {
        'SemanticScholarPaperId': semantic_scholar_paper_id,  # None if abstract is present in bibtex
        'PaperTitle': entry.get('title'),
        'VenueName': venue_name,
        'PublicationType': publication_type,
        'URL': entry.get('url', None),
        'PaperSource': 'ACM DL',
        'PaperAbstract': abstract,
        'GPT4oLabel': is_relevant,
        'Justification': justification,
    }


def main():
    bibtex_files_path = 'sources/literature_search/acm_dl/bibtex_files'
    remote_db = "sotorrent22"
    local_db = "TestDatabase"
    engine = get_db_engine(get_db_user(), get_db_password(), remote_db)

    bibtex_files = os.listdir(bibtex_files_path)
    for bibtex_file in bibtex_files:
        file_path = os.path.join(bibtex_files_path, bibtex_file)
        parsed_entries = read_bibtex_file(file_path)

        acm_papers = []
        for entry in parsed_entries:
            classified_entry = classify_entry(entry)
            if classified_entry:
                acm_papers.append(classified_entry)
        if len(acm_papers) == 0:
            logger.info(f"SKIPPED: No relevant publications found for '{file_path}'")
            continue
        try:
            df = pd.DataFrame(acm_papers)
            df.to_sql(name='CrawledPapers', con=engine, if_exists='append', index=False)
            logger.info(f"SUCCESS: Inserted {len(acm_papers)} papers into database from {bibtex_file}.")
        except Exception as e:
            logger.error(f"Error inserting data into database: {e}")
    logger.info(
        f"COMPLETED ACM DL: TotalFiles: {len(bibtex_files)}, Proceedings: {len(proceedings)}, Journals: {len(journals)})")


def test_parse_entry():
    bibtex_entry = """
    @inproceedings{10.1109/ICSE48619.2023.00158,
    author = {Kou, Bonan and Chen, Muhao and Zhang, Tianyi},
    title = {Automated Summarization of Stack Overflow Posts},
    year = {2023},
    isbn = {9781665457019},
    publisher = {IEEE Press},
    url = {https://doi.org/10.1109/ICSE48619.2023.00158},
    doi = {10.1109/ICSE48619.2023.00158},
    abstract = {Software developers often resort to Stack Overflow (SO) to fill their programming needs. Given the abundance of relevant posts, navigating them and comparing different solutions is tedious and time-consuming. Recent work has proposed to automatically summarize SO posts to concise text to facilitate the navigation of SO posts. However, these techniques rely only on information retrieval methods or heuristics for text summarization, which is insufficient to handle the ambiguity and sophistication of natural language.This paper presents a deep learning based framework called Assort for SO post summarization. Assort includes two complementary learning methods, AssortS and AssortIS, to address the lack of labeled training data for SO post summarization. AssortS is designed to directly train a novel ensemble learning model with BERT embeddings and domain-specific features to account for the unique characteristics of SO posts. By contrast, AssortIS is designed to reuse pre-trained models while addressing the domain shift challenge when no training data is present (i.e., zero-shot learning). Both AssortS and AssortIS outperform six existing techniques by at least 13\% and 7\% respectively in terms of the F1 score. Furthermore, a human study shows that participants significantly preferred summaries generated by AssortS and AssortIS over the best baseline, while the preference difference between AssortS and AssortIS was small.},
    booktitle = {Proceedings of the 45th International Conference on Software Engineering},
    pages = {1853–1865},
    numpages = {13},
    keywords = {stack overflow, text summarization, deep learning},
    location = {Melbourne, Victoria, Australia},
    series = {ICSE '23'}
    }
    """

    parsed_entry = parse_bibtex_entry(bibtex_entry)
    print(parsed_entry)


if __name__ == "__main__":
    # store the citation key of all bibtext entries in a cache
    # only entries whose citation keys are not in the cache will be processed
    citation_key_cache = set()
    proceedings = []
    journals = []
    logger = get_logger("acm_dl.log")
    bibtex_errors_file_path = 'case_studies/src/literature_search/acm_dl/bibtex_errors.txt'
    main()
