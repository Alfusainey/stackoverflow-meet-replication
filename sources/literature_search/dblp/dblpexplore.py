"""
Copied and adapted from <todo> to search for papers on DBLP.
"""
import time
from typing import List, Dict

from bs4 import BeautifulSoup as bs
import pandas as pd
from datetime import datetime
import argparse


from case_studies.src.literature_search.semantic_scholar import get_paper_info
from case_studies.src.literature_search.util import classify_paper, get_api_key, get_db_engine, get_search_keywords, \
    get_venues, get_db_password, get_db_user, crawl, get_logger
from main.util import get_colossus04_ip


def get_paper_titles(proceedings_url: str, venue_type: str) -> List[str]:
    """Extract paper titles and authors from the given URL.
    The function returns a pandas DataFrame with the following columns:
    > PaperId, Title, Authors, Venue, Year, Source, Abstract
    """
    response = crawl(proceedings_url)
    if not response:
        logger.info(f"FAILED: Unable to extract data from {proceedings_url}")
        return []

    soup = bs(response.text, 'html.parser')
    publ_list = soup.find_all("cite", {"class": "data tts-content", "itemprop": "headline"})
    logger.info(f"SUCCESS: Extracted {len(publ_list)} publication(s) at {proceedings_url}")

    first_row = True
    titles = []
    for publ in publ_list:
        try:
            if first_row and venue_type == 'conference':
                # Exclude the first row as it is the proceeding's name
                first_row = False
                continue
            authors = ', '.join(a.find("span", {"itemprop": "name"}).string for a in publ.find_all("span", {"itemprop": "author"}))
            if authors.strip() == '' or authors.strip() is None:
                # this is not a paper, skip
                continue
            title = publ.find("span", {"class": "title", "itemprop": "name"}).string
            if title:
                titles.append(title)
        except:
            pass

    return titles


def get_papers_info(titles: List[str]) -> List[Dict]:
    """
    Use the Semantic Scholar API to get details (i.e., title,venue,abstract,openAccessPdf) of the papers with the given titles.
    """
    results = []
    for title in titles:
        paper_info = get_paper_info(title, logger)
        if paper_info["PaperAbstract"]:
            # can happen if paper is an editorial or similar e.g., 'Editorial: Toward the Future with Eight Issues Per Year.'
            results.append(paper_info)
    return results


def apply_keyword_gpt4o_filter(papers_info: List[Dict], keywords: List[str], venue_key: tuple, source_ref='DBPL') -> List[Dict]:
    """Filters the given set of papers based on the provided keywords and
       classifies whether a paper is relevant using the OpenAI API.
    """
    if not keywords:
        return papers_info  # Return all if no keywords are provided

    # result = pd.DataFrame(columns=["title", "authors", "conf_id", "abstract"])
    lower_keywords = [keyword.lower() for keyword in keywords]

    results = []
    reset_count = 1
    for paper_info in papers_info:
        try:
            paper_title = paper_info["PaperTitle"].lower()
            paper_abstract = paper_info["PaperAbstract"].lower()
            if any(keyword in paper_title or keyword in paper_abstract for keyword in lower_keywords):
                # for those containing our keywords, classify them
                if reset_count == 9_999:
                    # the x-ratelimit-reset-requests is 6ms, so we wait for 1 second
                    time.sleep(1)
                    reset_count = 1
                is_relevant, justification = classify_paper(get_api_key(), paper_info["PaperAbstract"])
                paper_info["GPT4oLabel"] = is_relevant
                paper_info["Justification"] = justification
                paper_info["VenueName"] = venues.get(venue_key)
                paper_info["PublicationType"] = venue_key[1]
                paper_info["PaperSource"] = source_ref
                results.append(paper_info)
                reset_count += 1
        except Exception as e:
            logger.warning(f"OpenAIError: Failed to classify paper abstract due to: {repr(e)}")
            pass
            continue
    return results


def list_journal_index(soup: bs) -> List[Dict]:
    index_listings = []
    list_items = soup.find_all('a', href=lambda href: href and href.startswith('https://dblp.org/db/journals/') and href.endswith('.html'))
    for list_item in list_items:
        volume_url = list_item.get('href')
        year = list_item.text.rsplit(' ', 1)[-1]
        try:
            year = int(year)
        except ValueError:
            continue
        html_file = volume_url.rsplit('/', 1)[-1]
        config_id = html_file.replace('.html', '')
        index_listings.append({"link": volume_url, "date_published": year, "conf_id": config_id})
    return index_listings


def list_conference_index(soup: bs) -> List[Dict]:
    proceedings = soup.find_all("cite", {"class": "data tts-content", "itemprop": "headline"})
    index_listings = []
    for pcd in proceedings:
        proceedings_url = pcd.find("a", {"class": "toc-link"}).get('href')
        date_published = int(pcd.find("span", {"itemprop": "datePublished"}).string)
        conf_id = proceedings_url.rsplit('/', 1)[-1].rsplit('.', 1)[0]
        index_listings.append({"link": proceedings_url, "date_published": date_published, "conf_id": conf_id})
    return index_listings


def list_index(name: str, venue_type) -> List[Dict]:
    """List the index webpage of the specified journal or conference proceedings.

    This function lists all proceeedings and volumes for the specified conference or journal.
    The listing will be filtered at a later step.
    """
    CONF_BASE = "https://dblp.org/db/conf/"
    JOURNAL_BASE = "https://dblp.org/db/journals/"

    url = CONF_BASE + name + "/index.html" if venue_type == "conference" else JOURNAL_BASE + name + "/index.html"
    response = crawl(url)

    if not response:
        logger.info(f"FAILED: Unable to extract data from {url}")
        return []

    soup = bs(response.text, 'html.parser')
    if venue_type == "conference":
        return list_conference_index(soup)
    else:
        return list_journal_index(soup)


def get_dblp_indexed_papers(venue_key: tuple) -> List[Dict]:
    dbpl_acronym = venue_key[0]
    venue_type = venue_key[1]
    results = []
    index_listing = list_index(dbpl_acronym, venue_type)
    if len(index_listing) == 0:
        logger.info(f"Cannot find data for {dbpl_acronym}")
        return results

    df = pd.DataFrame(index_listing)
    df = df[(df['date_published'] >= start) & (df['date_published'] <= end)]
    logger.info(f"SUCCESS: Extracted {len(df)} proceedings during [{start}, {end}] of {dbpl_acronym}")

    for _, pcd in df.iterrows():
        paper_titles = get_paper_titles(pcd["link"], venue_type)
        # use semantic scholar to retrieve paper details (abstracts, pdflinks, etc.)
        papers_info = get_papers_info(paper_titles)
        # filter papers based on keywords and classify them using GPT-4o
        papers_info = apply_keyword_gpt4o_filter(papers_info, search_keywords, venue_key, source_ref='DBPL')
        results.extend(papers_info)
    return results


def main():
    logger.info(f"START: Proceesing {len(venues.keys())} conferences and all their proceedings during [{start}, {end}]")
    engine = get_db_engine(get_db_user(), get_db_password(), "sotorrent22")
    for key, venue_name in venues.items():
        dbpl_acronym = key[0]
        dest = f"case_studies/src/literature_search/dblp_papers/{dbpl_acronym}.tsv"
        indexed_papers = get_dblp_indexed_papers(key)
        if len(indexed_papers) == 0:
            logger.info(f"SKIPPED: No relevant publications found for '{dbpl_acronym}' during [{start}, {end}]")
            continue
        try:
            df = pd.DataFrame(indexed_papers)
            df.to_sql(name='CrawledPapers', con=engine, if_exists='append', index=False)
            df.to_csv(dest, sep='\t', mode='a', header=False, index=False)
            logger.info(f"SUCCESS: Saved {len(df)} relevant publication(s) from '{dbpl_acronym}' to '{dest}'")
        except:
            logger.info(f"FAILED: Unable to save publications from '{dbpl_acronym}' to '{dest}'.")
        logger.info(f"COMPLETED: Completed extracting publications from '{dbpl_acronym}' during [{start}, {end}], with keywords '{search_keywords}', to '{dest}'.")
    logger.info(f"END: Completed processing {len(venues.keys())} conferences and all their proceedings during [{start}, {end}]")


def test_crawl_dbpl():
    DB_NAME = "sotorrent22"
    TEST_DB_NAME = "TestDatabase"
    remote_engine = get_db_engine(get_db_user(), get_db_password(), DB_NAME, db_host=get_colossus04_ip())
    local_engine = get_db_engine(get_db_user(), get_db_password(), TEST_DB_NAME)
    for (journal_name, venue_type) in [('tosem', 'journal'), ('acsac', 'conference')]:
        index_listing = list_index(journal_name, venue_type)
        assert len(index_listing) > 0
        titles = get_paper_titles(index_listing[0]['link'], venue_type)
        assert len(titles) > 0
        papers_info = get_papers_info(titles[:5])
        papers_info = apply_keyword_gpt4o_filter(papers_info, search_keywords, (journal_name, venue_type))
        if len(papers_info) > 0:
            df = pd.DataFrame(papers_info)
            df.to_sql(name='CrawledPapers', con=local_engine, if_exists='append', index=False)


def test_crawl():
    for key, venue_name in venues.items():
        list_index(key[0], key[1])


if __name__ == '__main__':
    venues = get_venues()
    start = 2005
    end = 2023
    logger = get_logger()
    search_keywords = get_search_keywords()
    main()
