"""
All functions for accessing the Semantic Scholar API are in this source file.
"""
import time
from typing import List, Dict, Set

import pandas as pd
import requests
import logging

from case_studies.src.literature_search.util import get_venue_type, get_venue_name


def get_paper_batch(paper_ids: List, source_ref: str) -> List[Dict]:
    """
    See https://api.semanticscholar.org/api-docs/graph#tag/Paper-Data/operation/post_graph_get_papers for more details
    """
    batch_url = "https://api.semanticscholar.org/graph/v1/paper/batch"
    headers = {
        "Content-Type": "application/json"
    }
    params = {
        "fields": "title,venue,abstract,openAccessPdf"
    }
    max_retries = 10
    retry_delay = 1  # initial delay in seconds

    paper_infos = []
    for attempt in range(max_retries):
        response = requests.post(batch_url, headers=headers, json={"ids": paper_ids}, params=params)
        if response.status_code == 200:
            paper_data = response.json()  # paper_data is a list of dictionaries
            for paper_dict in paper_data:
                open_access_pdf = paper_dict.get('openAccessPdf', {})
                citation_info = {
                    "PaperId": paper_dict.get('paperId'),
                    "Title": paper_dict.get('title'),
                    "Venue": paper_dict.get('venue'),
                    "PdfLink": open_access_pdf.get('url', None) if open_access_pdf else None,
                    "Source": source_ref,
                    "Abstract": paper_dict.get('abstract')
                }
                paper_infos.append(citation_info)
            break
        elif response.status_code == 429:
            logging.info(f"get_paper_batch: Rate limit exceeded. Retrying in {retry_delay} seconds.")
            time.sleep(retry_delay)
            retry_delay *= 2  # exponential backoff

    return paper_infos


def get_paper_info(title: str, logger) -> Dict:
    url = "https://api.semanticscholar.org/graph/v1/paper/search/match"

    params = {
        "query": f"{title}",
        "fields": "abstract,openAccessPdf"
    }
    max_retries = 10
    retry_delay = 1  # initial delay in seconds
    status_code = 0
    for _ in range(max_retries):
        response = requests.get(url, params=params)
        status_code = response.status_code
        if status_code == 200:
            data = response.json()
            data = data.get('data', [])
            if data:
                paper_dict = data[0]
                open_access_pdf = paper_dict.get('openAccessPdf', {})
                return {
                    "SemanticScholarPaperId": paper_dict.get('paperId'),
                    "PaperTitle": title,
                    "URL": open_access_pdf.get('url', None) if open_access_pdf else None,
                    "PaperAbstract": paper_dict.get('abstract')
                }
        elif status_code == 429:
            logger.info(f"Rate limit exceeded. Retrying in {retry_delay} seconds.")
            time.sleep(retry_delay)
            retry_delay *= 2  # exponential backoff

    # semantic scholar does not have the paper or the API call failed after 15 attempts
    logger.info(f"NoAbstract: StatusCode: {status_code}, Title: {title}")
    return {
        'Title': title,
        'PaperAbstract': None
    }


def get_paper_id(paper_title):
    # API endpoint for paper title search (https://academia.stackexchange.com/a/215136)
    url = "https://api.semanticscholar.org/graph/v1/paper/search/match"

    params = {
        "query": f"{paper_title}",
        "fields": "title"
    }
    max_retries = 5
    retry_delay = 1  # initial delay in seconds
    for _ in range(max_retries):
        response = requests.get(url, params=params)
        if response.status_code == 200:
            data = response.json()
            if data['data']:
                return data['data'][0]['paperId']
        elif response.status_code == 429:
            print(f"Rate limit exceeded. Retrying in {retry_delay} seconds.")
            time.sleep(retry_delay)
            retry_delay *= 2  # exponential backoff
    return None


def get_citations_data(paper_id) -> List[Dict]:
    url = f"https://api.semanticscholar.org/graph/v1/paper/{paper_id}/citations"
    params = {
        "fields": "title,venue,abstract,openAccessPdf,publicationTypes,journal,publicationVenue,year"
    }
    max_retries = 5
    retry_delay = 1  # initial delay in seconds

    for attempt in range(max_retries):
        response = requests.get(url, params=params)
        if response.status_code == 200:
            data = response.json()
            return [citation_dict.get('citingPaper') for citation_dict in data.get('data', []) if citation_dict]
        elif response.status_code == 429:
            logging.info(f"get_paper_citations: Rate limit exceeded. Retrying in {retry_delay} seconds.")
            time.sleep(retry_delay)
            retry_delay *= 2  # exponential backoff
    return []


def get_paper_abstract(title, authors: str):
    """
    The Semantic Scholar API does implement rate limits.
    To work around rate limits without interrupting the retrieval of paper abstracts, you can implement a retry mechanism with exponential backoff.
    This approach will help you handle rate limit errors gracefully by waiting for a certain period before retrying the request.
    """
    if authors.strip() == '' or authors.strip() is None:
        return None

    url = "https://api.semanticscholar.org/graph/v1/paper/search/match"

    params = {
        "query": f"{title}",
        "fields": "abstract"
    }
    max_retries = 5
    retry_delay = 1  # initial delay in seconds
    for _ in range(max_retries):
        response = requests.get(url, params=params)
        if response.status_code == 200:
            data = response.json()
            if data['data']:
                # Find the paper that matches both the title and the author
                for paper in data['data']:
                    abstract = paper['abstract']
                    return abstract
            return None
        elif response.status_code == 429:
            logging.warning(f"get_paper_citations: Rate limit exceeded. Retrying in {retry_delay} seconds.")
            time.sleep(retry_delay)
            retry_delay *= 2  # exponential backoff

    return None
