import os

import pandas as pd

from sources.literature_search.util import get_api_key, classify_paper, get_logger, get_db_engine
from sources.util import get_db_user, get_db_password


def main():
    csv_files_dir = 'sources/literature_search/ieee/csv_files'

    engine = get_db_engine(get_db_user(), get_db_password(), 'sotorrent22')
    for entry_type in os.listdir(csv_files_dir):
        logger.info(f"Processing {entry_type}...")
        publication_type = 'journal'.upper() if entry_type == 'journals' else 'conference'.upper()
        entry_type_dir = os.path.join(csv_files_dir, entry_type)
        papers_to_insert = []
        for entry in os.listdir(entry_type_dir):
            entry_path_csv = os.path.join(entry_type_dir, entry)
            df = pd.read_csv(entry_path_csv, sep=',')
            for index, row in df.iterrows():
                doi = row["DOI"]
                title = row["Document Title"]
                if len(title) > 250:
                    logger.info(f"TitleTooLong: {title}")
                    continue
                if doi in doi_cache:
                    logger.info(f"DocumentExist: {title}")
                    continue
                doi_cache.add(doi)
                abstract = row["Abstract"]
                if pd.isnull(abstract) or pd.isna(abstract):
                    logger.info(f"NoAbstract: {title}")
                    continue

                is_relevant, justification = classify_paper(get_api_key(), abstract)
                papers_to_insert.append({
                    'PaperTitle': title,
                    'VenueName': row["Publication Title"],
                    'PublicationType': publication_type,
                    'URL': row["PDF Link"],
                    'PaperSource': 'IEEE Xplore',
                    'PaperAbstract': abstract,
                    'GPT4oLabel': is_relevant,
                    'Justification': justification,
                })
            # insert papers_to_insert into database
            df = pd.DataFrame(papers_to_insert)
            df.to_sql('CrawledPapers', con=engine, if_exists='append', index=False)
            logger.info(f"Inserted {len(papers_to_insert)} papers from {entry_path_csv}")
        logger.info(f"Finished processing {entry_type}.")
    logger.info("Finished processing all IEEE Xplore entries.")


if __name__ == "__main__":
    doi_cache = set()
    logger = get_logger()
    main()
