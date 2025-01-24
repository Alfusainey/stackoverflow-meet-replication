import pandas as pd

from sources.literature_search.util import is_dblp_indexed
from sources.util import get_db_password, get_db_user, get_db_engine
from prettytable import PrettyTable

from sources.util import get_colossus04_ip


def mark_duplicates(group: pd.DataFrame):
    """
    Function to mark duplicates based on the rules
    """
    group = group.copy()
    group['IsDuplicate'] = True

    if 'StackExchangeSite' in group['PaperSource'].values:
        group.loc[group['PaperSource'] == 'StackExchangeSite', 'IsDuplicate'] = False
    elif 'SOTorrentRef' in group['PaperSource'].values:
        group.loc[group['PaperSource'] == 'SOTorrentRef', 'IsDuplicate'] = False
    else:
        # Check for DBPL
        if 'DBPL' in group['PaperSource'].values:
            group.loc[group['PaperSource'] == 'DBPL', 'IsDuplicate'] = False
        else:
            # Check for ACM DL and IEEE Xplore
            if 'ACM DL' in group['PaperSource'].values:
                group.loc[group['PaperSource'] == 'ACM DL', 'IsDuplicate'] = False
            elif 'IEEE Xplore' in group['PaperSource'].values:
                group.loc[group['PaperSource'] == 'IEEE Xplore', 'IsDuplicate'] = False

    return group


def main():
    """
    To run this script, the following environment variables must be set:
    - DB_USER: The username to connect to the database
    - DB_PASSWORD: The password to connect to the database
    - DB_HOST: The IP address of the database. Defaults to localhost
    - OPENAI_API_KEY: The API key for OpenAI
    """
    sql_query = """SELECT PaperTitle, VenueName, PaperSource, GPT4oLabel
                   FROM CrawledPapers
                """
    engine = get_db_engine(get_db_user(), get_db_password(), 'sotorrent22', db_host=get_colossus04_ip())
    df = pd.read_sql(sql_query, engine)

    # DBLP always adds a dot at the end of the title, so we remove it first and normalize the title to lowercase
    df['PaperTitle'] = df['PaperTitle'].str.replace('.', '').str.lower()

    # Apply the function to each group of PaperTitle
    df = df.groupby('PaperTitle').apply(mark_duplicates).reset_index(drop=True)

    #  count the number of duplicates
    print(f"Duplicates Value Counts: {df['IsDuplicate'].value_counts()}")

    # relevant studies which are not duplicates
    sr_papers = df[(df['IsDuplicate'] == False) & (df['GPT4oLabel'] == True)]

    # mark studies that are DBLP-indexed
    sr_papers['IsDBLPIndexed'] = sr_papers['VenueName'].apply(is_dblp_indexed)

    sr_papers = sr_papers[sr_papers['IsDBLPIndexed'] == True]
    # uncomment to save the relevant studies to a csv file
    # sr_papers.to_csv('relevant_studies.csv', index=False, header=True, sep=',', columns=['PaperSource', 'PaperTitle'])

    table = PrettyTable()
    table.field_names = ["Paper Source", "Paper Title"]
    for index, row in sr_papers.iterrows():
        table.add_row([row['PaperSource'], row['PaperTitle']])
    print(table)


if __name__ == '__main__':
    main()
