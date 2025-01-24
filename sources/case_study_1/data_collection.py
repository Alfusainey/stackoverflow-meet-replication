"""
Script that predicts the programming language of code snippets.
The script is used for reproducing the results presented in the original study.
This script is not used in the replication because we removed Guesslang from the pipeline
"""
import os
from datetime import date

from guesslang import Guess

from sources.case_study_1.util import get_code_snippets
from sources.util import test_db_connection
from sources.queryservice import QueryService


def get_language(source_code):
    """Predict the programming language name of the given source code.
    """
    if guess.is_trained:
        language = guess.language_name(source_code)
        return language
    return None


def main():
    records = get_code_snippets(year)

    print(f"Processing {len(records)} records", flush=True)
    qs = QueryService()
    qs.connect(db_name='sotorrent22')
    query = """INSERT INTO CodeBlockVersionHaoxiangZhang(PostId, PostBlockVersionId, RootPostBlockVersionId, Language, DataSetReleaseDate)
               VALUES(%s, %s, %s, %s, %s)"""

    ds_release_date = date(2022, 6, 30) if year == 2022 else date(2018, 12, 9)
    for record in records:
        source_code = record['Content']
        language = get_language(source_code)  # if language is None, then no assignment was possible

        snippet_id = record['PostBlockVersionId']
        root_id = record['RootPostBlockVersionId']
        post_id = record['PostId']
        qs.execute_insert_and_commit(query, [(post_id, snippet_id, root_id, language, ds_release_date)])

    qs.close()


if __name__ == '__main__':
    os.environ['TF_CPP_MIN_LOG_LEVEL'] = '2'
    year = 2018
    guess = Guess()
    main()
    print("DONE!!!")
