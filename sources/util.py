import csv
import os
import sys
import threading
import subprocess
import traceback
import shlex
from datetime import datetime

from sqlalchemy import create_engine

from sources.queryservice import QueryService


class Command(object):
    """
        https://gist.github.com/kirpit/1306188, which is based on https://stackoverflow.com/a/4825933/8462878
    """
    def __init__(self, command):
        self.process = None
        self.rc = None
        self.output = None
        self.error = None
        self.is_terminated = False
        if isinstance(command, str):
            self.arguments = shlex.split(command)
        else:
            self.arguments = command

    def run(self, timeout=300, workdir='/home/alfusainey.jallow/cppcheck'):
        """ Run a command then return: (status, output, error). """
        def target():
            try:
                self.process = subprocess.Popen(self.arguments, cwd=workdir, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, encoding='UTF-8')
                self.output, self.error = self.process.communicate()
                self.rc = self.process.returncode
            except:
                self.error = traceback.format_exc()
                self.rc = -1

        # spawn a thread to run and wait on the subprocess
        thread = threading.Thread(target=target)
        thread.start()
        thread.join(timeout)
        if thread.is_alive():
            self.process.terminate()
            thread.join()
            self.is_terminated = True
        return self.rc, self.output, self.error


def get_db_user():
    return os.environ["DB_USER"]


def get_db_password():
    return os.environ["DB_PASSWORD"]


def get_db_engine(db_user, db_password, database, db_host='127.0.0.1'):
    return create_engine(f"mysql+mysqlconnector://{db_user}:{db_password}@{db_host}/{database}")


def get_base_path() -> str:
    return '/home/alfusainey.jallow/gitlab' if len(sys.argv) > 1 else '/Users/alfu/phd/gitlab'

def get_snippet_creation_date(snippet_id, qs: QueryService):
    history_id = qs.get_pbv_PostHistoryId(snippet_id)
    return qs.getSnippetCreationDate(history_id)


def str_to_date(date_str: str) -> datetime.date:
    return datetime.strptime(date_str, '%Y-%m-%d').date()


def str_to_datetime(date_str: str) -> datetime:
    """
    Converts the given Date STR to a datetime object.
    """
    # e.g 2019-12-16 17:18:33
    return datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S')


def get_posts_with_code():
    query = """SELECT PostId, Language
               FROM PostBlockVersion pbv
               INNER JOIN CodeBlockVersion cbv
                 ON pbv.Id = cbv.PostBlockVersionId
               GROUP BY PostId, Language
            """
    qs = QueryService()
    qs.connect(db_name='sotorrent22')
    records = qs.execute_and_fetchall(query)
    qs.close()

    posts = set()
    for row in records:
        post_id = row['PostId']
        language = row['Language']
        posts.add((post_id, language))
    return posts


def write_to_file(file_path):
    # To reduce false positives, we select only posts containing code snippets
    posts = get_posts_with_code()
    with open(file_path, 'w') as f:
        writer = csv.writer(f)
        writer.writerow(['PostId', 'Language'])
        for post_id, language in posts:
            writer.writerow([post_id, language])


def get_posts_with_code_from_file(file_path):
    posts = set()
    with open(file_path, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            post_id = int(row['PostId'])
            language = row['Language']
            posts.add((post_id, language))
            posts.add(post_id)
    return posts


def get_csv_data(csv_file: str) -> dict:
    result = {}
    with open(csv_file, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            answer_id = row['AnswerID']
            answer_rows = result.setdefault(answer_id, [])
            answer_rows.append(
                {
                    'QuestionID': row['QuestionID'],
                    'AnswerID': answer_id,
                    'PostHistoryDate': row['PostHistoryDate'],
                    'VersionNumber': row['VersionNumber'],
                    'VersionIndex': row['VersionIndex'],
                    'Result': row['Result'],
                    'CWE': row['CWE'],
                    'lines': row['lines']
                }
            )

    return result


def get_csv_data_new(csv_file: str) -> dict:
    result = {}
    with open(csv_file, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            answer_id = int(row['AnswerID'])
            ph_ids = result.setdefault(answer_id, set())
            ph_id = row['PostHistoryDate']
            ph_ids.add(ph_id)
    return result


def get_snippet_length_data(input_tuple):
    """
    Selects all code snippets and versions of the given post
    """
    post_id, language = input_tuple
    query = f"""SELECT RootPostBlockVersionId, Id, LineCount
                FROM PostBlockVersion
                WHERE PostId={post_id} AND PostBlockTypeId=2
                GROUP BY RootPostBlockVersionId, Id, LineCount
    """
    qs = QueryService()
    qs.connect(db_name='sotorrent22')
    records = qs.execute_and_fetchall(query)

    rows_to_insert = []
    for row in records:
        snippet_id = row['Id']
        root_id = row['RootPostBlockVersionId']
        post_history_id = qs.get_pbv_PostHistoryId(root_id)
        creation_date = qs.getSnippetCreationDate(post_history_id)
        line_count = row['LineCount']
        rows_to_insert.append((post_id, root_id, language, snippet_id, line_count, creation_date))
    qs.close()

    return rows_to_insert


def get_language(snippet_id: int, qs: QueryService):
    return qs.execute_and_fetchone(f"SELECT Language FROM CodeBlockVersion WHERE PostBlockVersionId={snippet_id}")['Language']


def is_modified_since(post_id: int, release_date: datetime, qs: QueryService):
    """
    Returns True if the given post has been modified since the given release_date.

    This function uses the "08-Sep-2023 12:36" release of the Stack Overflow dataset.
    """
    last_edit_date = qs.execute_and_fetchone(f"SELECT LastEditDate FROM September2023Posts WHERE Id={post_id}")['LastEditDate']
    if last_edit_date is None:
        modified = False
    else:
        modified = last_edit_date >= release_date
    return modified


def is_deleted(post_id: int, qs: QueryService) -> bool:
    """
    Returns True if the given post has been deleted from the September 2023 release
    of the SO data dump. This dump was released on 08-Sep-2023 at 12:36
    """
    cursor = qs.client.run_query(f"SELECT Id FROM September2023Posts WHERE Id={post_id}")
    return cursor.rowcount == 0


def get_release_date(author: str) -> datetime:
    if author == 'fischer':
        return datetime(2018, 3, 13)
    elif author == 'fischer_oakland':
        return datetime(2016, 3, 0)
    elif author == 'uriel':
        # minig rule violations paper and c/c++ code weaknesses paper
        # both used the same sotorrent release
        return datetime(2018, 9, 12)
    else:
        return None


def get_post_version_count(post_id, qs: QueryService):
    return qs.execute_and_fetchone(f"SELECT COUNT(Id) as count FROM PostVersion WHERE PostId={post_id}")['count']


def test_db_connection(qs: QueryService):
    # Testing that we connected to the correct DB.
    count = qs.execute_and_fetchone(f"select count(Id) as count FROM PostBlockVersion")['count']
    return count

def get_colossus04_ip():
    return '134.96.225.229'
