import csv

from case_studies.src.util import get_base_path
from data_access.queryservice import QueryService
from main.util import get_colossus04_ip


def get_accepted_answers() -> set:
    query = """SELECT AcceptedAnswerId
               FROM Posts
            """
    qs = QueryService()
    qs.connect(host=get_colossus04_ip())
    rows = qs.execute_and_fetchall(query)
    qs.close()
    return {row['AcceptedAnswerId'] for row in rows if row['AcceptedAnswerId']}


def get_answers_by_type(post_type_id: int = 2):
    base_path = get_base_path()
    csv_file = f"{base_path}/prior_work/sources/case_studies/src/snakes_in_paradies/data/scan_output.csv"

    KEY_SECURE = 'secure'
    KEY_INSECURE = 'insecure'
    mapping = {
        KEY_SECURE: set(),
        KEY_INSECURE: set()
    }
    with open(csv_file, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            type_count = int(row['TYPE_COUNT'])
            if post_type_id == 2:
                answer_id = int(row['BODY_ID'])
                if type_count <= 0: # its usually 0
                    mapping[KEY_SECURE].add(answer_id)
                else:
                    mapping[KEY_INSECURE].add(answer_id)
            else:
                question_id = int(row['PARENT_ID'])
                if type_count <= 0: # its usually 0
                    mapping[KEY_SECURE].add(question_id)
                else:
                    mapping[KEY_INSECURE].add(question_id)
    return mapping
