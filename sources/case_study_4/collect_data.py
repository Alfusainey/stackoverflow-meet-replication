import csv
from typing import Set, List, Tuple

from sources.case_study_4.util import get_accepted_answers
from sources.queryservice import QueryService
from sources.util import get_colossus04_ip


def get_answers_with_body(question_id: int, qs: QueryService) -> List[Tuple]:
    query = f"""SELECT Id, Body, CreationDate
                FROM Posts
                WHERE ParentId={question_id} AND Body IS NOT NULL"""
    rows = qs.execute_and_fetchall(query)
    results = []
    for row in rows:
        results.append((row['Id'], row['Body'], row['CreationDate']))
    return results


def get_csv_rows(qs: QueryService) -> List[dict]:
    query = """SELECT Id, Score, ViewCount, CommentCount, FavoriteCount
               FROM Posts  s
               WHERE PostTypeId=1 AND AnswerCount > 0 AND Score > 0 AND ViewCount > 0 AND
                     Id IN (SELECT PostId FROM PostReferenceGH  WHERE FileExt='.py' AND PostTypeId=1 GROUP BY PostId)"""

    rows = qs.execute_and_fetchall(query)
    csv_rows = []
    for row in rows:
        question_id = row['Id']
        score = row['Score']
        view_count = row['ViewCount']
        comment_count = row['CommentCount']
        favorite_count = row['FavoriteCount']
        answers = get_answers_with_body(question_id, qs)
        for answer in answers:
            answer_id = answer[0]
            body = answer[1]
            creation_date = answer[2]
            csv_rows.append({'QuestionId': question_id,
                            'AnswerId': answer_id,
                            'Score': score,
                            'ViewCount': view_count,
                            'CommentCount': comment_count,
                            'FavouriteCount': favorite_count,
                            'Body': body,
                            'CreationDate': creation_date})
    return csv_rows


def _get_code_blocks(answer_id: int, qs: QueryService) -> Set[int]:
    query = f"""SELECT RootPostBlockVersionId
                FROM PostBlockVersion
                WHERE PostId={answer_id}"""
    rows = qs.execute_and_fetchall(query)
    return {row['RootPostBlockVersionId'] for row in rows}


def main_data_collection_stats():
    """
    Needed to collect stats about questions/answers/accepted answers/code blocks to write in the paper.
    """
    all_accepted_answers = get_accepted_answers()
    qs = QueryService()
    qs.connect(host=get_colossus04_ip())
    rows = get_csv_rows(qs)

    r = {
        'questions': set(),
        'answers': set(),
        'accepted_answers': set(),
        'code_blocks': set()
    }

    already_queried_code_blocks = set()
    for row in rows:
        question_id = row['QuestionId']
        r['questions'].add(question_id)
        answer_id = row['AnswerId']
        r['answers'].add(answer_id)
        if answer_id in all_accepted_answers:
            r['accepted_answers'].add(answer_id)
        if answer_id not in already_queried_code_blocks:
            code_blocks = _get_code_blocks(answer_id, qs)
            r['code_blocks'].update(code_blocks)
            already_queried_code_blocks.add(answer_id)
    qs.close()

    print(f"Questions: {len(r['questions'])}, Answers: {len(r['answers'])}, Accepted Answers: {len(r['accepted_answers'])}, Code Blocks: {len(r['code_blocks'])}")


def main():
    qs = QueryService()
    qs.connect(host=get_colossus04_ip())
    csv_rows = get_csv_rows(qs)
    qs.close()
    with open('data.csv', 'w') as f:
        writer = csv.DictWriter(f, fieldnames=set(csv_rows[0].keys()), delimiter='\t')
        writer.writeheader()
        for row in csv_rows:
            writer.writerow(row)


if __name__ == '__main__':
    # main()
    main_data_collection_stats()
    print('Done')
