"""
count the number of questions linked to > 0 .py files
"""
from sources.queryservice import QueryService


def get_question_to_files_mapping(qs: QueryService) -> dict:
    query = """SELECT PostId, COUNT(FileExt) as files
               FROM September2018PostReferenceGH 
               WHERE FileExt='.py'
               GROUP BY PostId"""
    rows = qs.execute_and_fetchall(query)
    results = {}
    for row in rows:
        results[row['PostId']] = row['files']
    return results


def get_questions_linked_to_GH(qs: QueryService) -> set:
    query = """SELECT Id 
               FROM September2018Posts  
               WHERE PostTypeId=1 AND AnswerCount > 0 AND Score > 0 AND ViewCount > 0"""
    rows = qs.execute_and_fetchall(query)
    posts = set()
    mapping = get_question_to_files_mapping(qs)
    for row in rows:
        post_id = row['Id']
        py_files_count = mapping.get(post_id, 0)
        if py_files_count > 0:
            posts.add(post_id)
    return posts


def main():
    qs = QueryService()
    qs.connect(db_name='sotorrent18_12', port=3307)
    print(f"Results is: {len(get_questions_linked_to_GH(qs))}")
    qs.close()


if __name__ == '__main__':
    main()
    print("Done!")
