import csv

from sources.case_study_4.util import get_accepted_answers
from sources.queryservice import QueryService
from sources.util import get_colossus04_ip


def get_insecure_type_mapping(csv_file: str, all_accepted_answers: set, qs: QueryService) -> dict:
    KEY_DATAPARSE = 'DATAPARSE'
    KEY_CIPHER = 'CIPHER'
    KEY_XSS = 'XSS'
    KEY_RACE = 'RACE'
    KEY_CMDINJECT = 'CMDINJECT'
    KEY_INSECURECONN = 'INSECURECONN'

    mapping = {
        KEY_DATAPARSE: {
            'code_block': [],
            'answers': set(),
            'accepted_answers': set(),
            'questions': set()
        },
        KEY_CIPHER: {
            'code_block': [],
            'answers': set(),
            'accepted_answers': set(),
            'questions': set()
        },
        KEY_XSS: {
            'code_block': [],
            'answers': set(),
            'accepted_answers': set(),
            'questions': set()
        },
        KEY_RACE: {
            'code_block': [],
            'answers': set(),
            'accepted_answers': set(),
            'questions': set()
        },
        KEY_CMDINJECT: {
            'code_block': [],
            'answers': set(),
            'accepted_answers': set(),
            'questions': set()
        },
        KEY_INSECURECONN: {
            'code_block': [],
            'answers': set(),
            'accepted_answers': set(),
            'questions': set()
        }
    }
    with open(csv_file, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            type_count = int(row['TYPE_COUNT'])
            if type_count == 0:
                continue
            question_id = int(row['PARENT_ID'])
            answer_id = int(row['BODY_ID'])
            type_ = row['TYPE']
            snippet_count = int(row['SNIPPET_CNT'])
            type_dict = mapping.get(type_, None)
            if type_dict:
                type_dict['code_block'].append(snippet_count)
                type_dict['answers'].add(answer_id)
                type_dict['questions'].add(question_id)
                if answer_id in all_accepted_answers:
                    type_dict['accepted_answers'].add(answer_id)
    return mapping


def get_questions_and_accepted_answers() -> dict:
    """
    Get the total number of accepted answers in the data.csv file.
    """
    csv_file = '/Users/alfu/phd/gitlab/prior_work/sources/case_studies/src/snakes_in_paradies/data/scan_output.csv'
    result = {
        'questions': set(),
        'accepted_answers': set(),
        'insecure_questions': set(),
        'insecure_accepted_answers': set()
    }
    with open(csv_file, 'r') as f:
        reader = csv.DictReader(f, delimiter=',')
        for row in reader:
            answer_id = int(row['BODY_ID'])
            question_id = int(row['PARENT_ID'])
            result['questions'].add(question_id)
            if answer_id in all_accepted_answers:
                result['accepted_answers'].add(answer_id)
            type_count = int(row['TYPE_COUNT'])
            if type_count > 0:
                result['insecure_questions'].add(question_id)
                if answer_id in all_accepted_answers:
                    result['insecure_accepted_answers'].add(answer_id)
    return result


def main_general_stats():
    result = get_questions_and_accepted_answers()
    # we observed x/Y questions to contain at least one insecure answer
    questions_with_insecure_answers = result['insecure_questions']
    total_questions = result['questions']
    print(f"we observed {len(questions_with_insecure_answers)}/{len(total_questions)} questions to contain at least one insecure answer")
    # we observed X/Y of the accepted answers to contain at least one insecure code snippet
    insecure_accepted_answers = result['insecure_accepted_answers']
    total_accepted_answers = result['accepted_answers']
    print(f"we observed {len(insecure_accepted_answers)}/{len(total_accepted_answers)} of the accepted answers to contain at least one insecure code snippet")


def main():
    """
    {
        'code_injection': {
          'code_block': int_value,
          'answers': int_value,
          'accepted_answers': int_value,
          'questions': int_value
        },
        'xss': {
             'code_block': int_value,
                'answers': int_value,
                'accepted_answers': int_value,
                'questions': int_value
        }
        'insecure_cipher': int_value,
        'insecure_connection': int_value,
        'race_condition': int_value,
        'untrusted_data_serialization': int_value,
    }
    """
    csv_file = '/Users/alfu/phd/gitlab/prior_work/sources/case_studies/src/snakes_in_paradies/data/scan_output.csv'

    qs = QueryService()
    qs.connect(host=get_colossus04_ip())
    insecure_type_mapping = get_insecure_type_mapping(csv_file, all_accepted_answers, qs)
    qs.close()
    for insecure_type, type_dict in insecure_type_mapping.items():
        print(insecure_type)
        print('Code block count:', sum(type_dict['code_block']))
        print('Answer count:', len(type_dict['answers']))
        print('Accepted answer count:', len(type_dict['accepted_answers']))
        print('Question count:', len(type_dict['questions']))
        print()


if __name__ == '__main__':
    all_accepted_answers = get_accepted_answers()
    # main()
    main_general_stats()
    print('Akond Rahman, Oct 30 2018, Answer to RQ1')
