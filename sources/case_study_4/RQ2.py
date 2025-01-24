import csv
import datetime
from statistics import mean

from sources.case_study_4 import cliffsDelta
from sources.case_study_4.util import get_answers_by_type
from sources.queryservice import QueryService
from sources.util import get_colossus04_ip
from scipy import stats


def get_users(qs: QueryService) -> dict:
    """
    Get users who have posted  code snippets and the IDs of the posts.
    """
    query = """SELECT OwnerUserId, p.Id AS PostId, Reputation, DATE(u.CreationDate) AS CreationDate, DATE(u.LastAccessDate) AS LastAccessDate
               FROM Posts p
               INNER JOIN Users u
                ON p.OwnerUserId = u.Id
               WHERE p.PostTypeId = 2 AND OwnerUserId > 0
            """
    rows = qs.execute_and_fetchall(query)
    users = {}
    for row in rows:
        user_id = row['OwnerUserId']
        post_id = row['PostId']
        normalized_reputation = get_normalized_reputation(row['Reputation'], row['CreationDate'], row['LastAccessDate'])
        users.setdefault(post_id, []).append((user_id, normalized_reputation))
    return users


def get_normalized_reputation(reputation: int, start_date: datetime.date, last_access_date: datetime.date) -> float:
    try:
        duration_days = (last_access_date - start_date).days / 30
        duration_days = 1 if duration_days < 1 else duration_days
        return float(reputation) / float(duration_days)
    except ZeroDivisionError:
        return float(1)


def compare_reputations(insecure_reputations: list, secure_reputations: list) -> tuple:
    try:
        TS, p = stats.mannwhitneyu(insecure_reputations, secure_reputations, alternative='greater')
    except ValueError:
        TS, p = 0.01, 0.95
    cliffs_delta = cliffsDelta.cliffsDelta(insecure_reputations, secure_reputations)
    insecure_reputations_mean = mean(insecure_reputations)
    secure_reputations_mean = mean(secure_reputations)
    return p, cliffs_delta, insecure_reputations_mean, secure_reputations_mean


def main():
    """
    1. Get users who have posted insecure code snippets and the IDs of the posts
    2. Users who have not posted insecure code snippets and the IDs of the posts
    """
    answers_by_type = get_answers_by_type()
    qs = QueryService()
    qs.connect(host=get_colossus04_ip())
    users_mapping = get_users(qs)
    users_secure_reputation = []
    users_insecure_reputation = []
    for post_id, users in users_mapping.items():
        for user_id, normalized_reputation in users:
            if post_id in answers_by_type['secure']:
                users_secure_reputation.append(normalized_reputation)
            else:
                users_insecure_reputation.append(normalized_reputation)

    # now compare the two sets of reputation
    p_value, cliffs_delta, insecure_mean, secure_mean = compare_reputations(users_insecure_reputation, users_secure_reputation)
    print(f"p-value:{p_value}, cliffs:{cliffs_delta}, mean value [insecure: {insecure_mean}, secure: {secure_mean}]")


if __name__ == '__main__':
    main()
    print('DONE!!!')
