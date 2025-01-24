"""
Analyze the code weaknesses reported in CppCheckWeakness table.
Approach:
We first distinguish between code snippets that have never been revised and code snippets that have been revised.

"""

from prettytable import PrettyTable

from sources.util import get_post_version_count
from sources.queryservice import QueryService
from sources.util import get_colossus04_ip


def get_cwe_snippets_dict(cppcheck_version: float = 2.13):
    """
        {
         root_id_1: {
           version_1: [190, 461] (the list of cwes reported for this version),
           version_2: [190, 461]
         },
         root_id_2: {
           version_1: [190, 461]
           version_2: [190, 461]
         }

        }
    """
    query = f"""SELECT PostId, RootPostBlockVersionId, PostBlockVersionId, CWE
                FROM   CppCheckWeakness
                WHERE  Version={cppcheck_version} AND
                       IgnoreSnippetVersion=False AND
                       IsGuesslangUsed=False AND
                       CWE != 0 AND 
                       YEAR(DataSetReleaseDate)=2022
                       
    """
    qs = QueryService()
    qs.connect(host=get_colossus04_ip())
    rows = qs.execute_and_fetchall(query)
    qs.close()

    data_dict = {}
    for row in rows:
        cwe = row['CWE']
        post_id = row['PostId']
        root_id = row['RootPostBlockVersionId']
        snippet_id = row['PostBlockVersionId']
        versions_dict = data_dict.setdefault(root_id, {})
        cwes = versions_dict.setdefault(snippet_id, [])
        cwes.append(cwe)
    return data_dict


def print_general_stats(data_dict: dict):
    for revision_count in [0, 1, 2, '>=1', '>=3']:
        # how many code snippets with weaknesses
        snippets = set()
        versions = set()
        posts = set()
        dict_list = []
        if isinstance(revision_count, int):
            revision_dict = data_dict.get(revision_count)
            for post_id, values in revision_dict.items():
                posts.add(post_id)
                for value in values:
                    root_id, snippet_id, cwe = value
                    if cwe != 0:
                        snippets.add(root_id)
                        versions.add(snippet_id)
        elif revision_count == '>=1':
            keys = {k for k in data_dict.keys() if k >= 1}
            dict_list = [data_dict.get(k) for k in keys]
        else:
            keys = {k for k in data_dict.keys() if k >= 3}
            dict_list = [data_dict.get(k) for k in keys]

        for revs_dict in dict_list:
            for post_id, values in revs_dict.items():
                posts.add(post_id)
                for value in values:
                    root_id, snippet_id, cwe = value
                    if cwe != 0:
                        snippets.add(root_id)
                        versions.add(snippet_id)
        if revision_count == 0:
            print(
                f"{revision_count} Revisions [Posts: {len(posts)}, Snippets: {len(snippets)}, Versions: {len(versions)}]")
        else:
            # check
            print(
                f"{revision_count} Revisions [Posts: {len(posts)}, Snippets: {len(snippets)}, Versions: {len(versions)}]")


def reorganize_by_revision_count(data_dict: dict) -> dict:
    """
    Input dictionary is of the form:
    {
      root_id_1: {
        version_id_1: [list of cwes reported in this version],
        version_id_2: [list of cwes reported in this version]
      },
      root_id_2: {
       ....
      }
    }

    And returns a dictionary of the form:
    {
      2 (# of versions of root_id_1 reported to contain weaknesses): {
           root_id_1: {
            version_id_1: [list of cwes reported in this version],
            version_id_2: [list of cwes reported in this version]
          }
      1: {
         root_id_2: {
            version_id_1: [list of cwes reported in this version],
          }
      }
      }
    }
    """
    organize_by_update_count = {}
    for root_id, versions_dict in data_dict.items():
        version_count = len(versions_dict.keys())
        revisions_dict = organize_by_update_count.setdefault(version_count, {})
        revisions_dict[root_id] = versions_dict
    return organize_by_update_count


def row_level_function(data_dict: dict, revision_count):
    """
    Determines whether versions of each code snippets improved/worsened the security weakness
    for each code snippet in the given input dictionary.

    data_dict is a dictionary of the form:
    {
    revision_count: {
          root_id_1: {
            version_id_1: [704, 908],
            version_id_2: []
          },
          root_id_2: {
            version_id_1: [],
            version_id_2: []
          },
    }

    }
    """
    snippets = set()
    unchanged = set()
    improved = set()
    deteriorated = set()

    if isinstance(revision_count, str):
        # the keys are int values denoting the number of code versions detected to contain a weaknesses for a code snippet.
        keys = set(data_dict.keys())
        if revision_count == '>=1':
            # this means at least two edits of the code snippet (excluding the root snippet)
            revision_counts = [key for key in keys if key >= 2]
        else:
            revision_counts = [key for key in keys if key >= 4]
    else:
        revision_counts = [revision_count]
    for revision_count in revision_counts:
        revisions_dict = data_dict.get(revision_count)
        for root_id, versions_dict in revisions_dict.items():

            # get improvements/unchanged/deteriorated for those snippets revised at lease once
            # order the snippets by their unique identifiers
            snippets.add(root_id)
            if revision_count != 1:
                snippet_ids = sorted(versions_dict.keys())
                first_version = snippet_ids[0]
                last_version = snippet_ids[-1]
                first_version_cwes = versions_dict.get(first_version)
                latest_version_cwes = versions_dict.get(last_version)
                if len(latest_version_cwes) > len(first_version_cwes):
                    # deteriorated
                    deteriorated.add(root_id)
                elif len(latest_version_cwes) < len(first_version_cwes):
                    # improved
                    improved.add(root_id)
                else:
                    unchanged.add(root_id)
    return snippets, unchanged, improved, deteriorated


def describe_root_id_weaknesses(data: dict) -> tuple:
    no_weakness = set()
    weakness = set()
    for root_id, versions_dict in data.items():
        # get all the versions of this code snippet reported to contain weaknesses
        versions_with_weaknesses = {version_id for version_id in versions_dict.keys()}
        if root_id in versions_with_weaknesses:
            weakness.add(root_id)
        else:
            no_weakness.add(root_id)
    return len(data.keys()), no_weakness, weakness


def get_version_combinations(data: dict):
    """
    Transforms the given dictionary to the following form:
    {
        root_id: [all subsequent version combinations of the root_id]
    }

    Example:
    {
        root_id: [(version_1, version_2), (version_2, version_3)],
        ...
    }
    """
    results = {}
    for root_id, versions_dict in data.items():
        version_combination = results.setdefault(root_id, [])
        version_ids = sorted(versions_dict.keys())
        if len(version_ids) > 1:
            # version_ids must have at least two versions for the combination to work
            start = 0
            end = 1
            while end < len(version_ids):
                version_combination.append((version_ids[start], version_ids[end]))
                start += 1
                end += 1
    return results


def cwes_in_recent_answer_versions():
    """
    select the latest version of each code snippet, count the cwes in each latest snippet
    and the answers they belong to.

    {
      answer_id_1: {
        latest_snippet_id_1: [list of cwes],
        latest_snippet_id_2: [list of cwes]
      },
      answer_id_2: {
        latest_snippet_id_1: [list of cwes],
        latest_snippet_id_2: [list of cwes]
      },
    }
    """
    query = """SELECT PostId, RootPostBlockVersionId, PostBlockVersionId, CWE
               FROM   CppCheckWeakness
               WHERE  CWE != 0 AND YEAR(DataSetReleaseDate) = 2022
            """
    qs = QueryService()
    qs.connect(host=get_colossus04_ip())
    rows = qs.execute_and_fetchall(query)
    qs.close()

    data_dict = {}
    for row in rows:
        cwe = row['CWE']
        post_id = row['PostId']
        root_id = row['RootPostBlockVersionId']
        snippet_id = row['PostBlockVersionId']
        root_id_dict = data_dict.setdefault(post_id, {})
        versions_dict = root_id_dict.setdefault(root_id, {})
        cwes = versions_dict.setdefault(snippet_id, [])
        cwes.append(cwe)

    result = {}
    for post_id, root_id_dict in data_dict.items():
        count = result.setdefault(post_id, 0)
        for root_id, versions_dict in root_id_dict.items():
            sorted_versions = sorted(versions_dict.keys())
            latest_snippet_cwes = versions_dict.get(sorted_versions[-1])
            # count.update(latest_snippet_cwes)
            count += len(latest_snippet_cwes)
        result[post_id] = count
    # test: 2642888, 9816734
    print(f"Answers: {len(result.keys())}, CWES = {sum(result.values())}")


def compare_subsequent_versions():
    """
    To easily compare subsequent versions for each code snippets, we should transform the
    ff. dictionary:
    {
        root_id_1: {
            root_id_1: [190, 461] (the list of cwes reported for this version),
            version_2: [190, 461],
            version_3: [109, 403]
        },
        root_id_2: {
            version_2: [190, 461],
            version_3: [190, 461]
        }
    }
    to
    {
      root_id_1 : [(root_id_1, version_2), (version_2, version_3)],
      root_id_2: [(version_2, version_3)]
    }
    """
    data_dict = get_cwe_snippets_dict()
    transformed_dict = get_version_combinations(data_dict)

    improved = set()
    worsen = set()
    unchanged = set()
    count = 0
    all_versions = set()
    for root_id, version_combinations in transformed_dict.items():
        for v1, v2 in version_combinations:
            all_versions.update({v1, v1})
            v1_cwes = data_dict.get(root_id).get(v1)
            v2_cwes = data_dict.get(root_id).get(v2)
            if len(v2_cwes) < len(v1_cwes):
                # v2 improved v1
                improved.add(v2)
            elif len(v1_cwes) < len(v2_cwes):
                # v2 deteriorated/worsened since it has more weaknesses than v1
                worsen.add(v2)
            else:
                # both v1 and v2 have the same no. of weaknesses
                cwe_ids_changed = True if set(v1_cwes).difference(set(v2_cwes)) else False
                if cwe_ids_changed:
                    print(f"CWES differs: v1_cews{(v1_cwes)}, v2_cews:{v2_cwes}")
                    count += 1
                unchanged.add(v2)
    """
    Results:
    +-----------+----------+--------------+-------+
    | Unchanged | Improved | Deteriorated | Count |
    +-----------+----------+--------------+-------+
    |    5406   |   194    |     109      |   63  |
    +-----------+----------+--------------+-------+
    """
    table = PrettyTable()
    table.field_names = ['AllVersions', 'Unchanged', 'Improved', 'Deteriorated', 'Count']
    table.add_row([len(all_versions), len(unchanged), len(improved), len(worsen), count])
    print(table)


def compare_first_and_last_version():
    """
    This function tests the following observation made by the authors using the June 2022 dataset:

    <quote>
    ...
    while after multiple code revisions, i.e., comparing the last version with the first version, 33.4% eventually
    correct code weaknesses.
    Therefore, more weaknesses are eventually fixed even though they were not fixed in earlier revisions.
    </quote>

    +-----------+----------+--------------+
    | Unchanged | Improved | Deteriorated |
    +-----------+----------+--------------+
    |    3832   |   169    |      86      |
    +-----------+----------+--------------+
    """
    data_dict = get_cwe_snippets_dict()
    improved = set()
    worsened = set()
    unchanged = set()
    for root_id, versions_dict in data_dict.items():
        version_ids = sorted(versions_dict.keys())
        if len(version_ids) > 1:
            first_version = version_ids[0]
            last_version = version_ids[-1]
            if len(versions_dict.get(last_version)) < len(versions_dict.get(first_version)):
                improved.add(root_id)
            elif len(versions_dict.get(first_version)) < len(versions_dict.get(last_version)):
                worsened.add(root_id)
            else:
                # equal
                unchanged.add(root_id)
    table = PrettyTable()
    table.field_names = ["Unchanged", "Improved", "Deteriorated"]
    table.add_row([len(unchanged), len(improved), len(worsened)])
    print(table)


def weaknesses_found_latest_version_of_answers():
    """
        12,998 CWE instances are identified within the latest versions of the 7,481 answers
    """
    pass


def RQ1_1():
    """
    How many CWE instances are found in the latest versions of answers?

    The first dict is as follows:
    {
         root_id_1: {
           version_1: [190, 461] (the list of cwes reported for this version),
           version_2: [190, 461]
         },
         root_id_2: {
           version_1: [190, 461]
           version_2: [190, 461]
         }
    }

    """
    cwe_dict = get_cwe_snippets_dict()
    total_cwes = []
    answers = set()
    qs = QueryService()
    qs.connect()
    for root_id, versions_dict in cwe_dict.items():
        version_ids = sorted(versions_dict.keys())
        if len(version_ids) > 1:
            latest_version = version_ids[-1]
            cwes = versions_dict.get(latest_version)
            total_cwes.extend(cwes)
            answer_id = qs.get_pbv_postId(root_id)
            answers.add(answer_id)
    qs.close()
    print(f"Latest versions of answers: Answers: {len(answers)}, cwes: {len(total_cwes)}")


def RQ2_1():
    """
    How many code snippets has weaknesses introduced in their first versions?
    This addresses the following claim made by the authors:
    <quote>
        92.6 percent (i.e., 10,884) of the 11,748 Codew has weaknesses introduced when their code snippets were initially created on Stack Overflow
    </quote>
    """
    data_dict = get_cwe_snippets_dict()
    total_snippets, root_ids_no_weaknesses, root_ids_with_weakneses = describe_root_id_weaknesses(data_dict)
    total_with_weaknesses = len(root_ids_with_weakneses)
    total_without_weaknesses = len(root_ids_no_weaknesses)
    print(f"""first versions with weaknesses: {total_with_weaknesses} ({percentage(total_with_weaknesses, total_snippets)}),
              first versions with no weakness: {total_without_weaknesses} ({percentage(total_without_weaknesses, total_snippets)})""")


def percentage(part, whole):
    percent = 100 * float(part) / float(whole)
    percent = round(percent, 1)
    return f"{part} ({str(percent)}%)"


def main():
    data_dict = get_cwe_snippets_dict()
    organized_dict = reorganize_by_revision_count(data_dict)

    table = PrettyTable()
    table.field_names = ['#Revisions', 'Code_w', '#Unchanged', '#Improved', '#Deteriorated']
    for revision_count in [1, 2, 3, '>=1', '>=3']:
        snippets, unchanged, improved, deteriorated = row_level_function(organized_dict, revision_count)
        total_snippets = len(snippets)
        total_unchanged = len(unchanged)
        total_improved = len(improved)
        total_deteriorated = len(deteriorated)
        table.add_row([revision_count, total_snippets,
                       percentage(total_unchanged, total_snippets),
                       percentage(total_improved, total_snippets),
                       percentage(total_deteriorated, total_snippets)])
    print(table)


if __name__ == '__main__':
    #RQ2_1()
    RQ1_1()
    #main()
    # cwes_in_recent_answer_versions()
    print('Analysis of June 2022 weaknesses complete')
