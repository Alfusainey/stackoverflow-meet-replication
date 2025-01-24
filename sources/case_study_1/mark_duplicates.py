from data_access.queryservice import QueryService


def main():
    """
    Marks snippet versions that should be ignored when counting CWE instances for code snippets.

    There are code snippet versions which Cppcheck detected to be valid C AND C++ code and reported the same CWEs for
    both of them. Remember we first assumed that all code snippets are C snippets (and named the corresponding file
    with a .c extension) and then assumed that they are all C++ snippets (and thus named them with a .cpp extension)
    If we currently count as is, then we will be double counting the number of CWE instances reported for each code
    snippet (once for C and once for C++). So I want to mark cases (i.e., code snippet versions) so that we do not
    count them twice.

    This requires setting IgnoreSnippetVersion=True to mark that this snippet version should be ignored and not counted.
    We choose to ignore C code snippets since C++ is the higher order language and they share a common subset (C++ is not a superset of C).
    """
    language_to_ignore = 'C' # language we choose to exclude in the event of duplicates CWEs
    qs = QueryService()
    qs.connect()
    for year, version in [(2022, 2.13), (2022, 1.86), (2018, 2.13), (2018, 1.86)]:
        query = f"""SELECT c_answers.PostId, c_answers.RootPostBlockVersionId, c_answers.PostBlockVersionId, c_answers.CWE
                       FROM (SELECT PostId, RootPostBlockVersionId, PostBlockVersionId, CWE
                              FROM CppCheckWeakness 
                              WHERE Language='C' AND IsGuesslangUsed=False AND 
                              YEAR(DataSetReleaseDate)={year} AND Version={version} AND 
                              CWE != 0
                             ) as c_answers
                       INNER JOIN
                          (SELECT PostId, RootPostBlockVersionId, PostBlockVersionId, CWE 
                            FROM CppCheckWeakness 
                            WHERE Language='C++' AND IsGuesslangUsed=False AND 
                                 YEAR(DataSetReleaseDate)={year} AND Version={version} AND 
                                 CWE != 0
                          ) as cpp_answers
                        ON c_answers.PostBlockVersionId=cpp_answers.PostBlockVersionId
                        WHERE c_answers.CWE = cpp_answers.CWE;
            """
        records = qs.execute_and_fetchall(query)
        already_updated = set()
        for row in records:
            cwe = row['CWE']
            post_id = row['PostId']
            root_id = row['RootPostBlockVersionId']
            snippet_id = row['PostBlockVersionId']
            if (post_id, root_id, snippet_id, language_to_ignore) not in already_updated:
                qs.execute_update_and_commit(f"""UPDATE CppCheckWeakness
                                                 SET IgnoreSnippetVersion=True 
                                                 WHERE PostBlockVersionId={snippet_id} AND
                                                       RootPostBlockVersionId={root_id} AND
                                                       PostId={post_id} AND
                                                       CWE={cwe} AND 
                                                       Language='{language_to_ignore}' AND 
                                                       Version={version} AND 
                                                       YEAR(DataSetReleaseDate)={year}""")
                already_updated.add((post_id, root_id, snippet_id, language_to_ignore))
        # write_to_file(records, year, version)
    qs.close()


if __name__ == '__main__':
    main()
