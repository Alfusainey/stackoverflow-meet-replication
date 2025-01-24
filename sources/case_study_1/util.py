from sources.util import Command
from sources.queryservice import QueryService


def checkout_and_compile(version: float):
    """
    Checks out the given cppcheck version (using Git) and compiles that version (using Make).

    This is equivalent to:
    $ git checkout 2.13.0
    $ make clean
    $ make
    $ ./cppcheck --version
    Cppcheck 2.13.0

    :Args:
        version: The version of cppcheck to checkout and compile.
    :Returns:
        The compiled version.
    """
    version_str = '2.13.0' if version == 2.13 else '1.86'
    git_command = Command(f"git checkout {version_str}")
    git_command.run()
    make_clean = Command('make clean')
    make_clean.run()
    make_command = Command('make')
    make_command.run()
    cpp_check_version = Command('./cppcheck --version')
    rc, out, error = cpp_check_version.run()
    compiled_version = float(out.split(' ', 1)[-1].strip())
    return compiled_version


def run_cpp_check(snippet_file: str, timeout=300) -> list:
    """
    Runs the currently checkedout and compiled version of cppcheck on the given snippet file and returns
    a list of dictionary containing the results. When the cppcheck analyzer takes
    longer than the configured timeout value, its process is terminated.
    """

    # run cppcheck --language=c --template={cwe}:{id}:{severity}:{message}
    language = 'c++' if snippet_file.endswith('.cpp') else 'c'
    arguments = ['./cppcheck', f"--language={language}", '--template={cwe}:{id}:{severity}:{message}', snippet_file]
    command = Command(arguments)
    rc, out, error = command.run(timeout)
    results = []
    if command.is_terminated:
        print(f"""CPPCheck terminated: Took longer than {timeout} on {snippet_file}""", flush=True)
        return results
    elif rc != 0:
        print(f"DEBUG (Return Code): {rc}, Error: {error}", flush=True)
        return results
    # The output of cppcheck can span multiple lines, each severity printed in a separate line
    cpp_check_results = [line for line in out.split('\n') if line and 'Checking' not in line]
    for line in cpp_check_results:
        template_parts = line.split(':', 3)
        assert len(template_parts) == 4
        results.append({'cwe': int(template_parts[0]),
                        'message_id': template_parts[1],
                        'severity': template_parts[2],
                        'message': template_parts[3]})
    return results


def get_code_snippets(year: int):
    """
    Retrieve all code snippets with at least 5 LoC from answers tagged with C/C++.
    """
    qs = QueryService()
    if year == 2018:
        # Reproduction case: using the December 2018 dataset (same dataset used by the authors)
        qs.connect(port=3307, db_name='sotorrent18_12')
        # TODO: move this Posts table to the sotorrent18_12 database.
        posts_table = 'sotorrent20_03.December2018Posts'
    else:
        # Replication case: using a newer dataset version (April 2022)
        qs.connect(db_name='sotorrent22')
        posts_table = 'Posts'

    r = qs.execute_and_fetchall(f"""SELECT Answers.PostId AS PostId, RootPostBlockVersionId, pbv.Id AS PostBlockVersionId, Content
                                    FROM PostBlockVersion pbv
                                    INNER JOIN (SELECT distinct(Id) as PostId from {posts_table}
                                                WHERE PostTypeId=2 AND ParentId IN 
                                                (
                                                  select distinct(Id) FROM {posts_table} WHERE PostTypeId=1 AND (Tags LIKE '%<c++>%' OR Tags LIKE '%<c>%'))
                                                ) AS Answers 
                                    ON pbv.PostId=Answers.PostId 
                                    WHERE PostBlockTypeId=2 AND (PredEqual=0 OR PredEqual IS NULL) AND LineCount >= 5;
                                    GROUP BY PostId, RootPostBlockVersionId, PostBlockVersionId
                                """)
    qs.close()
    return r


def get_root_id(post_id: int, qs: QueryService, qs_3307: QueryService):
    q = f"""SELECT RootPostBlockVersionId
            FROM PostBlockVersion 
            WHERE PostBlockTypeId=2 AND 
                  LineCount >= 5 AND 
                  (PredEqual=0 OR PredEqual IS NULL) AND 
                  PostId={post_id}
            GROUP BY RootPostBlockVersionId
    """
    rows = qs_3307.execute_and_fetchall(q)
    if len(rows) == 1:
        return rows[0]['RootPostBlockVersionId'], None

    root_ids = {row['RootPostBlockVersionId'] for row in rows}

    query = f"""SELECT RootPostBlockVersionId
                FROM   CodeBlockVersionHaoxiangZhang
                WHERE  PostId={post_id} AND
                       Language IN ('C', 'C++') AND 
                       YEAR(DataSetReleaseDate)=2018
            """
    rows = qs.execute_and_fetchall(query)
    root_ids_new = {row['RootPostBlockVersionId'] for row in rows}
    intersect_set = root_ids.intersection(root_ids_new)

    if len(intersect_set) == 1:
        return list(intersect_set)[0], None

    return None, intersect_set
