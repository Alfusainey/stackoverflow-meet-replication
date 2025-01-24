"""
Uses the CppCheck static analyzer to detect weaknesses in code snippets that were downloaded using the download_snippets.py script.
"""
import csv
import os
from datetime import datetime
from multiprocessing.pool import Pool

from util import run_cpp_check, checkout_and_compile
from sources.util import Command
from sources.queryservice import QueryService


def write_to_file(records: tuple, year: int, version: float) -> None:
    file_name = f"{year}_{version}_cppcheck_duplicates.csv"
    file_path = f"{os.getcwd()}/{file_name}"
    with open(file_path, 'w') as f:
        writer = csv.writer(f)
        writer.writerow(['PostId', 'RootPostBlockVersionId', 'PostBlockVersionId', 'CWE'])
        for row in records:
            post_id = row['PostId']
            root_id = row['RootPostBlockVersionId']
            snippet_id = row['PostBlockVersionId']
            cwe = row['CWE']
            writer.writerow((post_id, root_id, snippet_id, cwe))


def cppcheck_task(input_: tuple):
    version, ds_release_date, snippet_file = input_
    results = run_cpp_check(snippet_file)
    rows_to_insert = set()
    insert_query = f"""INSERT INTO CppCheckWeakness(Language, PostId, RootPostBlockVersionId, PostBlockVersionId, 
    CWE, MessageId, Severity, Message, DataSetReleaseDate, IsGuesslangUsed, Version) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, 
    %s, %s, %s) """
    # remove the base path
    file_name = snippet_file.rsplit('/', 1)[-1]
    # remove file extension
    file_name, file_ext = file_name.split('.')
    language = 'C++' if file_ext == 'cpp' else 'C'
    post_id, root_id, snippet_id = tuple(map(int, file_name.split('_')))

    for result_dict in results:
        cwe = result_dict.get('cwe')
        message_id = result_dict.get('message_id')
        severity = result_dict.get('severity')
        message = result_dict.get('message')
        rows_to_insert.add(
            (language, post_id, root_id, snippet_id, cwe, message_id, severity, message, ds_release_date,
             is_guesslang_used, version))

    if rows_to_insert:
        qs = QueryService()
        qs.connect(db_name='sotorrent22')
        qs.execute_insert_and_commit(insert_query, list(rows_to_insert))
        qs.close()


def main():
    for version in [1.86, 2.13]:
        compiled_version = checkout_and_compile(version)
        if version in compiled_version:
            for ds_release_date in [datetime(2018, 12, 9), datetime(2022, 6, 30)]:
                year = ds_release_date.year
                print(f"Running Cppcheck {version} on {year} version of SOTorrent", flush=True)
                snippets_directory = f"{base_directory}/{year}"
                for language_dir in os.listdir(snippets_directory):
                    dir_name = f"{snippets_directory}/{language_dir}"
                    language_files = [(version, ds_release_date, f"{dir_name}/{snippet_file}") for snippet_file in os.listdir(dir_name)]
                    print(f"Processing {language_dir} containing {len(language_files)} files", flush=True)
                    with Pool(processes=50) as pool:
                        pool.map(cppcheck_task, language_files)
                    print(f"Completed {dir_name}", flush=True)


def cpp_check_testing():
    """
    Testing different versions of cppcheck on sample answers. Version 1.86 reports security weaknesses for the
    answers in the samples directory while version 2.9 did not detect any weaknesses.
    """
    """
    for version in [1.86, 2.9]:
    """
    path_prefix = '~/cppcheck/so_samples'
    for version in [1.86, 2.13]:
        # this is now in cppcheck_checkout_and_compile(version)
        version_str = '2.13.0' if version == 2.13 else '1.86'
        git_command = Command(f"git checkout {version_str}")
        git_command.run()
        make_clean = Command('make clean')
        make_clean.run()
        make_command = Command('make')
        make_command.run()
        cpp_check_version = Command('./cppcheck --version')
        rc, out, error = cpp_check_version.run()
        print(f"checked-out version: {version_str}, compiled-version: {out}")
        for snippet_file in [f"{path_prefix}/answer_15324370.cpp", f"{path_prefix}/answer_50541784_2.c",
                             f"{path_prefix}/answer_50541784.c"]:
            results = run_cpp_check(snippet_file)
            print(f"{out} {snippet_file}: {results}")


if __name__ == '__main__':
    base_directory = '~/dataset/cppcheck_snippets'
    is_guesslang_used = False
    # cpp_check_testing()
    main()
    print('DONE!!!')
