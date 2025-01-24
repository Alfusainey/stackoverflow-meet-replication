from multiprocessing import Pool

from case_studies.src.code_weakness_paper.util import get_code_snippets


def write_task(record: dict):
    year = 2022
    snippets_directory = f"{base_directory}/{year}"
    answer_id = record['PostId']
    root_id = record['RootPostBlockVersionId']
    snippet_id = record['PostBlockVersionId']
    c_file_name = f"{answer_id}_{root_id}_{snippet_id}.c"
    cpp_file_name = f"{answer_id}_{root_id}_{snippet_id}.cpp"
    c_file_path = f"{snippets_directory}/c/{c_file_name}"
    cpp_file_path = f"{snippets_directory}/cpp/{cpp_file_name}"
    # we write the content as both .c and .cpp
    with open(c_file_path, 'w') as f:
        f.write(record['Content'])
    with open(cpp_file_path, 'w') as f:
        f.write(record['Content'])


def main():
    year = 2022
    records = get_code_snippets(year)
    with Pool(processes=50) as pool:
        pool.map(write_task, records)


if __name__ == '__main__':
    base_directory = 'zhang_dataset/snippets'
    main()

