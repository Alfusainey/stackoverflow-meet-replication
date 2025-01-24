import os

from prettytable import PrettyTable

from sources.literature_search.util import get_api_key, classify_paper


def read_contents(file_path):
    with open(file_path, 'r') as f:
        lines = f.readlines()
        return lines[0], lines[1], lines[2]


def llm_evaluate(file_list: list, start) -> list:
    result = []
    for file in file_list:
        title, doi_link, abstract = read_contents(file)
        api_key = get_api_key()
        is_relevant, justification = classify_paper(api_key, abstract)
        result.append((f"{title.strip()}[[Paper Link]]({doi_link.strip()})", is_relevant))
        start += 1
    return result


def main():
    base_dir = 'sources/literature_search/abstracts'
    positive_files = [f"{base_dir}/relevant/{file}" for file in os.listdir(f"{base_dir}/relevant")]
    negative_files = [f"{base_dir}/not_relevant/{file}" for file in os.listdir(f"{base_dir}/not_relevant")]

    table = PrettyTable()
    table.field_names = ["Paper Title", "ExpectedLabel", "GPT4Label"]

    positive_results = llm_evaluate(positive_files, start=ord('A'))
    negative_results = llm_evaluate(negative_files, start=ord('K'))

    header = "| Paper Title | True Level | GPT-4o Label |"
    header += "\n" + "| :---------------------------------------------------- | :------------------ | :------------------ |"

    results = ""
    for title, is_relevant in positive_results:
        results += "| " + title + " | " + str(True) + " | " + str(is_relevant) + " |" + "\n"
    for title, is_relevant in negative_results:
        results += "| " + title + " | " + str(False) + " | " + str(is_relevant) + " |" + "\n"

    print(header + "\n" + results)


if __name__ == '__main__':
    main()
    print("Evaluation completed.")
