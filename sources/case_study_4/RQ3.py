from nltk.corpus import stopwords
from nltk.stem.wordnet import WordNetLemmatizer
import string
from gensim import corpora
from gensim.models.ldamodel import LdaModel

from sources.case_study_4.util import get_answers_by_type
from sources.queryservice import QueryService
from sources.util import get_colossus04_ip


def preprocess_title(single_ques_title):
    stop_words = stopwords.words('english')
    # print type(stop_words), len(stop_words)
    stop_words.append('python')

    exclude = set(string.punctuation)
    lemma = WordNetLemmatizer()

    stop_free = " ".join([i for i in single_ques_title.lower().split() if i not in stop_words])  ## one string
    # print stop_free
    punc_free = ''.join(ch for ch in stop_free if ch not in exclude)  ## one string
    # print punc_free
    splitted_title = punc_free.split(' ')  ## split single string to multiple
    encoded_list = [x_ for x_ in splitted_title]  ## lsit of strings , unicode error resolution: https://stackoverflow.com/questions/21129020/how-to-fix-unicodedecodeerror-ascii-codec-cant-decode-byte/35444608
    # print encoded_list
    final_title = " ".join(lemma.lemmatize(word) for word in encoded_list)  ## one string
    # print final_title

    return final_title


def model_topics(titles: list, topic_count: int) -> LdaModel:
    clean_title_list = [preprocess_title(single_title).split() for single_title in titles]
    dictionary = corpora.Dictionary(clean_title_list)
    doc_term_matrix = [dictionary.doc2bow(doc_) for doc_ in clean_title_list]
    # Lda = gensim.models.ldamodel.LdaModel
    ldamodel = LdaModel(doc_term_matrix, num_topics=topic_count, id2word=dictionary, passes=50)
    return ldamodel


def get_title(question_id: int, qs: QueryService) -> str:
    """
    Get the title of the question
    :param question_id: question id
    :return: title of the question
    """
    query = f"""SELECT Title FROM Posts WHERE Id = {question_id} AND PostTypeId=1"""
    return qs.execute_and_fetchone(query)['Title']


def get_titles(question_ids: list, qs: QueryService) -> list:
    """
    Get the titles of the questions
    :param question_ids: list of question ids
    :return: list of titles
    """
    return [get_title(question_id, qs) for question_id in question_ids]


def main():
    """
    1. Get the questions with at least one insecure answer and the questions with no insecure answer.
    2. Get the title of each set of questions
    3. Clean the titles
    """
    questions_map = get_answers_by_type(post_type_id=1)

    qs = QueryService()
    qs.connect()
    # get secure question titles
    secure_questions = questions_map['secure']
    secure_titles = [get_title(question_id, qs) for question_id in secure_questions]
    # get insecure questions
    insecure_questions = questions_map['insecure']
    insecure_titles = [get_title(question_id, qs) for question_id in insecure_questions]
    qs.close()

    # model secure topics
    num_topics = 5
    secure_topics_model = model_topics(secure_titles, num_topics)
    # perplexed_lda = secure_topics_model.log_perplexity(doc_term_matrix)
    print(f"Secure Question Titles[Topics: {secure_topics_model.get_topics()}, Rest: {secure_topics_model.print_topics(num_topics=num_topics, num_words=10)}")

    # model insecure topics
    insecure_topics_model = model_topics(insecure_titles, num_topics)
    print(f"Insecure Question Titles[Topics: {insecure_topics_model.get_topics()}, Rest: {insecure_topics_model.print_topics(num_topics=num_topics, num_words=10)}")


if __name__ == '__main__':
    main()
    print("Done")
