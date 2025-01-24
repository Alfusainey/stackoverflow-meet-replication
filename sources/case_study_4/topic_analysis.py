"""
Topic Modeling 
Akond Rahman 
Oct 17 2018

Conda NLTK installtion warning:
 WARNING: The script tqdm is installed in '/Users/alfu/.local/bin' which is not on PATH.
  Consider adding this directory to PATH or, if you prefer to suppress this warning, use --no-warn-script-location.
  WARNING: The script nltk is installed in '/Users/alfu/.local/bin' which is not on PATH.
  Consider adding this directory to PATH or, if you prefer to suppress this warning, use --no-warn-script-location.
"""
import pandas as pd
import numpy as np

## LDA stuff
from nltk.corpus import stopwords
from nltk.stem.wordnet import WordNetLemmatizer
import string
import gensim
from gensim import corpora
import os, time, datetime


def preprocessTitle(single_ques_title):
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
    encoded_list = [unicode(x_, 'utf-8') for x_ in
                    splitted_title]  ## lsit of strings , unicode error resolution: https://stackoverflow.com/questions/21129020/how-to-fix-unicodedecodeerror-ascii-codec-cant-decode-byte/35444608
    # print encoded_list
    final_title = " ".join(lemma.lemmatize(word) for word in encoded_list)  ## one string
    # print final_title

    return final_title


def giveTimeStamp():
    tsObj = time.time()
    strToret = datetime.datetime.fromtimestamp(tsObj).strftime('%Y-%m-%d %H:%M:%S')
    return strToret


def modelTopics(list_of_titles, topic_cnt):
    print("Topic modeling started at:", giveTimeStamp())
    clean_title_list = [preprocessTitle(single_title).split() for single_title in list_of_titles]
    # print clean_title_list ## list of lists , each sublist is a list of strings incluided in the title 
    dictionary = corpora.Dictionary(clean_title_list)
    doc_term_matrix = [dictionary.doc2bow(doc_) for doc_ in clean_title_list]
    # print type(doc_term_matrix)  ## list if lists , each sublist is a list if tuples 
    # print doc_term_matrix
    Lda = gensim.models.ldamodel.LdaModel
    the_ldamodel = Lda(doc_term_matrix, num_topics=topic_cnt, id2word=dictionary, passes=50)
    print(the_ldamodel.print_topics(num_topics=topic_cnt, num_words=7))
    perp_lda = the_ldamodel.log_perplexity(
        doc_term_matrix)  ##reff for negative perplexity: https://groups.google.com/forum/#!topic/gensim/uQxQiR2oC98, higher is better
    print("Topic modeling ended at:", giveTimeStamp())
    return perp_lda


def constructQuestionDataset(answer_df, raw_ans_df_, ques_df, out_fil):
    at_least_one_postID_list = np.unique(answer_df[answer_df['INSECURE_SNIPPET_CNT'] > 0]['ID'].tolist())
    none_postID_list = np.unique(answer_df[answer_df['INSECURE_SNIPPET_CNT'] <= 0]['ID'].tolist())

    at_least_one_ans_df = raw_ans_df_[raw_ans_df_['Id'].isin(at_least_one_postID_list)]
    none_ans_df = raw_ans_df_[raw_ans_df_['Id'].isin(none_postID_list)]

    at_least_one_ques_list = np.unique(at_least_one_ans_df['ParentId'].tolist())
    none_ques_list = np.unique(none_ans_df['ParentId'].tolist())
    print('Questions with at least one insecure answer: {}, no insecure answer:{}'.format(len(at_least_one_ques_list),
                                                                                          len(none_ques_list)))
    print('=' * 100)
    at_least_one_ques_df = ques_df[ques_df['Id'].isin(at_least_one_ques_list)]
    none_ques_df = ques_df[ques_df['Id'].isin(none_ques_list)]

    at_least_one_ques_title = at_least_one_ques_df['Title'].tolist()
    none_ques_title = none_ques_df['Title'].tolist()

    # topic_nums = [2, 3, 4]
    perp_list = []
    # topic_nums = [(x_ + 1)*5 for x_ in xrange(20)]
    # print topic_nums
    topic_num = 5
    print('At least one insecure answer ...')
    modelTopics(at_least_one_ques_title, topic_num)
    print('=' * 100)
    print('No insecure answers ...')
    modelTopics(none_ques_title, topic_num)
    print('=' * 100)
    # for topic_num in topic_nums:
    #     # topic_model_perp = modelTopics(at_least_one_ques_title, topic_num)    
    #     topic_model_perp        = modelTopics(none_ques_title, topic_num)
    #     print 'For topic count:{}, perplexity score:{}'.format(topic_num, topic_model_perp)
    #     perp_list.append((topic_num, abs(topic_model_perp)))
    #     print '='*50
    # perp_df_ = pd.DataFrame(perp_list, columns=['Count', 'Perplexity'])
    # # perp_df_.to_csv('PERP_INSECURE_TM.csv')
    # perp_df_.to_csv('PERP_NON_INSECURE_TM.csv')


if __name__ == '__main__':
    answer_file = 'IDS_SO_GH_PY_ANS_RES.csv'
    answer_df = pd.read_csv(answer_file)

    raw_ans_fil = 'SO_GH_PYTHON_ANS_DETAILS.csv'
    raw_ans_df = pd.read_csv(raw_ans_fil)

    ques_fil = 'SO_GH_PY_QUES_DETAILS.csv'
    ques_df = pd.read_csv(ques_fil)

    constructQuestionDataset(answer_df, raw_ans_df, ques_df,
                             '/Users/akond/Documents/AkondOneDrive/MSR-MiningChallenge/msr19/output/QUES_DF')
