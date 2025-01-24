'''
Compare reputation
Akond Rahman 
Oct 17 2018 
'''
from scipy import stats
import pandas as pd
import numpy as np
import cliffsDelta
from datetime import date
import os


def dumpContentIntoFile(strP, fileP):
    fileToWrite = open(fileP, 'w')
    fileToWrite.write(strP)
    fileToWrite.close()
    return str(os.stat(fileP).st_size)


def compareDists(ls_1, ls_2, type_):
    print("Reputation metric:", type_)
    print("List#1 values [MEDIAN]:{}, [MEAN]:{}, [COUNT]:{}".format(np.median(list(ls_1)), np.mean(list(ls_1)),
                                                                    len(ls_1)))
    print("List#2 values [MEDIAN]:{}, [MEAN]:{}, [COUNT]:{}".format(np.median(list(ls_2)), np.mean(list(ls_2)),
                                                                    len(ls_2)))
    try:
        TS, p = stats.mannwhitneyu(list(ls_1), list(ls_2), alternative='greater')
    except ValueError:
        TS, p = 0.01, 0.95
    cliffs_delta = cliffsDelta.cliffsDelta(list(ls_1), list(ls_2))
    print('*' * 25)
    print('Pee value:{}, cliffs:{}, type:{}'.format(p, cliffs_delta, type_))
    print('*' * 25)


def getDurationInSO(end_, start_):
    end_dat = end_.split(' ')[0]
    sta_dat = start_.split(' ')[0]

    end_date = end_dat.split('-')
    end_date = [int(x_) for x_ in end_date]
    sta_date = sta_dat.split('-')
    sta_date = [int(x_) for x_ in sta_date]

    d0 = date(sta_date[0], sta_date[1], sta_date[2])
    d1 = date(end_date[0], end_date[1], end_date[2])
    delta_ = d1 - d0
    # print   delta_.days 
    # return  delta_.days 
    return delta_.days / 30


def getNormalizedRepu(user_data_df, repu_type):
    # print user_data_df
    r_, c_ = user_data_df.shape
    if r_ > 0:
        repu = user_data_df[repu_type].tolist()[0]
        start = user_data_df['CreateDate'].tolist()[0]
        end = user_data_df['LastAccDate'].tolist()[0]

        dura_days = getDurationInSO(end, start)

        if dura_days < 1:
            dura_days = 1

        norm_repu = float(repu) / float(dura_days)
    else:
        repu = 1
        dura_days = 1
        norm_repu = float(1)
    # print repu, dura_days, norm_repu
    return norm_repu


def dumpReputation(insecure, neutral):
    str_ = ''
    for elem in insecure:
        str_ = str_ + str(elem) + ',' + '\n'
    dumpContentIntoFile(str_, 'FOR_PAPER_INSECURE_ANSWER.csv')

    str_ = ''
    for elem in neutral:
        str_ = str_ + str(elem) + ',' + '\n'
    dumpContentIntoFile(str_, 'FOR_PAPER_NEUTRAL_ANSWER.csv')


def compareReputation(user_df, user_post_df, answer_df, repu_typ):
    filtered_user_post_df = user_post_df[user_post_df['UserID'] != 0]

    total_df = answer_df[answer_df['TYPE'] == 'TOTAL']

    at_least_one_postID_list = np.unique(total_df[total_df['INSECURE_SNIPPET_CNT'] > 0]['ID'].tolist())
    none_postID_list = np.unique(total_df[total_df['INSECURE_SNIPPET_CNT'] <= 0]['ID'].tolist())

    '''
    first get user IDs
    '''

    none_user_IDs, atleast_one_user_IDs = [], []

    for postID in none_postID_list:
        userIDList = filtered_user_post_df[filtered_user_post_df['PostID'] == postID]['UserID'].tolist()
        if len(userIDList) > 0:
            userID = userIDList[0]
            none_user_IDs.append(userID)

    for postID in at_least_one_postID_list:
        userIDList = filtered_user_post_df[filtered_user_post_df['PostID'] == postID]['UserID'].tolist()
        if (len(userIDList) > 0):
            userID = userIDList[0]
            atleast_one_user_IDs.append(userID)

    none_user_IDs = np.unique(none_user_IDs)
    atleast_one_user_IDs = np.unique(atleast_one_user_IDs)
    '''
    then get reputations 
    '''
    none_user_repu, atleast_one_user_repu = [], []

    for userID in none_user_IDs:
        userID_DF = user_df[user_df['AccountID'] == userID]
        norm_repu = getNormalizedRepu(userID_DF, repu_typ)
        none_user_repu.append(norm_repu)

    for userID in atleast_one_user_IDs:
        userID_DF = user_df[user_df['AccountID'] == userID]
        norm_repu = getNormalizedRepu(userID_DF, repu_typ)
        atleast_one_user_repu.append(norm_repu)
    '''
    now compare
    '''
    compareDists(atleast_one_user_repu, none_user_repu, repu_typ)
    dumpReputation(atleast_one_user_repu, none_user_repu)


if __name__ == '__main__':
    user_file_ = 'SO_PY_ANS_USERS.csv'
    user_df_ = pd.read_csv(user_file_)

    user_post_file = 'SO_PY_ANS_USERID.csv'
    user_post_df = pd.read_csv(user_post_file)

    answer_file = 'IDS_SO_GH_PY_ANS_RES.csv'
    answer_df = pd.read_csv(answer_file)

    compareReputation(user_df_, user_post_df, answer_df, 'Reputation')  # Reputation,Ups,Downs,Views
