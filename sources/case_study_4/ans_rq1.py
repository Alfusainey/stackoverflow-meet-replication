'''
Akond Rahman 
Oct 30 2018 
Answer to RQ1 
'''
import pandas as pd 
import numpy as np 


def getAllSnippetCount(df_p):
    IDs = np.unique(df_p['ID'].tolist())
    sum_ = 0 
    for ID_ in IDs : 
        ID_df = df_p[df_p['ID']==ID_] 
        ID_snippet_count = np.unique(ID_df['TOT_SNIP'].tolist())[0]
        sum_ = sum_ + ID_snippet_count
    return sum_ 


def getRawOccurences(df_p):
    IDs = np.unique(df_p['ID'].tolist())    
    tot_snip_cnt = getAllSnippetCount(df_p)
    print('Total answers:', len(IDs))
    types = np.unique(df_p['TYPE'].tolist())
    for type_ in types:
        type_df = df_p[df_p['TYPE']==type_]
        smelly_df = type_df[type_df['INSECURE_SNIPPET_CNT'] > 0 ]
        # Part # 1
        ans_IDs = np.unique( smelly_df['ID'].tolist() )
        print('Category:{}, raw count:{}, proportion:{}'.format(type_, len(ans_IDs),
                                                                (float(len(ans_IDs)) / float(len(IDs))) * 100))
        # Part # 2
        smelly_snippet_count = sum(smelly_df['INSECURE_SNIPPET_CNT'].tolist())
        print('Category:{}, snippet count:{}, proportion:{}'.format(type_, smelly_snippet_count, (
                    float(smelly_snippet_count) / float(tot_snip_cnt)) * 100))
        print('-' * 50)


def getQuestionOccurrences(ans_df, raw_ans_df_, q_df):
    types = np.unique(ans_df['TYPE'].tolist())    
    total_q_cnt = len( np.unique( q_df['Id'].tolist()) )
    print('Total question count:', total_q_cnt)
    for type_ in types:
        type_df = ans_df[ans_df['TYPE']==type_]

        at_least_one_postID_list = np.unique( type_df[type_df['INSECURE_SNIPPET_CNT'] > 0 ]['ID'].tolist() )
        none_postID_list         = np.unique( type_df[type_df['INSECURE_SNIPPET_CNT'] <= 0 ]['ID'].tolist() )

        at_least_one_ans_df = raw_ans_df_[raw_ans_df_['Id'].isin( at_least_one_postID_list )]
        none_ans_df         = raw_ans_df_[raw_ans_df_['Id'].isin( none_postID_list )]    

        at_least_one_ques_list = list( np.unique( at_least_one_ans_df['ParentId'].tolist() ) )
        none_ques_list         = list( np.unique( none_ans_df['ParentId'].tolist() ) )

        print('Category:{}, smelly question count:{}, proportion:{}'.format(type_, len(at_least_one_ques_list), (
                    float(len(at_least_one_ques_list)) / float(total_q_cnt)) * 100))
        print('-' * 50)


if __name__=='__main__':
   the_fil = 'IDS_SO_GH_PY_ANS_RES.csv'
   the_df  = pd.read_csv(the_fil)

   raw_ans_fil = 'SO_GH_PYTHON_ANS_DETAILS.csv'
   raw_ans_df  = pd.read_csv(raw_ans_fil)

   ques_fil = 'SO_GH_PY_QUES_DETAILS.csv'
   ques_df  = pd.read_csv(ques_fil)

   '''
   Add accepted answer data: get raw occurrences 
   '''
   # the_fil = 'IDS_SO_GH_PY_ACC_ANS_RES.csv'
   # the_df  = pd.read_csv(the_fil)

   print('All snippets count:', getAllSnippetCount(the_df))
   print('Occurrences count')
   print('=' * 100)
   getRawOccurences(the_df)
   print('=' * 100)

   getQuestionOccurrences(the_df, raw_ans_df, ques_df)
   print('=' * 100)
