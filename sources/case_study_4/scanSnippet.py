'''
Akond Rahman 
Oct 13, 2018 
Scan code snippet 
'''
import pandas as pd
import re
import pickle

regex_string = r'code>.*</code'

'''
##reff: https://docs.openstack.org/bandit/1.4.0/blacklists/blacklist_calls.html

func_list = [ 'pickle.loads', 'pickle.load', 'pickle.Unpickler', 'cPickle.loads', 'cPickle.load', 'cPickle.Unpickler', 
'marshal.loads', 'marshal.load', 'hashlib.md5', 'cryptography.hazmat.primitives .hashes.MD5', 'Crypto.Hash.MD2.new', 
'Crypto.Hash.MD4.new', 'Crypto.Hash.MD5.new', 'Crypto.Cipher.ARC2.new', 'Crypto.Cipher.ARC4.new', 'Crypto.Cipher.Blowfish.new',   
'Crypto.Cipher.DES.new', 'Crypto.Cipher.XOR.new', 'cryptography.hazmat.primitives .ciphers.algorithms.ARC4', 'cryptography.hazmat.primitives .ciphers.algorithms.Blowfish', 
'cryptography.hazmat.primitives .ciphers.algorithms.IDEA', 'cryptography.hazmat.primitives .ciphers.modes.ECB', 
'tempfile.mktemp', 'eval', 'django.utils.safestring.mark_safe', 'httplib.HTTPSConnection', 'http.client.HTTPSConnection', 
'six.moves.http_client.HTTPSConnection', 'urllib.urlopen', 'urllib.request.urlopen', 'urllib.urlretrieve', 'urllib.request.urlretrieve',
'urllib.URLopener', 'urllib.request.URLopener', 'urllib.FancyURLopener', 'urllib.request.FancyURLopener', 'urllib2.urlopen',
'urllib2.Request', 'six.moves.urllib.request.urlopen', 'six.moves.urllib.request.urlretrieve', 'six.moves.urllib.request .URLopener', 
'six.moves.urllib.request.FancyURLopener', 'random.random', 'random.randrange', 'random.randint', 'random.choice', 'random.uniform', 
'random.triangular', 'telnetlib.*', 'xml.etree.cElementTree.parse', 'xml.etree.cElementTree.iterparse', 'xml.etree.cElementTree.fromstring', 
'xml.etree.cElementTree.XMLParser', 'xml.etree.ElementTree.parse', 'xml.etree.ElementTree.iterparse', 'xml.etree.ElementTree.fromstring', 
'xml.etree.ElementTree.XMLParser', 'xml.sax.expatreader.create_parser', 'xml.dom.expatbuilder.parse', 'xml.dom.expatbuilder.parseString',
'xml.sax.parse', 'xml.sax.parseString', 'xml.sax.make_parser', 'xml.dom.minidom.parse', 'xml.dom.minidom.parseString', 
'xml.dom.pulldom.parse', 'xml.dom.pulldom.parseString', 'lxml.etree.parse', 'lxml.etree.fromstring', 'lxml.etree.RestrictedElement', 
'xml.etree.GlobalParserTLS', 'lxml.etree.getDefaultParser', 'lxml.etree.check_docinfo', 'ftplib.*', 'input'
]
'''

# pickle_list  = ['pickle.loads', 'pickle.load', 'pickle.Unpickler', 'cPickle.loads', 'cPickle.load', 'cPickle.Unpickler']
# marshal_list = ['marshal.loads', 'marshal.load']
# wrong_hash_list = ['hashlib.md5', 'cryptography.hazmat.primitives .hashes.MD5', 'Crypto.Hash.MD2.new', 'Crypto.Hash.MD4.new', 'Crypto.Hash.MD5.new']
# insecure_cipher_list = ['Crypto.Cipher.ARC2.new', 'Crypto.Cipher.ARC4.new', 'Crypto.Cipher.Blowfish.new',   
# 'Crypto.Cipher.DES.new', 'Crypto.Cipher.XOR.new', 'cryptography.hazmat.primitives.ciphers.algorithms.ARC4', 'cryptography.hazmat.primitives.ciphers.algorithms.Blowfish', 
# 'cryptography.hazmat.primitives.ciphers.algorithms.IDEA', 'cryptography.hazmat.primitives .ciphers.modes.ECB']
# xss_scripting_list =  ['django.utils.safestring.mark_safe']
# insecure_func_list = ['tempfile.mktemp', 'eval', 'httplib.HTTPSConnection', 'http.client.HTTPSConnection', 'six.moves.http_client.HTTPSConnection', 'telnetlib.*', 'input', 'mktemp']
# url_list = ['urllib.urlopen', 'urllib.request.urlopen', 'urllib.urlretrieve', 'urllib.request.urlretrieve',
# 'urllib.URLopener', 'urllib.request.URLopener', 'urllib.FancyURLopener', 'urllib.request.FancyURLopener', 'urllib2.urlopen',
# 'urllib2.Request', 'six.moves.urllib.request.urlopen', 'six.moves.urllib.request.urlretrieve', 'six.moves.urllib.request .URLopener', 
# 'six.moves.urllib.request.FancyURLopener', 'urlopen', 'urlretrieve']
# random_list = ['random.random', 'random.randrange', 'random.randint', 'random.choice', 'random.uniform', 'random.triangular']
# xml_list = ['xml.etree.cElementTree.parse', 'xml.etree.cElementTree.iterparse', 'xml.etree.cElementTree.fromstring', 
# 'xml.etree.cElementTree.XMLParser', 'xml.etree.ElementTree.parse', 'xml.etree.ElementTree.iterparse', 'xml.etree.ElementTree.fromstring', 
# 'xml.etree.ElementTree.XMLParser', 'xml.sax.expatreader.create_parser', 'xml.dom.expatbuilder.parse', 'xml.dom.expatbuilder.parseString',
# 'xml.sax.parse', 'xml.sax.parseString', 'xml.sax.make_parser', 'xml.dom.minidom.parse', 'xml.dom.minidom.parseString', 
# 'xml.dom.pulldom.parse', 'xml.dom.pulldom.parseString', 'lxml.etree.parse', 'lxml.etree.fromstring', 'lxml.etree.RestrictedElement', 
# 'xml.etree.GlobalParserTLS', 'lxml.etree.getDefaultParser', 'lxml.etree.check_docinfo']
# ftp_list = ['ftplib.*']

data_parsing = ['pickle.loads', 'pickle.load', 'pickle.Unpickler', 'cPickle.loads', 'cPickle.load', 'cPickle.Unpickler',
                'marshal.loads', 'marshal.load',
                'xml.etree.cElementTree.parse', 'xml.etree.cElementTree.iterparse', 'xml.etree.cElementTree.fromstring',
                'xml.etree.cElementTree.XMLParser', 'xml.etree.ElementTree.parse', 'xml.etree.ElementTree.iterparse',
                'xml.etree.ElementTree.fromstring',
                'xml.etree.ElementTree.XMLParser', 'xml.sax.expatreader.create_parser', 'xml.dom.expatbuilder.parse',
                'xml.dom.expatbuilder.parseString',
                'xml.sax.parse', 'xml.sax.parseString', 'xml.sax.make_parser', 'xml.dom.minidom.parse',
                'xml.dom.minidom.parseString',
                'xml.dom.pulldom.parse', 'xml.dom.pulldom.parseString', 'lxml.etree.parse', 'lxml.etree.fromstring',
                'lxml.etree.RestrictedElement',
                'xml.etree.GlobalParserTLS', 'lxml.etree.getDefaultParser', 'lxml.etree.check_docinfo'
                ]
cipher = ['hashlib.md5', 'cryptography.hazmat.primitives.hashes.MD5', 'Crypto.Hash.MD2.new', 'Crypto.Hash.MD4.new',
          'Crypto.Hash.MD5.new',
          'Crypto.Cipher.ARC2.new', 'Crypto.Cipher.ARC4.new', 'Crypto.Cipher.Blowfish.new', 'Crypto.Cipher.DES.new',
          'Crypto.Cipher.XOR.new',
          'cryptography.hazmat.primitives.ciphers.algorithms.ARC4',
          'cryptography.hazmat.primitives.ciphers.algorithms.Blowfish',
          'cryptography.hazmat.primitives.ciphers.algorithms.IDEA', 'cryptography.hazmat.primitives.ciphers.modes.ECB',
          'random.random', 'random.randrange', 'random.randint', 'random.choice', 'random.uniform', 'random.triangular'
          ]
xss_scripting = ['django.utils.safestring.mark_safe']
race_condition = ['mktemp', 'tempfile.mktemp']
cmd_injection = ['input', 'eval']
insecure_connection = ['httplib.HTTPSConnection', 'http.client.HTTPSConnection',
                       'six.moves.http_client.HTTPSConnection', 'telnetlib.*',
                       'urllib.urlopen', 'urllib.request.urlopen', 'urllib.urlretrieve', 'urllib.request.urlretrieve',
                       'urllib.URLopener', 'urllib.request.URLopener', 'urllib.FancyURLopener',
                       'urllib.request.FancyURLopener', 'urllib2.urlopen',
                       'urllib2.Request', 'six.moves.urllib.request.urlopen', 'six.moves.urllib.request.urlretrieve',
                       'six.moves.urllib.request .URLopener',
                       'six.moves.urllib.request.FancyURLopener', 'urlopen', 'urlretrieve', 'ftplib.*'
                       ]


def matchSecurityWords(code_str):
    # pkl_match_cnt, marshal_match_cnt, hash_match_cnt, cipher_match_cnt, xss_match_cnt, func_match_cnt, url_match_cnt, rand_match_cnt, xml_match_cnt, ftp_match_cnt = 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 
    data_parse_cnt, cip_cnt, xss_cnt, race_cnt, cmd_cnt, conn_cnt = 0, 0, 0, 0, 0, 0

    data_parse_list = [kw for kw in data_parsing if kw in code_str]
    data_parse_cnt = len(data_parse_list)

    cip_list = [kw for kw in cipher if kw in code_str]
    cip_cnt = len(cip_list)

    xss_list = [kw for kw in xss_scripting if kw in code_str]
    xss_cnt = len(xss_list)

    race_list = [kw for kw in race_condition if kw in code_str]
    race_cnt = len(race_list)

    cmd_list = [kw for kw in cmd_injection if kw in code_str]
    cmd_cnt = len(cmd_list)

    conn_list = [kw for kw in insecure_connection if kw in code_str]
    conn_cnt = len(conn_list)

    return data_parse_cnt, cip_cnt, xss_cnt, race_cnt, cmd_cnt, conn_cnt


def matchCode(body_para):
    snippets = 0
    # t_pkl_, t_marshal_, t_hash_, t_cipher_, t_xss_, t_func_, t_url_, t_rand_, t_xml_, t_ftp_ = 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 
    t_data_parse_cnt, t_cip_cnt, t_xss_cnt, t_race_cnt, t_cmd_cnt, t_conn_cnt = 0, 0, 0, 0, 0, 0
    matches_ = re.findall(regex_string, body_para)
    snippets = snippets + len(matches_)
    for match in matches_:
        internal_matches = re.findall(regex_string, match)
        snippets = snippets + len(internal_matches)
        for a_match in internal_matches:
            data_parse_cnt, cip_cnt, xss_cnt, race_cnt, cmd_cnt, conn_cnt = matchSecurityWords(a_match)
            t_data_parse_cnt = t_data_parse_cnt + data_parse_cnt
            t_cip_cnt = t_cip_cnt + cip_cnt
            t_xss_cnt = t_xss_cnt + xss_cnt
            t_race_cnt = t_race_cnt + race_cnt
            t_cmd_cnt = t_cmd_cnt + cmd_cnt
            t_conn_cnt = t_conn_cnt + conn_cnt

    return [t_data_parse_cnt, t_cip_cnt, t_xss_cnt, t_race_cnt, t_cmd_cnt, t_conn_cnt, snippets]


def processBody(bodies, full_df):
    list_ = []
    cnt_ = 0
    for body in bodies:
        body_df = full_df[full_df['Body'] == body]
        body_ID = body_df['AnswerId'].tolist()[0]
        post_ID = body_df['QuestionId'].tolist()[0]
        createDate = body_df['CreationDate'].tolist()[0]
        score = body_df['Score'].tolist()[0]
        view = body_df['ViewCount'].tolist()[0]
        comment = body_df['CommentCount'].tolist()[0]
        favs = body_df['FavouriteCount'].tolist()[0]

        insecure_tup_with_snippets = matchCode(body)
        snip_cnt = insecure_tup_with_snippets.pop()
        insecure_tup = [x_ for x_ in insecure_tup_with_snippets]
        tot_insecurities = sum(insecure_tup)
        """print(body_ID)
        print(tot_insecurities, insecure_tup)
        print('{} left'.format(len(bodies) - cnt_))
        print('-' * 50)"""
        x_ = body_df['CreationDate'].tolist()[0]
        createDate = x_.split('-')[0] + '-' + x_.split('-')[1]

        list_.append((body_ID, 'TOTAL', tot_insecurities, post_ID, createDate, snip_cnt, score, view, comment, favs))
        list_.append((body_ID, 'DATAPARSE', insecure_tup[0], post_ID, createDate, snip_cnt, score, view, comment, favs))
        list_.append((body_ID, 'CIPHER', insecure_tup[1], post_ID, createDate, snip_cnt, score, view, comment, favs))
        list_.append((body_ID, 'XSS', insecure_tup[2], post_ID, createDate, snip_cnt, score, view, comment, favs))
        list_.append((body_ID, 'RACE', insecure_tup[3], post_ID, createDate, snip_cnt, score, view, comment, favs))
        list_.append((body_ID, 'CMDINJECT', insecure_tup[4], post_ID, createDate, snip_cnt, score, view, comment, favs))
        list_.append(
            (body_ID, 'INSECURECONN', insecure_tup[5], post_ID, createDate, snip_cnt, score, view, comment, favs))

        cnt_ += 1
    df_ = pd.DataFrame(list_,
                       columns=['BODY_ID', 'TYPE', 'TYPE_COUNT', 'PARENT_ID', 'MONTH', 'SNIPPET_CNT', 'SCORE', 'VIEW',
                                'COMMENT', 'FAVORITE'])
    return df_


if __name__ == '__main__':
    """ans_dat = 'SO_GH_PYTHON_ANS_DETAILS.csv'
    out_fil = 'DF_SO_GH_PY_ANS_DETAILS.PKL'

    ans_dat = 'SO_GH_PY_ACC_ANS_DETAILS.csv'
    out_fil = 'DF_SO_GH_PY_ACC_ANS_DETAILS.PKL'"""

    data_file = 'data/data.csv'
    ans_df_ = pd.read_csv(data_file, delimiter='\t')
    bodies = ans_df_['Body'].tolist()

    output_df = processBody(bodies, ans_df_)
    output_df.to_csv('data/scan_output.csv', index=False, header=True)
    print('DONE!!!')
    # print(full_data_df.head())


    # pickle.dump(full_data_df, open(out_fil, 'wb'))
