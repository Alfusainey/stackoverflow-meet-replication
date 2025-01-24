"""This script implements unsafe queries and are prone to SQL injection
   attacks. To fix, implement a run_query() and that take arguments to pass
   to the underlying execute(...) call.
"""

from sotorrent import SOTorrentDB


class QueryService():
    """
       A service for querying data stored in the SOTorrent DB.
    """
    def __init__(self):
        # client for connecting to the SOTorrent database.
        # an explicit call to connect() MUST be done to establish a connection to the DB.
        self._client = None

    @property
    def client(self):
        return self._client
    @client.setter
    def client(self, client):
        self._client = client

    def connect(self, host='127.0.0.1', port=3306, db_name='sotorrent22'):
        """Connects to the MySQL server at host:port using the given database name.
        """
        if self.client is None:
            self.client = SOTorrentDB(host=host, port=port, db=db_name)

    def execute(self, query):
        return self.client.run_query(query)

    def execute_and_fetchall(self, query) -> tuple:
        return self.execute(query).fetchall()

    def execute_and_fetchone(self, query) -> dict:
        '''Executes the given query and returns a dictionary containing a single row of a DB table.
        '''
        return self.execute(query).fetchone()

    def execute_insert_and_commit(self, query : str, rows_to_insert : list) -> int:
        """
        Executes an INSERT operation and commit the results to the Database.

        Args:
            query: The insert SQL query.
            rows_to_insert: The rows to insert. This is a list of tuple entries, each entry corresponding to a DB column.
        Returns:
            The number of inserted rows.
        """
        inserted_row_count = self.client.cursor.executemany(query, rows_to_insert)
        self.commit()
        return inserted_row_count

    def execute_update_and_commit(self, query) -> None:
        self.execute(query)
        self.commit()

    def execute_delete_and_commit(self, query) -> None:
        self.execute(query)
        self.commit()

    def commit(self):
        """
        Commit the last DB transaction.
        """
        self.client.db.commit()

    def close(self):
        if self.client:
            self.client.close()
            self.client = None

    def getLatestVersion(self, rootid):
        '''Gets the last snippet version ID for the version chain of the specified rootid
        '''
        query_str = f"""SELECT Id
                       FROM PostBlockVersion
                       WHERE RootPostBlockVersionId={rootid} AND
                             (PredEqual IS NULL OR PredEqual = 0)
                       ORDER BY PostHistoryId ASC
        """
        cursor = self.client.run_query(query_str)
        version_ids = [row['Id'] for row in cursor]
        return version_ids[-1]

    def get_commit_date(self, repo_name, repo_file, commit):
        query = f"""SELECT CommitDate
                    FROM CloneFileCommits
                    WHERE RepoName='{repo_name}' AND
                    RepoFile='{repo_file}' AND
                    Commit='{commit}'"""
        ret = self.execute_and_fetchone(query)
        if ret:
            return ret['CommitDate']
        return None

    def getCreationDateOfRecentVersion(self, postid):
        """Gets the creation date of the most recent version of the given post.

           Assumes that a connection to the SOTorrent database is already established.
        """
        query_str = """SELECT CreationDate FROM sotorrent19_03.PostVersion 
                       WHERE PostId={} AND MostRecentVersion=true
        """.format(postid)
        iter_cursor = self.client.run_query(query_str)
        if iter_cursor.rowcount > 0:
            return iter_cursor.fetchone()['CreationDate']
        return None

    def getPostCreationDate(self, postid):
        query_str = f"""SELECT CreationDate
                        FROM   Posts
                        WHERE  Id={postid}
                    """
        cursor = self.client.run_query(query_str)
        return cursor.fetchone()['CreationDate']

    def queryCodeBlockIdandContent(self):
        query_str = """SELECT Id, Content FROM CodeBlockVersion"""
        iter_cursor = self.client.run_query(query_str)
        if iter_cursor.rowcount > 0:
            return iter_cursor
        return []

    def queryCodeBlockContent(self, snippetid):
        query_str = f"SELECT Content FROM PostBlockVersion WHERE Id={snippetid}"
        iter_cursor = self.client.run_query(query_str)
        if iter_cursor.rowcount > 0:
            return iter_cursor.fetchone()['Content']
        return None

    def queryNullLanguageCodeBlocks(self):
        query_str = """SELECT Id FROM CodeBlockVersionNew WHERE Language IS NULL"""
        cursor = self.client.run_query(query_str)
        return cursor.fetchall()

    def getMostRecentCodeBlocks(self):
        query_str = """SELECT Id, PostId, RootPostBlockVersionId, Content 
                       FROM   PostBlockVersion
                       WHERE  MostRecentVersion=True AND 
                              PostBlockTypeId=2 AND 
                              LineCount >= 5
                       """
        cursor = self.client.run_query(query_str)
        return cursor.fetchall()

    def get_descriptions(self, postHistoryid):
        query_str = f"""SELECT Content 
                       FROM PostBlockVersion 
                       WHERE PostBlockTypeId=1 AND PostHistoryId={postHistoryid}
                    """
        cursor = self.client.run_query(query_str)
        return [row['Content'] for row in cursor.fetchall()]

    def getCodeBlockVersionLanguage(self, id_key):
        query_str = """SELECT Language FROM CodeBlockVersion WHERE Id={}""".format(id_key)
        iter_cursor = self.client.run_query(query_str)
        return iter_cursor.fetchone()['Language']

    def update_CodeBlockVersionlanguage(self, id_key, language):
        if not self.getCodeBlockVersionLanguage(id_key):
            query_str="""UPDATE CodeBlockVersion SET Language='{}' WHERE Id={}""".format(language, id_key)
            iter_cursor = self.client.run_query(query_str)
            self.client.db.commit()
            if iter_cursor.rowcount == 1:
                # only single row should be updated
                return True
            return False

    def update_CodeBlockVersion_RootPostBlockVersionId(self, rowid, rootid):
        query_str= f"""UPDATE CodeBlockVersion SET RootPostBlockVersionId={rootid} WHERE Id={rowid}"""
        self.client.run_query(query_str)
        self.client.db.commit()
        
    def insertIntoCodeBlockVersion(self, rows_to_insert):
        self.client.cursor.executemany(
            """INSERT INTO CodeBlockVersion(VersionId, Language)
            VALUES (%s, %s)
            """, rows_to_insert)
        self.client.db.commit()

    def queryCodeBlockSnippets(self, language):
        query_str = """SELECT Id, VersionId, Content FROM CodeBlockVersion WHERE Language='{}' LIMIT 100""".format(language)
        iter_cursor = self.client.run_query(query_str)
        if iter_cursor.rowcount > 0:
            return iter_cursor
        return []

    def getInsecureCommentCount(self, labelid):
        query_str = """SELECT PostId, PostType, CommentCount FROM LabeledCodeExamples WHERE LabelId={} AND CommentCount > 0""".format(labelid)
        return self.client.run_query(query_str)

    def getCodeBlockVersionForPost(self, postid):
        query_str = """SELECT VersionId, Content FROM CodeBlockVersion WHERE PostId={}""".format(postid)
        return self.client.run_query(query_str)
    
    def getAllCodeSnippets(self):
        query_str = """SELECT VersionId, PostId, Content FROM CodeBlockVersion
        """
        iter_cursor = self.client.run_query(query_str)
        return iter_cursor

    def getCodeSnippetsByLanguage_deprecated(self, language):
        query_str = """SELECT VersionId, PostId, Content FROM CodeBlockVersion WHERE Language='{}'""".format(language)
        iter_cursor = self.client.run_query(query_str)
        return iter_cursor

    def get_root_postblockversion_ids(self, database_table_or_view, language='Java', limit=None, is_view=False):
        if limit:
            query_str = f"""SELECT RootPostBlockVersionId
                       FROM {database_table_or_view}
                       WHERE Language='{language}'
                       GROUP BY RootPostBlockVersionId
                       LIMIT {limit}"""
        elif is_view:
            query_str = f"""SELECT DISTINCT(RootPostBlockVersionId) as RootPostBlockVersionId
                                       FROM {database_table_or_view}"""

        else:
            query_str = f"""SELECT RootPostBlockVersionId
                           FROM {database_table_or_view}
                           WHERE Language='{language}'
                           GROUP BY RootPostBlockVersionId
                         """
        cursor = self.client.run_query(query_str)
        return cursor.fetchall()

    def get_view_snippets_count(self, rootid, databaseView='JavaCodeSnippets'):
        """Get the number of code snippets >=10 LoC rooted at the given rootid.
        """
        query_str = f"""SELECT COUNT(DISTINCT(SnippetId)) as count
                       FROM  View_{databaseView}
                       WHERE RootId={rootid}
        """
        cursor = self.client.run_query(query_str)
        return cursor.fetchone()['count']

    def get_versions(self, post_id) -> dict:
        query = f"SELECT Id, CreationDate FROM PostVersion WHERE PostId={post_id} ORDER BY CreationDate"
        records = self.execute_and_fetchall(query)
        r = {}
        for row in records:
            r[row['Id']] = row['CreationDate']
        return r

    def get_view_snippet_ids(self, rootid, databaseView='JavaCodeSnippets'):
        query_str = f"""SELECT SnippetId
                        FROM  View_{databaseView}
                        WHERE RootId={rootid}
        """
        cursor = self.client.run_query(query_str)
        return cursor.fetchall()

    def get_view_snippets_content(self, rootid, databaseView='JavaCodeSnippets'):
        query_str = f"""SELECT Id, Content
                        FROM PostBlockVersion pbv
                        INNER JOIN View_JavaCodeSnippets jview
                        ON pbv.Id=jview.SnippetId
                       WHERE jview.RootId={rootid}
        """
        cursor = self.client.run_query(query_str)
        return cursor.fetchone()['count']

    def getMostRecentVersion(self, rootid):
        # Get the most recent code snippet version in this version chain identified
        # by the given rootId

        query_str = f"SELECT Id FROM PostBlockVersion WHERE RootPostBlockVersionId={rootid} AND MostRecentVersion=True"
        cursor = self.client.run_query(query_str)
        return cursor.fetchone()['Id']

    def getPostBlockContent(self, snippetid):
        query_str = f"SELECT Content FROM PostBlockVersion WHERE Id={snippetid}"
        cursor = self.client.run_query(query_str)
        return cursor.fetchone()['Content']

    def queryclones(self, reponame, repofile, startline, endline, rootid):
        query_str = f"""SELECT SnippetId, Similarity
                       FROM CloneResultsTMP
                       WHERE RepoName='{reponame}' AND RepoFile='{repofile}'
                             AND StartLine={startline} AND EndLine={endline}
                             AND RootId={rootid}
        """
        cursor = self.client.run_query(query_str)
        return cursor.fetchall()

    def get_codesnippets_postBlockVersionIds(self, language):
        query_str = f"""SELECT Id, PostBlockVersionId
                       FROM CodeBlockVersion
                       WHERE Language='{language}'
        """
        cursor = self.client.run_query(query_str)
        return cursor.fetchall()
    
    def hasSnippets(self, postid):
        query_str = """SELECT Id FROM CodeBlockVersion WHERE PostId={} LIMIT 1""".format(postid)
        iter_cursor = self.client.run_query(query_str)
        if iter_cursor.rowcount > 0:
            return True
        return False

    def get_outdated_clones(self, limit=False):
        if limit:
            query_str = '''SELECT * FROM OutdatedClones WHERE IsOutdated=True LIMIT 1'''
        else:
            query_str = '''SELECT * FROM OutdatedClones WHERE IsOutdated=True'''
        cursor = self.client.run_query(query_str)
        return cursor.fetchall()

    def getposttype(self, postid):
        query_str = """SELECT PostTypeId FROM Posts WHERE Id={}""".format(postid)
        iter_cursor = self.client.run_query(query_str)
        if iter_cursor.rowcount > 0:
            rdict = iter_cursor.fetchone()
            return rdict['PostTypeId']
        return None

    def getanswers(self, postid):
        """Returns a set of answers for the given question.
        """
        query_str = """SELECT Id FROM Posts WHERE ParentId={}""".format(postid)
        iter_cursor = self.client.run_query(query_str)
        rows = [row['Id'] for row in iter_cursor]
        return set(rows)

    def insertClones(self, rows_to_insert):
        query_str = """
        INSERT INTO ClonesCprojects(RepoName, Language, RootPostBlockVersionId, PostBlockVersionId, RepoFileStartLine, 
                        RepoFileEndLine, Similarity, RepoFile, SnippetFile, SnippetFileStartLine, SnippetFileEndLine)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        inserted_row_count = self.client.cursor.executemany(query_str, rows_to_insert)
        self.client.db.commit()
        return inserted_row_count

    def insertIntoCrossProductClonesFromClones(self, rows_to_insert):
        query_str = """
                INSERT INTO CrossProductClones( CloneReposId, RootPostBlockVersionId, PostBlockVersionId, 
                                                SoSnippet, Similarity, SnippetFile, SnippetFileStartLine, 
                                                SnippetFileEndLine)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """
        inserted_row_count = self.client.cursor.executemany(query_str, rows_to_insert)
        self.client.db.commit()
        return inserted_row_count

    def insertIntoCrossProductClones(self, rows_to_insert):
        query_str = """
        INSERT INTO CrossProductClones(RepoName, Language, RootPostBlockVersionId, PostBlockVersionId, RepoFileStartLine, 
                        RepoFileEndLine, Similarity, RepoFile, SnippetFile, SnippetFileStartLine, SnippetFileEndLine, Commit)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        inserted_row_count = self.client.cursor.executemany(query_str, rows_to_insert)
        self.client.db.commit()
        return inserted_row_count

    def insertIntoCloneFileCommits(self, rows_to_insert):
        query_str = """INSERT INTO CloneFileCommits(RepoName, RepoFile, Commit, CommitDate, Language)
                       VALUES (%s, %s, %s, %s, %s)
        """
        inserted_row_count = self.client.cursor.executemany(query_str, rows_to_insert)
        self.client.db.commit()
        return inserted_row_count

    def insertTimelineAnalysis(self, rows_to_insert):
        query_str = """INSERT INTO TimelineAnalysis(CloneId, Category, Comment, IsSimilar, RepoSnippet, DiffSnippet)
                       VALUES (%s, %s, %s, %s, %s, %s)
                    """
        inserted_row_count = self.client.cursor.executemany(query_str, rows_to_insert)
        self.client.db.commit()
        return inserted_row_count

    def insertIntoCloneResults(self, rows_to_insert, query_str):
        """schema in rows_to_insert:
           reponame, rootid, language, snippetid, snippet, 
           startline, endline, similarity, repofile, snippetfile
        """
        self.client.cursor.executemany(query_str, rows_to_insert)
        self.client.db.commit()

    def insertIntoCloneCommits(self, rows_to_insert):
        query_str = """INSERT INTO CloneCommits(CrossProductClonesId, CloneReposId, FileCommit, LastModifiedCommit)
                       VALUES (%s, %s, %s, %s)
                    """
        self.client.cursor.executemany(query_str, rows_to_insert)
        self.client.db.commit()

    def insertIntoRxJava(self, rows_to_insert):
        query_str = """INSERT INTO RxJava(
                RepoName, RootId, Language, SnippetId, RepoSnippet, 
                StartLine, EndLine, Similarity, RepoFile, SnippetFile
                )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        self.client.cursor.executemany(query_str, rows_to_insert)
        self.client.db.commit()

    def insertIntoClonesPerCommitResult(self, rows_to_insert):
        """
        (repo.reponame, commit_hash, snippet.rootid, snippet.snippetid,
        repo.startline, repo.endline, cloneclass.similarity, repo.repofile)
        """
        self.client.cursor.executemany(
            """INSERT INTO ClonesPerCommitResult(
                RepoName, CommitHash, RootSnippetVersionId, SnippetVersionId, 
                StartLine, EndLine, Similarity, RepoFile, OriginalRepoFile)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, rows_to_insert)
        self.client.db.commit()

    def insertIntoCodeSnippetGamification(self, rows_to_insert):
        self.client.cursor.executemany(
            """INSERT INTO CodeSnippetGamification (SnippetId, Snippet, Language)
               VALUES (%s, %s, %s)
            """, rows_to_insert)
        self.client.db.commit()
    
    def getCodeSnippetsUpto(self, n, language):
        query_str = """SELECT VersionId, PostId, Content FROM CodeBlockVersion WHERE Language='{}' LIMIT {}""".format(language, n)
        iter_cursor = self.client.run_query(query_str)
        return iter_cursor

    def getPopularCloneSnippets(self, n):
        """Gets the most copied n code snippets from the CloneResults table.
        """
        query_str = "SELECT SnippetId, COUNT(*) as Count FROM CloneResults GROUP BY SnippetId ORDER BY Count DESC LIMIT {}".format(n)
        iter_cursor = self.client.run_query(query_str)
        return iter_cursor

    def getcloneSnippetIDs(self):
        """Selects all distinct copied code snippets.
        """
        query_str = "SELECT SnippetId FROM CloneResults GROUP BY SnippetId"
        iter_cursor = self.client.run_query(query_str)
        return iter_cursor

    def getSnippet(self, snippetid):
        query_str = "SELECT Content FROM CodeBlockVersion WHERE VersionId={}".format(snippetid)
        iter_cursor = self.client.run_query(query_str)
        rdict = iter_cursor.fetchone()
        return rdict['Content']

    def get_commitdate(self, cloneReposId):
        query_str = f"""SELECT CommitDate
                        FROM CloneCommits 
                        WHERE CloneReposId={cloneReposId}
                    """
        rdict = self.client.run_query(query_str).fetchone()
        return rdict['CommitDate']

    def getCopiedSnippetsWithComments(self):
        query_str = """SELECT ct.SnippetId AS SnippetId, ct.PostId AS PostId, comments.Id AS CommentId,
                              comments.Text AS CommentText
                              FROM CloneResults ct 
                              INNER JOIN Comments comments 
                              WHERE ct.PostId = comments.PostId"""

        iter_cursor = self.client.run_query(query_str)
        return iter_cursor

    def insertIntoCloneSnippetWithComments(self, rows_to_insert):
        self.client.cursor.executemany(
            """INSERT INTO CloneSnippetWithComments (SnippetId, PostId, CommentId, CommentText)
               VALUES (%s, %s, %s, %s)
            """, rows_to_insert)
        self.client.db.commit()

    def insertIntoCrawledTutorials(self, rows_to_insert):
        self.client.cursor.executemany(
            """INSERT INTO CrawledTutorials (Domain, Snippet, Url, PageTitle, Language, CreationDate)
               VALUES (%s, %s, %s, %s, %s, %s)
            """, rows_to_insert)
        self.client.db.commit()

    def insertIntoOutdatedClones(self, rows_to_insert):
        inserted_row_count  = self.client.cursor.executemany(
            """INSERT INTO OutdatedClones (CrossProductClonesId, CopiedPostBlockVersionId, LatestPostBlockVersionId, IsOutdated)
               VALUES (%s, %s, %s, %s)
            """, rows_to_insert)
        self.client.db.commit()
        return inserted_row_count

    def getAllEvaluationSnippetIds(self):
        query_str = """SELECT SnippetId FROM CodeSnippetGamification
        """
        return self.client.run_query(query_str)

    def getPostId(self, snippetid):
        query_str = """SELECT PostId FROM CloneResults WHERE SnippetId={} LIMIT 1
        """.format(snippetid)
        iter_cursor = self.client.run_query(query_str)
        rdict = iter_cursor.fetchone()
        return rdict['PostId']

    def get_parentId(self, answerid):
        '''Gets the question the specified answer belongs to.
        '''
        query_str = f"SELECT ParentId FROM Posts WHERE Id={answerid}"
        cursor = self.client.run_query(query_str)
        return cursor.fetchone()['ParentId']

    def get_accepted_answer(self, questionid):
        query_str = f"SELECT AcceptedAnswerId FROM Posts WHERE Id={questionid}"
        cursor = self.client.run_query(query_str)
        return cursor.fetchone()['AcceptedAnswerId']

    def get_pbv_postId(self, rowid):
        query_str = f"""SELECT PostId 
                       FROM PostBlockVersion 
                       WHERE Id={rowid}
                       GROUP BY PostId
        """
        iter_cursor = self.client.run_query(query_str)
        rdict = iter_cursor.fetchone()
        return rdict['PostId']
    
    def get_pbv_LocalId(self, postBlockVersionId):
        query_str = f"""SELECT LocalId 
                       FROM PostBlockVersion 
                       WHERE Id={postBlockVersionId}
                       GROUP BY LocalId
        """
        iter_cursor = self.client.run_query(query_str)
        rdict = iter_cursor.fetchone()
        return rdict['LocalId']
    
    def get_pbv_PostHistoryId(self, postBlockVersionId):
        query_str = f"""SELECT PostHistoryId 
                       FROM PostBlockVersion 
                       WHERE Id={postBlockVersionId}
                       GROUP BY PostHistoryId
        """
        iter_cursor = self.client.run_query(query_str)
        rdict = iter_cursor.fetchone()
        return rdict['PostHistoryId']
    
    def get_pbv_Content(self, postBlockVersionId):
        query_str = f"""SELECT Content 
                       FROM PostBlockVersion 
                       WHERE Id={postBlockVersionId}
        """
        iter_cursor = self.client.run_query(query_str)
        rdict = iter_cursor.fetchone()
        return rdict['Content']

    def get_pbv_rootId(self, postBlockVersionId):
        query_str = f"""SELECT RootPostBlockVersionId 
                       FROM PostBlockVersion 
                       WHERE Id={postBlockVersionId}
                       GROUP BY RootPostBlockVersionId
        """
        iter_cursor = self.client.run_query(query_str)
        rdict = iter_cursor.fetchone()
        return rdict['RootPostBlockVersionId']

    def updateClonesRootId(self, rowid, rootid):
        query_str = f"""UPDATE CloneResultsNew SET RootId={rootid} WHERE Id={rowid}
        """
        self.client.run_query(query_str)
        self.client.db.commit()

    def updateCrossProductClonesCommit(self, language, repo_name, repo_file, last_commit):
        query_str = f"""UPDATE CrossProductClones 
                        SET Commit='{last_commit}'
                        WHERE RepoName='{repo_name}' AND RepoFile='{repo_file}' AND Language='{language}'
        """
        self.client.run_query(query_str)
        self.client.db.commit()

    def updateCloneReposId(self, rowid, cloneReposId):
        query_str = f"""UPDATE Clones SET CloneReposId={cloneReposId} WHERE Id={rowid}
        """
        self.client.run_query(query_str)
        self.client.db.commit()

    def updateClonesPostId(self, rowid, postid):
        query_str = f"""UPDATE CloneResultsNew SET PostId={postid} WHERE Id={rowid}
        """
        self.client.run_query(query_str)
        self.client.db.commit()

    def get_clones(self, all_columns=False):
        if all_columns:
            query_str = "SELECT * FROM Clones"
        else:
            query_str = "SELECT * FROM Clones WHERE CloneReposId IS NULL"
        cursor = self.client.run_query(query_str)
        return cursor.fetchall()

    def get_CrossProductClonesId(self, clonereposId, rootPostBlockVersionId, postBlockVersionId, similarity):
        query_str = f"""SELECT Id
                       FROM CrossProductClones
                       WHERE CloneReposId={clonereposId} 
                             AND Similarity={similarity}
                             AND PostBlockVersionId={postBlockVersionId}
                             AND RootPostBlockVersionId={rootPostBlockVersionId}
        """
        cursor = self.client.run_query(query_str)
        return cursor.fetchone()['Id']

    def queryOutdatedClones(self, language='java'):
        query_str = f"""SELECT CrossProductClonesId 
                       FROM OutdatedClones outdated
                       INNER JOIN CrossProductClones cpc
                       ON outdated.CrossProductClonesId=cpc.Id
                       AND cpc.Language='{language}'
                       AND FirstRun=1
                       GROUP BY CrossProductClonesId"""
        cursor = self.client.run_query(query_str)
        return cursor.fetchall()

    def get_cross_product_clones(self, language, first_run=True):
        query_str = f"""SELECT * 
                        FROM CrossProductClones 
                        WHERE Language='{language}' AND FirstRun={first_run}
                    """
        cursor = self.client.run_query(query_str)
        return cursor.fetchall()

    def get_clones_dataset(self, similarity=83):
        query_str = f"""SELECT RepoName, RepoFile, StartLine, EndLine, Similarity, RootId, SnippetId
                        FROM CloneResultsNew
                        WHERE Similarity>={similarity}
                        GROUP BY RepoName, RepoFile, StartLine, EndLine, Similarity, RootId, SnippetId
        """
        cursor = self.client.run_query(query_str)
        return cursor.fetchall()

    def get_sample_clones(self):
        query_str = """SELECT Id, RootId, SnippetId FROM CloneResultsNew
        """
        cursor = self.client.run_query(query_str)
        return cursor
    
    def get_clone_postid(self, rootid):
        # TODO: remove method, not
        query_str = f"""SELECT PostId
                       FROM CloneResultsNew
                       WHERE RootId={rootid}
                       GROUP BY PostId
        """
        cursor = self.client.run_query(query_str)
        return cursor.fetchone()['PostId']

    def get_sample_clones_for_testing(self, similarity=83, limit=10):
        query_str = f""" SELECT RepoName, RepoFile, StartLine, EndLine
                         FROM CloneResultsNew
                         WHERE Similarity>={similarity}
                         GROUP BY RepoName, RepoFile, StartLine, EndLine
                         LIMIT {limit}
        """
        cursor = self.client.run_query(query_str)
        # cursor.fetchall() returns a tuple of dictionaries
        return [row for row in cursor.fetchall()]

    def get_sample_clones_for_testing_1(self, clonereposids, similarity=83):
        query_str = f""" SELECT Commit, CloneReposId, RootPostBlockVersionId, PostBlockVersionId, Similarity
                         FROM CrossProductClones
                         WHERE Similarity>={similarity} 
                               AND CloneReposId IN {clonereposids}
                               GROUP BY Commit, CloneReposId, RootPostBlockVersionId, PostBlockVersionId, Similarity
                         
        """
        cursor = self.client.run_query(query_str)
        return cursor.fetchall() # returns ({}, {}, {}, {})

    def get_clone_repos(self, language):
        '''returns a tuple of dictionaries.
        '''
        query_str = f"""SELECT RepoName, RepoFile 
                        FROM CrossProductClones 
                        WHERE Language='{language}'
                        GROUP BY RepoName, RepoFile"""
        return self.client.run_query(query_str).fetchall()

    def get_clones_for_analysis(self, language='java', similarity=83):
        query_str = f""" SELECT Commit, CloneReposId, RootPostBlockVersionId, PostBlockVersionId, Similarity
                         FROM CrossProductClones
                         WHERE Language='{language}' AND Similarity >= {similarity}
                         GROUP BY Commit, CloneReposId, RootPostBlockVersionId, PostBlockVersionId, Similarity
        """
        cursor = self.client.run_query(query_str)
        return cursor.fetchall() # returns ({}, {}, {}, {})

    def get_clones_rowid(self, cloneReposId, similarity, postblockversionId):
        query_str = f""" SELECT Id
                         FROM Clones
                         WHERE CloneReposId={cloneReposId} AND
                               Similarity={similarity} AND
                               PostBlockVersionId={postblockversionId}
        """
        cursor = self.client.run_query(query_str)
        return cursor.fetchone()['Id']

    def get_post_score(self, postid):
        query_str = f"""SELECT Score
                        FROM Posts
                        WHERE Id={postid}
        """
        cursor = self.client.run_query(query_str)
        return cursor.fetchone()['Score']

    def getSnippetPostId(self, snippetid):
        query_str = f"""SELECT PostId FROM PostBlockVersion WHERE Id={snippetid} LIMIT 1
        """
        iter_cursor = self.client.run_query(query_str)
        rdict = iter_cursor.fetchone()
        return rdict['PostId']

    def getPostId_from_codeblockversion_table(self, snippetid):
        query_str = f"SELECT PostId FROM CodeBlockVersion WHERE VersionId={snippetid} LIMIT 1"
        iter_cursor = self.client.run_query(query_str)
        rdict = iter_cursor.fetchone()
        return rdict['PostId']

    def updateEvaluationTable(self, postid, snippetid):
        query_str = """UPDATE CodeSnippetGamification SET PostId={} WHERE SnippetId={}
        """.format(postid, snippetid)
        self.client.run_query(query_str)
        self.client.db.commt()

    def getNtestdata(self, n):
        query_str = """SELECT PostId, SnippetId AS VersionID FROM CodeSnippetGamification LIMIT {}
        """.format(n)
        return self.client.run_query(query_str)

    def insertIntoTestSet(self, rows_to_insert):
        #(id_, votesSecure, votesInsecure, votes, postid, versionid, snippet, language)
        self.client.cursor.executemany(
            """INSERT INTO securityCategorizing_codesnippet (id, votesSecure, votesInsecure, votes, PostId, VersionID, snippet, language)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """, rows_to_insert)
        self.client.db.commit()

    def queryGamification(self):
        query_str = """SELECT PostId, SnippetId, Snippet FROM CodeSnippetGamification
        """
        return self.client.run_query(query_str)

    # CloneResults
    def getCloneRepoFileAndSnippet(self, reponame):
        """Get all the files containing copied code snippets belonging to the specified repository.
        """
        query_str = """SELECT RepoFile, RepoSnippet FROM CloneResults WHERE RepoName={}""".format(reponame)
        return self.client.run_query(query_str)

    def getCloneRepos(self, cloneReposId):
        query_str = f"""SELECT *
                       FROM CloneRepos
                       WHERE Id={cloneReposId}
        """
        cursor = self.client.run_query(query_str)
        return cursor.fetchone()

    def getCloneReposId(self, reponame, repofile, startline, endline):
        """Get the rowid (primary key id) matching the given arguments.
        """
        query_str = f"""SELECT Id 
                        FROM CloneRepos
                        WHERE RepoName='{reponame}' AND
                              RepoFile='{repofile}' AND
                              RepoFileStartLine={startline} AND
                              RepoFileEndLine={endline}
                    """

        cursor = self.client.run_query(query_str)
        return cursor.fetchone()['Id']

    def get_clone_ids(self, similarity):
        query_str = f"""SELECT Id
                       FROM CloneResultsNew
                       WHERE Similarity={similarity}
        """
        return [row['Id'] for row in self.client.run_query(query_str)]

    def getRepositoryClones(self, orgname):
        processed_clones = self.getClonesWithCommits()
        query_str = f"SELECT * FROM CloneResults WHERE Org='{orgname}' AND Id NOT IN {processed_clones}"
        cursor = self.client.run_query(query_str)
        return cursor.fetchall()

    def getCloneRepoFiles(self):
        query_str = f"""SELECT Id, RepoName, RepoFile, RepoFileStartLine, RepoFileEndLine
                        FROM CloneRepos
                        GROUP BY RepoName, RepoFile, RepoFileStartLine, RepoFileEndLine
        """
        cursor = self.client.run_query(query_str)
        return cursor.fetchall() #[row for row in cursor.fetchall()]

    def getClonesWithCommits(self):
        """The clones whose commits are already analyzed.
        """
        query_str = """SELECT CloneId FROM CloneResults_Commits GROUP BY CloneId
        """
        cursor = self.client.run_query(query_str)
        records = cursor.fetchall()
        records = [row['CloneId'] for row in records]
        return tuple(records)

    def getAndroidRepositoryClones(self):
        query_str = """SELECT RepoName, RepoFile, RepoSnippet, StartLine, EndLine FROM CloneResultsAndroid GROUP BY RepoName, RepoFile, RepoSnippet, StartLine, EndLine
        """
        return self.client.run_query(query_str)

    def getSnippetClones(self):
        """SELECT all code snippets detected to have been copied to GitHub projects
        """
        query_str = """SELECT PostId, SnippetId FROM CloneResults GROUP BY PostId, SnippetId"""
        return self.client.run_query(query_str)

    def getSnippetIdClones(self):
        query_str = """SELECT SnippetId FROM CloneResults GROUP BY SnippetId"""
        cursor = self.client.run_query(query_str)
        return cursor.fetchall()

    def getAndroidSnippetClones(self):
        """SELECT all code snippets detected to have been copied to GitHub projects
        """
        query_str = """SELECT PostId, SnippetId FROM CloneResultsAndroid GROUP BY PostId, SnippetId"""
        return self.client.run_query(query_str)

    def insertIntoCloneResultsCommits(self, rows_to_insert):
        self.client.cursor.executemany(
            """INSERT INTO CloneResults_Commits (CloneId, CommitHash)
               VALUES (%s, %s)
            """, rows_to_insert)
        self.client.db.commit()

    def  insertIntoSnippetRevisionHistory(self, rows_to_insert):
        # postid, postversion_count, snippetid, creationdate, predcount, succCount, mostrecent_version
        # (PostId, PostVersionCount, SnippetId, CreationDate, PredCount, SuccCount, MostRecentVersion)
        self.client.cursor.executemany(
            """INSERT INTO CloneResults_SnippetRevisionHistory (PostId, PostVersionCount, SnippetId, CreationDate, PredCount, SuccCount, MostRecentVersion)
               VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, rows_to_insert)
        self.client.db.commit()

    def  insertIntoAndroidSnippetRevisionHistory(self, rows_to_insert):
        # postid, postversion_count, snippetid, creationdate, predcount, succCount, mostrecent_version
        # (PostId, PostVersionCount, SnippetId, CreationDate, PredCount, SuccCount, MostRecentVersion)
        self.client.cursor.executemany(
            """INSERT INTO CloneResults_SnippetRevisionHistoryAndroid (PostId, PostVersionCount, SnippetId, CreationDate, PredCount, SuccCount, MostRecentVersion)
               VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, rows_to_insert)
        self.client.db.commit()

    def insertRootSnippetDates(self, rows_to_insert):
        self.client.cursor.executemany(
            """INSERT INTO RootSnippetDates (PostHistoryId, CreationDate)
               VALUES (%s, %s)
            """, rows_to_insert)
        self.client.db.commit()

    def getRootCreationDate(self, historyid):
        query_str = f"""SELECT CreationDate FROM RootSnippetDates WHERE PostHistoryId = {historyid} AND CreationDate IS NOT NULL LIMIT 1"""
        cursor = self.client.run_query(query_str)
        return cursor.fetchone()['CreationDate']

    def getRootHistoriesWithNullDates(self):
        query_str = """SELECT PostHistoryId FROM RootSnippetDates WHERE CreationDate IS NULL 
        AND PostHistoryId IS NOT NULL"""
        return self.client.run_query(query_str)

    def getTestRepoSnippet(self, reponame, repofile):
        """delete later
        """
        query_str = """SELECT RepoSnippet FROM CloneResults WHERE RepoName='{}' AND RepoFile='{}' LIMIT 1""".format(reponame, repofile)
        iter_cursor = self.client.run_query(query_str)
        return iter_cursor.fetchone()['RepoSnippet']

    def getInsecurePost(self, to_set=False):
        """
        """
        query_str =  "SELECT PostId FROM LabeledCodeExamples WHERE LabelId='insecure' GROUP BY PostId"
        rows = {row['PostId'] for row in self.client.run_query(query_str)}
        if to_set:
            return rows
        return list(rows)

    def getcomments(self, postid):
        query_str = f"SELECT Id, Text, CreationDate FROM Comments WHERE PostId={postid}"
        return self.client.run_query(query_str)

    def get_commit_message(self, commit_id):
        query_str = f"SELECT Comment FROM PostVersion WHERE Id={commit_id}"
        cursor = self.client.run_query(query_str)
        return cursor.fetchone()['Comment']

    def get_commit(self, postid, posthistoryid):
        query_str = f"""SELECT Id, Comment
                        FROM PostVersion
                        WHERE PostId={postid} AND PostHistoryId={posthistoryid}
        """
        cursor = self.client.run_query(query_str)
        if cursor.rowcount == 0:
            return None, None
        rdict = cursor.fetchone()
        return rdict['Id'], rdict['Comment']

    def get_commit_messages_after_posthistory(self, postid, posthistoryid):
        '''Returns all the commit messages made to a post revision after the given
           posthistory.
        '''
        query_str = f"""SELECT Comment
                        FROM PostVersion
                        WHERE PostId={postid} AND PostHistoryId > {posthistoryid}
        """
        return [ row['Comment'] for row in self.client.run_query(query_str) if row['Comment'] ]

    def getcomments_aslist(self, post_id):
        """
        Gets all the comments of the post and returns them as a list.
        Args:
            post_id: The ID of the post to retrieve the comments for.
        Returns:
            A list of tuples of the form (Id, Text) consisting of the comment ID and comment text.
        """
        comments = []
        for row in self.getcomments(post_id):
            comments.append( (row['Id'], row['Text']) )
        return comments

    def insertIntoRelevantInsecurePostComments(self, rows_to_insert):
        # RelevantInsecurePostComments(Id, PostId, CommentId, CommentText, SecurityRelevant (smallint))
        self.client.cursor.executemany(
            """INSERT INTO RelevantInsecurePostComments (PostId, CommentId, CommentText)
               VALUES (%s, %s, %s)
            """, rows_to_insert)
        self.client.db.commit()
    
    def getPostsWithMostComments(self):
        """The top-200 posts containing comments that are not yet labeled.
        """
        query_str = """SELECT PostId, COUNT(*) as count FROM RelevantInsecurePostComments WHERE SecurityRelevant=-1 GROUP BY PostId ORDER BY count DESC LIMIT 200
        """
        s = {row['PostId'] for row in self.client.run_query(query_str)}
        return list(s)

    def getAlreadyLabeledPosts(self):
        """Get a set of posts already labeled
        """
        query_str = """SELECT PostId 
                       FROM RelevantInsecurePostComments 
                       WHERE SecurityRelevant=0 OR SecurityRelevant=1 
                       GROUP BY PostId
        """
        result = [row['PostId'] for row in self.client.run_query(query_str)]
        return result

    def getInsecureComments(self, postid):
        query_str = f"SELECT CommentId, CommentText, SecurityRelevant FROM RelevantInsecurePostComments WHERE PostId={postid}"
        return self.client.run_query(query_str)

    def getPostHistoryId(self, snippetid) -> int:
        """Returns the PostBlockVersion.PostHistoryId associated with the given snippet
        """
        query_str = f"SELECT PostHistoryId FROM PostBlockVersion WHERE Id={snippetid}"
        cursor = self.client.run_query(query_str)
        return cursor.fetchone()['PostHistoryId']
    
    def getPostHistoryIds(self, postid) -> list:
        """Returns the PostBlockVersion.PostHistoryId associated with the given post
        """
        query_str = f"""SELECT PostHistoryId 
                        FROM PostBlockVersion 
                        WHERE PostId={postid}
                        GROUP BY PostHistoryId
                        ORDER BY PostHistoryId ASC
                        """
        cursor = self.client.run_query(query_str)
        return [ row['PostHistoryId'] for row in cursor.fetchall() ]

    def getSnippetCreationDate(self, historyid):
        """Returns the date the given snippet was created.
           Checks the PostVersion table 
        """
        query_str = f"SELECT CreationDate FROM PostVersion WHERE PostHistoryId={historyid}"
        cursor = self.client.run_query(query_str)
        if cursor.rowcount > 0:
            return cursor.fetchone()['CreationDate']
        return None

    def getRecord(self, snippetid) -> int:
        """Gets the record matching the given snippet
        """
        query_str = f"SELECT PredCount, SuccCount, MostRecentVersion FROM PostBlockVersion WHERE Id={snippetid}"
        cursor = self.client.run_query(query_str)
        return cursor.fetchone()

    def get_timeline_clones(self, category):
        '''Gets either outdated or potentially up-to-date clones. Currently we have two categories:
           (1) Outdated clones (category=1)
           (2) Potentially up-to-date clones (category=1)
        '''
        query_str = f"SELECT ClonesId FROM TimelineAnalysis WHERE category={category}"
        cursor = self.client.run_query(query_str)
        return cursor.fetchall()

    def get_crossproductclone(self, clone_id):
        query_str = f"SELECT * FROM CrossProductClones WHERE Id={clone_id}"
        cursor = self.client.run_query(query_str)
        return cursor.fetchone()

    def getPostVersionCount(self, postid) -> int:
        """Gets the number of versions this post has
        """
        query_str = f"SELECT COUNT(Id) as count FROM PostVersion WHERE PostId={postid}"
        cursor = self.client.run_query(query_str)
        if cursor.rowcount > 0:
            return cursor.fetchone()['count']
        return 0

    def get_cloneresults(self):
        query_str = f"""SELECT RepoName, RepoFile, StartLine, EndLine, SnippetId, PostId, Similarity 
        FROM CloneResults
        GROUP BY RepoName, RepoFile, SnippetId, Similarity"""
        return self.client.run_query(query_str)  

    def get_clones_rootids(self):
        query_str = f"""SELECT RootId, RepoSnippet
                        FROM CloneResults
                        GROUP BY RootId"""
        return self.client.run_query(query_str)

    def selectCloneRepoNames(self, top, similarity):
        query_str = f"""SELECT RepoName, COUNT(DISTINCT(RepoSnippet)) as count 
                       FROM CloneResultsNew WHERE Similarity={similarity} 
                       GROUP BY RepoName 
                       ORDER BY count DESC
                       LIMIT {top}
        """
        cursor = self.client.run_query(query_str)
        return [row['RepoName'] for row in cursor.fetchall()]

    def getreposnippet(self, reponame, limit):
        query_str = f"""SELECT RootId, RepoSnippet
                        FROM CloneResultsNew
                        WHERE RepoName='{reponame}'
                        LIMIT {limit}
        """
        return self.client.run_query(query_str).fetchall()

    def get_subsampling_clone_record(self, row_id):
        query_str = f"""SELECT RootId, SnippetId, RepoSnippet
                       FROM CloneResultsNew
                       WHERE Id={row_id}
        """
        cursor = self.client.run_query(query_str)
        return cursor.fetchone()

    def get_android_cloneresults(self):
        query_str = f"""SELECT RepoName, RepoFile, StartLine, EndLine, SnippetId, PostId, Similarity FROM CloneResultsAndroid 
        GROUP BY RepoName, RepoFile, SnippetId, Similarity"""
        return self.client.run_query(query_str)    

    def get_filerevisionhistory(self, reponame, repofile, startline, endline):
        query_str = f"""SELECT * FROM CloneResults_FileRevisionHistory
        WHERE RepoName='{reponame}' AND RepoFile='{repofile}' AND StartLine={startline} AND EndLine={endline}"""
        cursor = self.client.run_query(query_str)
        if cursor.rowcount == 0:
            return None, 0
        elif cursor.rowcount == 1:
            return cursor.fetchone(), 1
        else:
            None, cursor.rowcount

    def get_android_filerevisionhistory(self, reponame, repofile, startline, endline):
        query_str = f"""SELECT Id, CommitDate FROM CloneResults_FileRevisionHistoryAndroid 
        WHERE RepoName='{reponame}' AND RepoFile='{repofile}' AND StartLine={startline} AND EndLine={endline}"""
        return self.client.run_query(query_str)

    def historyExists(self, reponame, repofile):
        query_str = f"""SELECT COUNT(DISTINCT(RepoName)) AS count FROM CloneResults_FileRevisionHistory where RepoName='{reponame}'
                       AND RepoFile='{repofile}'"""
        cursor = self.client.run_query(query_str)
        return cursor.fetchone()['count']

    def get_snippet_history(self, snippetid):
        query_str = f"SELECT * FROM CloneResults_SnippetRevisionHistory WHERE SnippetId={snippetid}"
        cursor = self.client.run_query(query_str)
        if cursor.rowcount == 0:
            return None, 0
        elif cursor.rowcount == 1:
            return cursor.fetchone(), 1
        else:
            return None, cursor.rowcount
        
        return self.client.run_query(query_str)

    def getCommentCount(self, postid):
        query_str = f"SELECT COUNT(Id) as count FROM Comments WHERE PostId={postid}"
        cursor = self.client.run_query(query_str)
        return cursor.fetchone()['count']

    def getSnippetsWithNullCreationDates(self):
        query_str = "SELECT SnippetId FROM CloneResults_SnippetRevisionHistory WHERE CreationDate IS NULL GROUP BY SnippetId"
        return self.client.run_query(query_str)

    def updateSnippetHistoryCreationDate(self, snippetid, creationdate):
        query_str = f"UPDATE CloneResults_SnippetRevisionHistory SET CreationDate='{creationdate}' WHERE SnippetId={snippetid}"
        self.client.run_query(query_str)
        self.client.db.commit()

    def updatedCloneResultsAndroid(self, reponame, rowid, repofile=None):
        if repofile:
            query_str = f"""UPDATE CloneResultsAndroid SET RepoName='{reponame}', RepoFile='{repofile}'
            WHERE Id={rowid}"""
        else:
            query_str = f"UPDATE CloneResultsAndroid SET RepoName='{reponame}' WHERE Id={rowid}"
        self.client.run_query(query_str)
        self.client.db.commit()

    def getAndroidGamesClones(self):
        query_str = "SELECT Id, RepoFile FROM CloneResultsAndroid WHERE RepoName='games'"
        return self.client.run_query(query_str)

    def getsnippetSuccCount(self, snippetid):
        query_str = f"SELECT SuccCount FROM PostBlockVersion WHERE Id={snippetid}"
        cursor = self.client.run_query(query_str)
        return cursor.fetchone()['SuccCount']

    def getrootcodeblock(self, snippetid):
        query_str = f"SELECT RootPostBlockVersionId FROM PostBlockVersion WHERE ID={snippetid}"
        cursor = self.client.run_query(query_str)
        return cursor.fetchone()['RootPostBlockVersionId']
    
    def getRootPostBlockIds(self, postid, totuple=False):
        query_str = f"""SELECT RootPostBlockVersionId 
                        FROM PostBlockVersion
                        WHERE PostId={postid} AND PostBlockTypeId=2
                        GROUP BY RootPostBlockVersionId 
                        ORDER BY PostHistoryId ASC
                    """
        cursor = self.client.run_query(query_str)
        if totuple:
            root_ids = [ row['RootPostBlockVersionId'] for row in cursor.fetchall() ]
            if len(root_ids) == 1:
                # solves the problem were the list contains a single element
                # and converting to a tuple gives (1,)
                # the trailing comma will lead to SQL query error
                return root_ids[0], root_ids[0]
            return tuple(root_ids)
        return cursor

    def getPredEqualAndSimilarity(self, snippetid):
        query_str = f"SELECT PredEqual, PredSimilarity FROM PostBlockVersion WHERE Id={snippetid}"
        cursor = self.client.run_query(query_str)
        return cursor.fetchone()

    def getcodeblockversionchain_count(self, rootid):
        query_str = f"""SELECT COUNT(Id) AS count 
                        FROM PostBlockVersion 
                        WHERE RootPostBlockVersionId={rootid} AND (PredEqual IS NULL OR PredEqual = 0) 
                        ORDER BY PostHistoryId ASC"""
        cursor = self.client.run_query(query_str)
        return cursor.fetchone()['count']

    def hasMostRecentVersion(self, rootid, linecount=10):
        query_str = f"""SELECT Id
                        FROM PostBlockVersion
                        WHERE RootPostBlockVersionId={rootid} AND LineCount >= {linecount} AND MostRecentVersion=True
                    """
        cursor = self.client.run_query(query_str)
        if cursor.rowcount > 0:
            return True
        else:
            return False

    def getcodeblockcontent(self, snippetid):
        query_str = f"""SELECT Content
                        FROM PostBlockVersion
                        WHERE Id={snippetid}
        """
        return self.client.run_query(query_str).fetchone()['Content']

    def getcodeblock_linecount(self, snippetid):
        query_str = f"""SELECT LineCount
                        FROM PostBlockVersion
                        WHERE Id={snippetid}
        """
        return self.client.run_query(query_str).fetchone()['LineCount']

    def getcodeblockversionchain_ids(self, rootid):
        query_str = f"SELECT Id FROM PostBlockVersion WHERE RootPostBlockVersionId={rootid} AND (PredEqual IS NULL OR PredEqual = 0) ORDER BY PostHistoryId ASC"
        return [row['Id'] for row in self.client.run_query(query_str)]

    def getcodeblockversionchain_list(self, rootid):
        query_str = f"""SELECT Id 
                        FROM PostBlockVersion 
                        WHERE RootPostBlockVersionId={rootid} 
                        AND (PredEqual IS NULL OR PredEqual = 0) 
                        ORDER BY PostHistoryId ASC"""
        cursor = self.client.run_query(query_str)
        return [row['Id'] for row in cursor]

    def deleterows(self, tablename, startrow, endrow):
        query_str = f"""DELETE FROM {tablename} WHERE Id between {startrow} and {endrow}
        """
        cursor = self.client.run_query(query_str)
        affected_rows = cursor.rowcount
        self.client.db.commit()
        return affected_rows

    def getPostBlockVersionIds(self, rootPostBlockVersionId):
        query_str = f""" SELECT Id
                        FROM PostBlockVersion
                        WHERE RootPostBlockVersionId={rootPostBlockVersionId}
                              AND (PredEqual IS NULL OR PredEqual = 0)
                              ORDER BY PostHistoryId ASC """
        cursor = self.client.run_query(query_str)
        return [ row['Id'] for row in cursor.fetchall() ]

    def getcodeblockversionchain(self, rootid, linecount, ignoreRecent=False):
        if ignoreRecent:
            query_str = f"""SELECT Id, Content, LineCount
                            FROM PostBlockVersion 
                            WHERE RootPostBlockVersionId={rootid} AND LineCount >= {linecount} AND MostRecentVersion=False
                            AND (PredEqual IS NULL OR PredEqual = 0) 
                            ORDER BY PostHistoryId ASC"""
        else:
            query_str = f"""SELECT Id, Content
                            FROM PostBlockVersion 
                            WHERE RootPostBlockVersionId={rootid} AND LineCount >= {linecount}
                            AND (PredEqual IS NULL OR PredEqual = 0) 
                            ORDER BY PostHistoryId ASC"""
        return self.client.run_query(query_str)

    def insertIntoRespiceAdspiceProspice(self, rows_to_insert):
        self.client.cursor.executemany(
            """INSERT INTO RespiceAdspiceProspice(Org, RepoName, RepoFile, RepoSnippet, StartLine, EndLine,
                CommitHash, CommitDate, RepoFileCommitCount, CommitCountSinceSnippetAdded, 
                RootSnippetVersionId, RootSnippetVersionDate, SnippetId, LastSnippetVersionDate,
                SnippetModificationCount, PostId, PostTypeId, CommentCount, ResultTypeId, SecurityRelevant, Similarity)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, rows_to_insert)
        self.client.db.commit()

    def getOutdatedResults(self, startlimit, endlimit, resulttypeid=3):
        query_str = f"""SELECT Id, Org, RepoSnippet, SnippetId, SecurityRelevant, CloneRelevant 
                        FROM RespiceAdspiceProspice
                        WHERE ResultTypeId={resulttypeid} LIMIT {startlimit},{endlimit}"""
        return self.client.run_query(query_str)

    def getCryptoSnippetClones(self):
        query_str = """SELECT  clones.RepoName AS RepoName, clones.RepoFile AS RepoFile, clones.RepoSnippet as RepoSnippet
                       FROM CloneResults clones 
                       INNER JOIN CodeSnippetGamification csg
                       WHERE clones.PostId=csg.PostId Group by clones.RepoName, clones.RepoFile"""
        return self.client.run_query(query_str)

    def getAndroidRepoFileNames(self):
        query_str = "SELECT Id, RepoFile FROM CloneResultsAndroid WHERE RepoName='home'"
        return self.client.run_query(query_str)

    def updateRelevantInsecurePostComments(self, commentid, securityrelevant):
        query_str=f"UPDATE RelevantInsecurePostComments SET SecurityRelevant={securityrelevant} WHERE CommentId={commentid}"
        self.client.run_query(query_str)
        self.client.db.commit()

    def getInsecurePostWithSecurityRelevantComment(self):
        query_str = "SELECT CommentId, CommentText FROM RelevantInsecurePostComments WHERE SecurityRelevant=1"
        return self.client.run_query(query_str)

    def get_commentdate(self, commentid):
        query_str = f"SELECT CreationDate FROM Comments WHERE Id={commentid}"
        cursor = self.client.run_query(query_str)
        return cursor.fetchone()['CreationDate']
    
    def getRespiceSnippet(self):
        query_str = "SELECT Id, PostId, ResultTypeId, CommentCount FROM RespiceAdspiceProspice WHERE ResultTypeId=1 OR ResultTypeId=4"
        return self.client.run_query(query_str)

    def getRespicePosts(self):
        query_str = "SELECT PostId FROM RespiceAdspiceProspice WHERE ResultTypeId=1 OR ResultTypeId=4 GROUP BY PostId"
        return self.client.run_query(query_str)

    def insertIntoCommentsAnalysisMajorRevision(self, rows_to_insert):
        self.client.cursor.executemany(
            """INSERT INTO CommentsAnalysisMajorRevision(PostId, CommentId, CommentText)
               VALUES (%s, %s, %s)
            """, rows_to_insert)
        self.client.db.commit()

    def insertIntoPriorVulnerableRepos(self, rows_to_insert):
        self.client.cursor.executemany(
            """INSERT INTO PriorVulnerableRepos(RepoName, PostId, PostTypeId, ClassifierIndex, ClassifierType)
               VALUES (%s, %s, %s, %s, %s)
            """, rows_to_insert)
        self.client.db.commit()

    def getWellMaintainedPosts(self, both=False):
        query_str = None
        if both:
            query_str = """SELECT PostId FROM PriorVulnerableRepos
                           WHERE WellMaintained=1 OR WellMaintained=0
                           GROUP BY PostId"""
        else:
            # only well-maintained
            query_str = """SELECT PostId FROM PriorVulnerableRepos
                           WHERE WellMaintained=1
                           GROUP BY PostId"""

        return [row['PostId'] for row in self.client.run_query(query_str)]

    def insertIntoPriorVulnerableReposComments(self, rows_to_insert):
        self.client.cursor.executemany(
            """INSERT INTO PriorVulnerableReposComments(PostId, CommentId, CommentText)
               VALUES (%s, %s, %s)
            """, rows_to_insert)
        self.client.db.commit()

    def insertIntoEclipseApacheAttributions(self, rows_to_insert):
        self.client.cursor.executemany(
            """INSERT INTO EclipseApacheAttributions(RepoName, RepoFile, LineNumber, 
               PostId, PostTypeId, OrgName)
               VALUES (%s, %s, %s, %s, %s, %s)
            """, rows_to_insert)
        self.client.db.commit()

    def getxxx(self):
        query_str = """SELECT PostId FROM PriorVulnerableRepos
                       WHERE WellMaintained=1
                       GROUP BY PostId
        """
        return []

    def getPostIdsFromVulnerableRepos(self, attributionprovided=None):
        query_str = None
        if attributionprovided == 0 or attributionprovided == 1:
            query_str = f"""SELECT PostId FROM PriorVulnerableRepos
                           WHERE AttributionProvided={attributionprovided}
                           GROUP BY PostId"""
        else:
            query_str = "SELECT PostId FROM PriorVulnerableRepos GROUP BY PostId"
        return self.client.run_query(query_str)

    def getRespiceSnippets(self):
        query_str = "SELECT SnippetId FROM RespiceAdspiceProspice GROUP BY SnippetId"
        return self.client.run_query(query_str)

    def get_snippets_by_category(self, categoryId):
        query_str = f"SELECT SnippetId FROM RespiceAdspiceProspice WHERE ResultTypeId={categoryId} GROUP BY SnippetId"
        return self.client.run_query(query_str)

    def get_root_id(self, snippetid):
        query_str = f"""SELECT RootSnippetVersionId FROM RespiceAdspiceProspice 
                        WHERE SnippetId={snippetid} LIMIT 1"""
        cursor = self.client.run_query(query_str)
        return cursor.fetchone()['RootSnippetVersionId']

    def getCrawledSnippets(self):
        query_str = "SELECT Domain, Snippet FROM CrawledTutorials GROUP BY Domain, Snippet"
        return self.client.run_query(query_str)

    def getRespiceRecord(self, resulttypeid):
        query_str = f"""SELECT PostId, RootSnippetVersionId, CommitDate, LastSnippetVersionDate 
        FROM RespiceAdspiceProspice WHERE ResultTypeId=1 OR ResultTypeId=4
        GROUP BY PostId, RootSnippetVersionId, CommitDate, LastSnippetVersionDate
        """
        return self.client.run_query(query_str)

    def getRespicePostsByResultType(self, resulttypeid):
        query_str = f"""SELECT PostId FROM RespiceAdspiceProspice 
                        WHERE ResultTypeId={resulttypeid}
                        GROUP BY PostId"""
        return self.client.run_query(query_str)

    def getRepoAndSnippetCopiesByCategory(self, resulttypeid):
        query_str = f"""SELECT SnippetId, CommitDate, RepoName, RepoFile
                        FROM RespiceAdspiceProspice 
                        WHERE ResultTypeId={resulttypeid}
                        GROUP BY SnippetId, CommitDate, RepoName, RepoFile"""
        return self.client.run_query(query_str)

    def getRespiceRootIds(self, resulttypeid):
        query_str = f"""SELECT Id, RootSnippetVersionId, RootSnippetVersionDate, CommitDate, SnippetId
                        FROM RespiceAdspiceProspice 
                        WHERE ResultTypeId={resulttypeid}
                        GROUP BY RootSnippetVersionId
        """
        return [(row['Id'], row['RootSnippetVersionId'], 
                 row['RootSnippetVersionDate'], row['CommitDate'], row['SnippetId']
                 ) for row in self.client.run_query(query_str)]

    def getRespiceOnlyRootIds(self):
        query_str = """SELECT RootSnippetVersionId
                        FROM RespiceAdspiceProspice 
                        GROUP BY RootSnippetVersionId
        """
        return self.client.run_query(query_str)

    def getRespiceOnlyRepos(self, resulttypeid, orgname):
        query_str = f"""SELECT RepoName
                        FROM RespiceAdspiceProspice 
                        WHERE ResultTypeId={resulttypeid} AND Org='{orgname}'
                        GROUP BY RepoName
        """
        return self.client.run_query(query_str)

    def getRespiceRepoNameAndFile(self, resulttypeid, orgname):
        query_str = f"""SELECT RepoName, RepoFile 
                        FROM RespiceAdspiceProspice 
                        WHERE ResultTypeId={resulttypeid} AND Org='{orgname}'
                        GROUP BY RepoName, RepoFile"""
        return self.client.run_query(query_str)

    def getRespiceRepoFileAndRootId(self, reponame, resulttypeid):
        query_str = f""" SELECT RootSnippetVersionId, RepoFile
                         FROM RespiceAdspiceProspice
                         WHERE RepoName='{reponame}' AND ResultTypeId={resulttypeid}
                         GROUP BY RootSnippetVersionId, RepoFile
                    """
        return self.client.run_query(query_str)

    def getUniqueRespiceRepos(self, resulttypeid):
        query_str = f"""SELECT RepoName 
                        FROM RespiceAdspiceProspice 
                        WHERE ResultTypeId={resulttypeid}
                        GROUP BY RepoName
        """
        return [row['RepoName'] for row in self.client.run_query(query_str)]
    
    def getClonesPerResultGroupBy(self):
        query_str = """SELECT RepoName, OriginalRepoFile, CommitHash,
                              RootSnippetVersionId, SnippetVersionId, Similarity
                       FROM ClonesPerCommitResult
                       GROUP BY RepoName, 
                                OriginalRepoFile, 
                               CommitHash, 
                               RootSnippetVersionId,
                               SnippetVersionId
        """
        return self.client.run_query(query_str)
    
    def getClonesPerResult(self, reponame, commit):
        query_str = f"""SELECT RootSnippetVersionId, SnippetVersionId, Similarity
                       FROM ClonesPerCommitResult
                       WHERE RepoName={reponame} AND CommitHash={commit}
        """
        # Returns a list of tuples
        return self.client.run_query(query_str).fetchall()
    
    def getClonesPerResultRootIds(self):
        """Returns a list of rootids.
        """
        query_str = """SELECT RootSnippetVersionId 
                       FROM ClonesPerCommitResult
                       GROUP BY RootSnippetVersionId
                    """
        return [ row['RootSnippetVersionId'] for row in self.client.run_query(query_str)]
    
    def getClonesPerResultSnippetIds(self, rootid):
        query_str = f"""SELECT SnippetVersionId
                       FROM ClonesPerCommitResult
                       WHERE RootSnippetVersionId={rootid}
        """
        return [row['SnippetVersionId'] for row in self.client.run_query(query_str)]

    def getRepoFileCommitHash(self, rootid):
        query_str = f"""SELECT RepoName, OriginalRepoFile, CommitHash 
                       FROM ClonesPerCommitResult
                       WHERE RootSnippetVersionId={rootid}
                       GROUP BY RepoName, OriginalRepoFile, CommitHash
        """
        return self.client.run_query(query_str)

    def getCloneSimilarity(self, reponame, repofile, commithash, rootid, snippetid):
        query_str = f"""SELECT Similarity FROM ClonesPerCommitResult
                        WHERE RepoName='{reponame}' AND OriginalRepoFile='{repofile}' 
                              AND CommitHash='{commithash}'
                              AND RootSnippetVersionId={rootid} AND SnippetVersionId={snippetid}
        """
        cursor = self.client.run_query(query_str)
        if cursor.rowcount == 0:
            # NotFound
            return -1
        return cursor.fetchone()['Similarity']
    
    def stats_root_snippets_locations(self):
        query_str = """SELECT RootSnippetVersionId, COUNT(DISTINCT(OriginalRepoFile)) as Locations 
                       FROM ClonesPerCommitResult 
                       GROUP BY RootSnippetVersionId 
                       ORDER BY Locations DESC;   
        """
        cursor = self.client.run_query(query_str)
        return cursor.fetchall()
    
    def stats_root_snippets_repositories(self):
        query_str = """SELECT RootSnippetVersionId, COUNT(DISTINCT(RepoName)) as RepoCount 
                       FROM ClonesPerCommitResult 
                       GROUP BY RootSnippetVersionId 
                       ORDER BY RepoCount DESC;   
        """
        cursor = self.client.run_query(query_str)
        return cursor.fetchall()

    def get_snippet_edit_timeline(self):
        query_str = """SELECT RootSnippetVersionId, StartSnippetVersionDate, EndSnippetVersionDate
                       FROM CloneSnippetsTimeline 
        """
        cursor = self.client.run_query(query_str)
        return cursor.fetchall()

    def stats_root_snippets_evolution(self):
        query_str = """SELECT RootPostBlockVersionId, COUNT(Id) AS count
                        FROM PostBlockVersion 
                        WHERE RootPostBlockVersionId IN (
                          SELECT RootSnippetVersionId
                          FROM ClonesPerCommitResult
                          GROUP BY RootSnippetVersionId
                        ) AND (PredEqual IS NULL OR PredEqual = 0)
                        ORDER BY PostHistoryId ASC
        """
        bq="""SELECT   pbv.RootPostBlockVersionId, COUNT(pbv.Id) as count
                       FROM PostBlockVersion pbv
                       INNER JOIN ClonesPerCommitResult cpcr
                       ON cpcr.RootSnippetVersionId=pbv.Id
                       WHERE PredEqual IS NULL OR PredEqual = 0
                       GROUP BY pbv.RootPostBlockVersionId
                       ORDER BY PostHistoryId ASC
        """
        cursor = self.client.run_query(bq)
        return cursor.fetchall()

    def insertCloneSnippetTimeline(self, rows_to_insert):
        # TODO: convert CloneSnippetsTimeline to View
        self.client.cursor.executemany(
            """INSERT INTO CloneSnippetsTimeline(
                RootSnippetVersionId, StartSnippetVersionId, StartSnippetVersionDate,
                EndSnippetVersionId, EndSnippetVersionDate
                ) VALUES (%s, %s, %s, %s, %s)
            """, rows_to_insert)
        self.client.db.commit()

    def insertIntoDataReference(self, rows_to_insert):
        inserted = self.client.cursor.executemany(
            """INSERT INTO 
               DataReferences(RepoName, Reference, PostId, RepoFile, ReferenceType, HasCode, Language) 
               VALUES (%s, %s,  %s, %s, %s, %s, %s)
            """, rows_to_insert)
        self.client.db.commit()
        return inserted

    def insertintoVersionUpdates(self, rows_to_insert):
        # (reponame, repofile, commit, rootid, snippetid_, outcome)
        self.client.cursor.executemany(
            """INSERT INTO VersionUpdates(
               RepoName, RepoFile, CommitHash, RootSnippetId, CopiedSnippetId, LatestSnippetId, Outcome) 
               VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, rows_to_insert)
        self.client.db.commit()

    def getReferenceFile(self, reponame, filext='.java'):
        query_str = f"""SELECT Path 
                        FROM PostReferenceGH 
                        WHERE RepoName='{reponame}' AND FileExt='{filext}'
                        GROUP BY Path
        """
        cursor = self.client.run_query(query_str)
        return cursor.fetchall()

    def get_attributing_projects(self, fileExt='.java', limit=None):
        """Gets the reponames and owners and the branch that attributed SO posts
           inside files matching the given file extension.
        """
        if limit:
            query_str = f"""SELECT RepoOwner, RepoName, Branch
                        FROM PostReferenceGH
                        WHERE FileExt='{fileExt}'
                        GROUP BY RepoOwner, RepoName, Branch LIMIT {limit}"""
        else:
            query_str = f"""SELECT RepoOwner, RepoName, Branch
                            FROM PostReferenceGH
                            WHERE FileExt='{fileExt}'
                            GROUP BY RepoOwner, RepoName, Branch
            """
        cursor = self.client.run_query(query_str)
        return cursor.fetchall()

    def getReferenceRepos(self, fileExt='.java'):
        query_str = f"""SELECT RepoOwner, RepoName, Branch FROM PostReferenceGH
                        WHERE FileExt='{fileExt}'
                        GROUP BY RepoOwner, RepoName, Branch
        """
        cursor = self.client.run_query(query_str)
        return cursor.fetchall()

    def getcodeblockscount(self, postid):
        # Does this post contains code blocks?
        query_str = f"""SELECT COUNT(Id) as count
                       FROM PostBlockVersion
                       WHERE PostBlockTypeId=2 AND PostId={postid}
                  
        """
        cursor = self.client.run_query(query_str)
        d = cursor.fetchone()
        count = d['count']
        return 1 if count > 0 else 0
    
    def updateCloneResultsOrgName(self, rowid, orgname):
        query_str = f"""UPDATE CloneResults 
                       SET Org='{orgname}'
                       WHERE Id={rowid}
        """
        self.client.run_query(query_str)
        self.client.db.commit()
        
    def getcloneresults_record(self):
        query_str  = """SELECT Id, RepoName
                        FROM CloneResults
                        WHERE Org IS NULL
        """
        return self.client.run_query(query_str)

    def get_reported_clones(self, reponame, similarity):
        query_str  = f"""SELECT Id, RepoFile,
                                RootId, StartLine,
                                EndLine, Similarity,
                                SnippetId, RepoSnippet
                        FROM CloneResults
                        WHERE RepoName='{reponame}'
                              AND Similarity >={similarity}
                        GROUP BY RepoFile, RootId, StartLine, EndLine, Similarity
        """
        cursor = self.client.run_query(query_str)
        return cursor.fetchall()

    def get_clone_reponames(self, similarity=83):
        '''Returns a list of reponames containing clones.
        '''
        query_str = f"""SELECT RepoName
                        FROM CloneResultsNew 
                        WHERE Similarity >={similarity}
                        GROUP BY RepoName
                    """

        reponames = [row['RepoName'] for row in self.client.run_query(query_str)]
        return reponames

    def getCodeBlockVersionVersionIds(self, language):

        query_str = f"""SELECT VersionId 
                       FROM CodeBlockVersion
                       WHERE Language='{language}'"""

        cursor = self.client.run_query(query_str)
        return cursor.fetchall()
    
    def getLineCount(self, snippetid):
        query_str =  f"""SELECT LineCount FROM PostBlockVersion
                        WHERE Id={snippetid}"""
        return self.client.run_query(query_str).fetchone()['LineCount']

    def get_comment(self, comment_id):
        """
        Gets the comment text identified by the given comment ID.

        Args:
            comment_id: The ID to use to retrieve the comment for
        Returns:
            A comment text corresponding to the given ID.
        """
        query = f"SELECT Text FROM Comments WHERE Id={comment_id}"
        return self.execute_and_fetchone(query)['Text']






    
    
    
    
    
