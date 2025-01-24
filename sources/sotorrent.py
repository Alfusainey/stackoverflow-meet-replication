import mysql.connector as mysql

from sources.util import get_db_user, get_db_password


class SOTorrentDB():
    # replace DB_NAME with the name of the database
    def __init__(self, host='127.0.0.1', port= 3306, user=get_db_user(), passwd=get_db_password(), db='DB_NAME'):
        self._connection = mysql.connect(host=host, port=port, user=user, passwd=passwd, db=db, use_unicode=True)
        self._connection.set_charset_collation(charset='utf8', collation='utf8mb4_unicode_ci')
        self._cursor = self._connection.cursor(dictionary=True)

    @property
    def cursor(self):
        return self._cursor
    @property
    def db(self):
        return self._connection

    def fetchall_changed_versions(self, root_post_block_version_id):
        """
        TODO: remove
        Given the RootId of a post block lifespan, fetch all the post blocks in that life span that has actually changed.

        @param root_post_block_version_id:   The root post block in the lifespance.
        """
        query = """
                  SELECT count(*) as edits FROM PostBlockVersion WHERE 
                  RootPostBlockVersionId={} AND (PredEqual IS NULL OR PredEqual = 0)
                """.format(root_post_block_version_id)
        self.cursor.execute(query)
        result = self.cursor.fetchone() # there MUST be at least one record in case there are no edits to the postblocks (it points to itself)
        return result['edits']

    def close(self):
        "Closes the underlying connection to the database"
        self.db.close()

    def run_query(self, query_str):
        """
        Execute the given query against the SOTorrent database and return an iterator over the result.
        """
        self.cursor.execute(query_str)
        return self.cursor
