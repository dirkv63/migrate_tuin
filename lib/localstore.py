"""
This module consolidates sqlite database functions.
"""

import logging
import os
import sqlite3


class SqliteUtils:
    """
    This class consolidates a number of Database utilities for sqlite, such as rebuild of the database or rebuild of a
    specific table.
    """

    def __init__(self, dbfile=None):
        """
        To drop a database in sqlite3, you need to delete the file.

        :param dbfile: Database filename to be loaded.
        """
        self.db = dbfile
        self.dbConn, self.cur = self._connect2db()

    def _connect2db(self):
        """
        Internal method to create a (sqlalchemy) database connection.
        Note that sqlite connection object does not test the Database connection. If database does not exist, this
        method will not fail. This is expected behaviour, since it will be called to create databases as well.

        :return: SqlAlchemy Database handle for the database.
        """
        if os.path.isfile(self.db):
            db_conn = sqlite3.connect(self.db)
            db_conn.row_factory = sqlite3.Row
            logging.debug("Datastore object and cursor are created")
            return db_conn, db_conn.cursor()
        else:
            logging.error("{} is not a file, DB Connection not created.".format(self.db))
            return False, False

    def create_table(self, tablename, row):
        """
        This method will create a table where the fields are the row list.
        :param tablename: Name of the table
        :param row: Comma separated list with field names. First field must be Node.
        :return: Length of the row.
        """
        query = "DROP TABLE IF EXISTS {tn}".format(tn=tablename)
        logging.debug("Drop Query: {q}".format(q=query))
        self.dbConn.execute(query)
        fieldlist = ["`{field}` text".format(field=field) for field in row]
        query = "CREATE TABLE {tn} ({fields})".format(tn=tablename, fields=", ".join(fieldlist))
        logging.debug("Create Query: {q}".format(q=query))
        self.dbConn.execute(query)
        logging.info("Table {tn} is built".format(tn=tablename))
        return len(row)

    def get_query(self, query):
        """
        This method will get a query and return the result of the query.

        :param query:
        :return:
        """
        self.cur.execute(query)
        res = self.cur.fetchall()
        return res

    def get_table(self, tablename):
        """
        This method will return the table as a list of named rows. This means that each row in the list will return
        the table column values as an attribute. E.g. row.name will return the value for column name in each row.

        :param tablename:
        :return:
        """
        query = "SELECT * FROM {t}".format(t=tablename)
        self.cur.execute(query)
        res = self.cur.fetchall()
        return res

    def insert_row(self, tablename, rowdict):
        """
        This method will insert a dictionary row into a table.

        :param tablename: Table Name to insert data into
        :param rowdict: Row Dictionary
        :return:
        """
        columns = ", ".join("`" + k + "`" for k in rowdict.keys())
        values_template = ", ".join(["?"] * len(rowdict.keys()))
        query = "insert into {tn} ({cols}) values ({vt})".format(tn=tablename, cols=columns, vt=values_template)
        values = tuple(rowdict[key] for key in rowdict.keys())
        logging.debug("Insert query: {q}".format(q=query))
        self.dbConn.execute(query, values)
        self.dbConn.commit()
        return

    def insert_rows(self, tablename, rowdict):
        """
        This method will insert a list of dictionary rows into a table.

        :param tablename: Table Name to insert data into
        :param rowdict: List of Dictionary Rows
        :return:
        """
        if len(rowdict) > 0:
            columns = ", ".join("`" + k + "`" for k in rowdict[0].keys())
            values_template = ", ".join(["?"] * len(rowdict[0].keys()))
            query = "insert into {tn} ({cols}) values ({vt})".format(tn=tablename, cols=columns, vt=values_template)
            logging.debug("Insert query: {q}".format(q=query))
            # cnt = my_env.LoopInfo(tablename, 50)
            for line in rowdict:
                # cnt.info_loop()
                logging.debug(line)
                values = tuple(line[key] for key in line.keys())
                try:
                    self.dbConn.execute(query, values)
                except sqlite3.IntegrityError:
                    logging.error("Integrity Error on query {q} with values {v}".format(q=query, v=values))
            # cnt.end_loop()
            self.dbConn.commit()
        return
