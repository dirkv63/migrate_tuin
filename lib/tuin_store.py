"""
This module consolidates Database access for the lkb project.
"""

import logging
import os
import sqlite3
from sqlalchemy import Column, Integer, Text, create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker


Base = declarative_base()


class Content(Base):
    """
    Table with Node Title and Node Contents.
    """
    __tablename__ = "content"
    id = Column(Integer, primary_key=True, autoincrement=True)
    node_id = Column(Integer, unique=True)
    title = Column(Text, nullable=False)
    body = Column(Text)


class Flickr(Base):
    """
    Table containing details of the Flickr Picture
    """
    __tablename__ = "flickr"
    id = Column(Integer, primary_key=True, autoincrement=True)
    node_id = Column(Integer, unique=True)
    photo_id = Column(Integer)


class FlickrDetails(Base):
    """
    Table with the whereabouts of the Flickr pictures.
    """
    __tablename__ = "flickrdetails"
    photo_id = Column(Integer, primary_key=True)
    datetaken = Column(Integer, nullable=False)
    title = Column(Text, nullable=False)
    url_c = Column(Text, nullable=False)
    url_l = Column(Text, nullable=False)
    url_m = Column(Text, nullable=False)
    url_n = Column(Text, nullable=False)
    url_o = Column(Text, nullable=False)
    url_q = Column(Text, nullable=False)
    url_s = Column(Text, nullable=False)
    url_sq = Column(Text, nullable=False)
    url_t = Column(Text, nullable=False)
    url_z = Column(Text, nullable=False)


class History(Base):
    """
    Table remembering which node is selected when.
    """
    __tablename__ = "history"
    id = Column(Integer, primary_key=True, autoincrement=True)
    node_id = Column(Integer, nullable=False)
    timestamp = Column(Integer, nullable=False)


class Lophoto(Base):
    """
    Table containing information about the local pictures.
    """
    __tablename__ = "lophoto"
    id = Column(Integer, primary_key=True, autoincrement=True)
    node_id = Column(Integer, nullable=False)
    filename = Column(Text, nullable=False)
    uri = Column(Text, nullable=False)
    created = Column(Integer, nullable=False)


class Node(Base):
    """
    Table containing the Node information of the database.
    Foreign key relation from parent_id to nid is called 'Adjacency list' in SQLAlchemy terminology.
    It is not implemented here because it is not known in which order the records will be loaded.

    A node can be of type Book - with Parent / Child relation, Picture or Message.
    Investigate if two types of Pictures need to be created: local pictures vs Flickr Pictures.
    A message is as a Blog message without pictures.
    """
    __tablename__ = "node"
    id = Column(Integer, primary_key=True)
    parent_id = Column(Integer, nullable=False)
    created = Column(Integer, nullable=False)
    modified = Column(Integer, nullable=False)
    revcnt = Column(Integer)
    type = Column(Text)


class Taxonomy(Base):
    """
    Table containing the taxonomy of a Node. Each term that can be assigned to the node is listed here.
    """
    __tablename__ = "taxonomy"
    id = Column(Integer, primary_key=True, autoincrement=True)
    node_id = Column(Integer, nullable=False)
    term_id = Column(Integer, nullable=False)
    created = Column(Integer, nullable=False)


class Term(Base):
    """
    Table containing the Terms from a Vocabulary.
    """
    __tablename__ = "term"
    id = Column(Integer, primary_key=True)
    vocabulary_id = Column(Integer, nullable=False)
    name = Column(Text, nullable=False)
    description = Column(Text)


class Vocabulary(Base):
    """
    Table containing the Taxonomy Vocabularies. In Drupal, vocabularies were 'Plaats' and 'Planten'.
    """
    __tablename__ = "vocabulary"
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(Text, nullable=False, unique=True)
    description = Column(Text)
    weight = Column(Integer)


class User(Base):
    """
    Table containing the User information.
    """
    __tablename__ = "users"
    id = Column(Integer, primary_key=True, autoincrement=True)
    username = Column(Text, nullable=False, unique=True, index=True)
    password_hash = Column(Text)


class DirectConn:
    """
    This class will set up a direct connection to the database. It allows to reset the database,
    in which case the database will be dropped and recreated, including all tables.
    """

    def __init__(self, config):
        """
        To drop a database in sqlite3, you need to delete the file.
        """
        self.db = config['Main']['db']
        self.dbConn = ""
        self.cur = ""

    def _connect2db(self):
        """
        Internal method to create a database connection and a cursor. This method is called during object
        initialization.
        Note that sqlite connection object does not test the Database connection. If database does not exist, this
        method will not fail. This is expected behaviour, since it will be called to create databases as well.

        :return: Database handle and cursor for the database.
        """
        logging.debug("Creating Datastore object and cursor")
        db_conn = sqlite3.connect(self.db)
        # db_conn.row_factory = sqlite3.Row
        logging.debug("Datastore object and cursor are created")
        return db_conn, db_conn.cursor()

    def rebuild(self):
        # A drop for sqlite is a remove of the file
        os.remove(self.db)
        # Reconnect to the Database
        self.dbConn, self.cur = self._connect2db()
        # Use SQLAlchemy connection to build the database
        conn_string = "sqlite:///{db}".format(db=self.db)
        engine = set_engine(conn_string=conn_string)
        Base.metadata.create_all(engine)


def init_session(db, echo=False):
    """
    This function configures the connection to the database and returns the session object.

    :param db: Name of the sqlite3 database.

    :param echo: True / False, depending if echo is required. Default: False

    :return: session object.
    """
    conn_string = "sqlite:///{db}".format(db=db)
    engine = set_engine(conn_string, echo)
    session = set_session4engine(engine)
    return session


def set_engine(conn_string, echo=False):
    engine = create_engine(conn_string, echo=echo)
    return engine


def set_session4engine(engine):
    session_class = sessionmaker(bind=engine)
    session = session_class()
    return session
