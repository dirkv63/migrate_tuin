# Consolidation for mysql related functions
# Some information at
# https://stackoverflow.com/questions/24475645/sqlalchemy-one-to-one-relation-primary-as-foreign-key

# import logging
import pymysql


class DirectConn:
    """
    This class will set up a direct connection to MySQL.
    For Drupal lkb this seems a better idea than emulating the database in a SQLAlchemy way.
    """

    def __init__(self, cfg):
        """
        The init procedure will set-up Connection to the Database Server, but not to a specific database.

        :param cfg: Link to the configuration object.
        """

        mysql_conn = dict(
            host=cfg['drupaltuin']['host'],
            port=int(cfg['drupaltuin']['port']),
            user=cfg['drupaltuin']["user"],
            passwd=cfg['drupaltuin']['passwd'],
            db=cfg['drupaltuin']['db']
        )
        self.conn = pymysql.connect(**mysql_conn)
        self.cur = self.conn.cursor(pymysql.cursors.DictCursor)

    def close(self):
        self.conn.close()

    def get_flickr(self):
        query = "SELECT * FROM field_data_field_flickr_foto"
        self.cur.execute(query)
        return self.cur.fetchall()

    def get_lophoto(self):
        query = """
        SELECT p.entity_id, fm.filename, fm.uri, fm.timestamp
        FROM field_data_field_photo p
        LEFT JOIN file_managed fm on fm.fid=p.field_photo_fid
        """
        self.cur.execute(query)
        return self.cur.fetchall()

    def get_menu_links(self):
        query = """
        SELECT menu_name, mlid, plid, depth, link_path
        FROM menu_links
        WHERE link_path like 'node%'
        ORDER BY depth desc
        """
        self.cur.execute(query)
        res = self.cur.fetchall()
        return res

    def get_node_info(self):
        """
        This method will collect information related to a node.

        :return: All nodes with fields nid, title, body, created, changed and type.
        """
        # Note: nid: 880 is page for 'Latest items', that need to be ignored.
        query = """
        SELECT n.nid, n.title, fdb.body_value, n.created, n.changed, n.type
        FROM node n
        LEFT JOIN field_data_body fdb ON fdb.entity_id = n.nid
        """
        self.cur.execute(query)
        res = self.cur.fetchall()
        return res

    def get_node_in_menu(self):
        query = """
        SELECT ml.menu_name, ml.depth, ml.link_path, n.title
        FROM menu_links as ml, book as b, node as n
        WHERE ml.mlid = b.mlid
          AND b.nid = n.nid
        ORDER BY menu_name, depth
        """
        self.cur.execute(query)
        return self.cur.fetchall()

    def get_parent_for_node(self):
        """
        This method will get parents for all nodes.

        :return: dictionary with key node id and value parent id.
        """
        query = """
        SELECT b.nid as nid, p.nid as parent
        FROM book b
        INNER JOIN menu_links ml ON ml.mlid=b.mlid
        INNER JOIN menu_links pl ON pl.mlid=ml.plid
        INNER JOIN book p ON p.mlid = pl.mlid
        WHERE ml.menu_name like 'book%'
        """
        self.cur.execute(query)
        res = self.cur.fetchall()
        parents = {}
        for rec in res:
            parents[rec['nid']] = rec['parent']
        return parents

    def get_rev_cnt(self):
        """
        This method will return the revision count per node.

        :return: dictionary with key node id and value revisions for the node. 1 = initial post.
        """
        query = "SELECT nid, count(*) as cnt FROM node_revision group by nid"
        self.cur.execute(query)
        res = self.cur.fetchall()
        revCnt = {}
        for rec in res:
            revCnt[rec['nid']] = rec['cnt']
        return revCnt

    def get_taxonomy(self):
        """
        This method will get the taxonomy per node.

        :return: Taxonomy.
        """
        query = "SELECT * FROM taxonomy_index"
        self.cur.execute(query)
        return self.cur.fetchall()

    def get_terms(self):
        """
        This method will get the terms of the vocabulary.

        :return: Terms.
        """
        query = "SELECT * FROM taxonomy_term_data"
        self.cur.execute(query)
        return self.cur.fetchall()

    def get_vocabulary(self):
        """
        This method will get the vocabulary.

        :return: Vocabulary.
        """
        query = "SELECT * FROM taxonomy_vocabulary"
        self.cur.execute(query)
        return self.cur.fetchall()
