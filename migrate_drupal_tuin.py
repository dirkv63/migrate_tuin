from lib import my_env
from lib import mysqlstore as drupal
from lib import tuin_store
from lib.tuin_store import *

cfg = my_env.init_env("tuin_migrate", __file__)
db = cfg['Main']['db']
logging.info("Start application")
ds = drupal.DirectConn(cfg)
tuin = tuin_store.init_session(db=db)
parents = ds.get_parent_for_node()
rev_cnt = ds.get_rev_cnt()

node_info = ds.get_node_info()
pg_info = my_env.LoopInfo("Handle Node", 500)
for rec in node_info:
    pg_info.info_loop()
    if rec['nid'] in parents:
        parent_id = parents[rec['nid']]
    else:
        parent_id = -1
    node = Node(
        id=rec['nid'],
        parent_id=parent_id,
        created=rec['created'],
        modified=rec['changed'],
        revcnt=rev_cnt[rec['nid']],
        type=rec['type']
    )
    tuin.add(node)
    content = Content(
        node_id=rec['nid'],
        title=rec['title'],
        body=rec['body_value']
    )
    tuin.add(content)
pg_info.end_loop()

# Get Vocabulary
vocs = ds.get_vocabulary()
for rec in vocs:
    vocabulary = Vocabulary(
        id=rec["vid"],
        name=rec["name"],
        description=rec["description"],
        weight=rec["weight"]
    )
    tuin.add(vocabulary)
# Get Taxonomy Terms
terms = ds.get_terms()
for rec in terms:
    term = Term(
        id=rec["tid"],
        vocabulary_id = rec["vid"],
        name=rec["name"],
        description=rec["description"]
    )
    tuin.add(term)
# Get Taxonomy per Node
tax = ds.get_taxonomy()
for rec in tax:
    taxonomy = Taxonomy(
        node_id=rec["nid"],
        term_id=rec["tid"],
        created=rec["created"]
    )
    tuin.add(taxonomy)
tuin.commit()

# Get Flickr Pictures
flick_rec = ds.get_flickr()
for rec in flick_rec:
    flickr = Flickr(
        node_id=rec["entity_id"],
        photo_id=rec["field_flickr_foto_id"]
    )
    tuin.add(flickr)
    node = tuin.query(Node).filter_by(id=rec["entity_id"]).first()
    node.type = "flickr"

# Get Local Pictures
lopic_rec = ds.get_lophoto()
for rec in lopic_rec:
    lophoto = Lophoto(
        node_id=rec["entity_id"],
        filename=rec["filename"],
        uri=rec["uri"],
        created=rec["timestamp"]
    )
    tuin.add(lophoto)
    node = tuin.query(Node).filter_by(id=rec["entity_id"]).first()
    node.type = "lophoto"

tuin.commit()
ds.close()
logging.info("End application")
