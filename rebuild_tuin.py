"""
This procedure will rebuild the sqlite lkb database
"""

import logging
from lib import my_env
from lib import tuin_store

cfg = my_env.init_env("tuin_migrate", __file__)
logging.info("Start application")
tuin = tuin_store.DirectConn(cfg)
tuin.rebuild()
logging.info("sqlite tuin rebuild")
logging.info("End application")
