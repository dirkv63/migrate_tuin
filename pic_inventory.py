"""
This script will map every filename in Flickr with a filename on pcloud.
"""

import logging
from lib import my_env
from lib import tuin_store

cfg = my_env.init_env("tuin_migrate", __file__)
db = cfg['Main']['db']
logging.info('Start Application')
tuin = tuin_store.init_session(db=db)

# Check for duplicate filenames on Flickr
