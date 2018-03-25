"""
This script will collect all details from Flickr for the Tuin site.
"""

import flickrapi
import logging
import time
from calendar import timegm
from lib import my_env
from lib import tuin_store
from lib.tuin_store import FlickrDetails


def search_photos(tuindb, flickr_obj, flickr_user, curr_page):
    """
    Search for all photos on this page. Handle all photos. Remember number of pages.

    :param tuindb: sqlalchemy connection to tuin DB.

    :param flickr_obj:

    :param flickr_user:

    :param curr_page:

    :return: pages
    """
    attribs = ["title", "url_sq", "url_t", "url_s", "url_q", "url_m", "url_n", "url_z", "url_c", "url_l", "url_o"]
    log_msg = "Now handling page " + str(curr_page)
    logging.info(log_msg)
    extras = "date_taken, url_sq, url_t, url_s, url_q, url_m, url_n, url_z, url_c, url_l, url_o"
    photos = flickr_obj.photos_search(user_id=flickr_user, page=curr_page, extras=extras)
    coll = photos["photos"]["photo"]
    my_loop = my_env.LoopInfo("Photos for page {p}".format(p=curr_page), 20)
    for photo in coll:
        my_loop.info_loop()
        params = dict(photo_id=int(photo["id"]))
        utc_time = time.strptime(photo["datetaken"], "%Y-%m-%d %H:%M:%S")
        params["datetaken"] = timegm(utc_time)
        for k in attribs:
            params[k] = photo[k]
        flickrdetail = FlickrDetails(**params)
        tuindb.add(flickrdetail)
    my_loop.end_loop()
    tuindb.commit()
    return int(photos["photos"]["pages"])


cfg = my_env.init_env("tuin_migrate", __file__)
db = cfg['Main']['db']
logging.info('Start Application')
tuin = tuin_store.init_session(db=db)
# Connect to Flickr
api_key = cfg['flickr']['api_key']
api_secret = cfg['flickr']['api_secret']
user_id = cfg['flickr']['user_id']
flickr = flickrapi.FlickrAPI(api_key, api_secret, format="parsed-json")
# Now collect all pictures
page_nr = 0
pages = 1
while page_nr < pages:
    page_nr += 1
    pages = search_photos(tuin, flickr, user_id, page_nr)
    if page_nr > pages:
        logging.error("Page number " + str(page_nr) + " > Pages " + str(pages))
        break
