"""
This script maps each picture title in the flickr details table with files on pcloud.
Output is one list with files that cannot be found, a dictionary with files for which there is one match and a
dictionary with files for which there are multiple matches.
"""


import csv
import logging
import os
from lib import my_env
from lib import localstore

cfg = my_env.init_env("tuin_migrate", __file__)
db = cfg['Main']['db']
logging.info('Start Application')
tuindb = localstore.SqliteUtils(cfg['Main']['tuindb'])
pclouddb = localstore.SqliteUtils(cfg['Main']['pclouddb'])

# Get distinct filenames from tuindb.flickrdetails
query = "SELECT distinct(title) FROM flickrdetails"
res = tuindb.get_query(query)
titles = [rec['title'] for rec in res]

no_match = []
one_match = {}
multi_match = {}
# Find each title in pclouddb
cnt = my_env.LoopInfo("Titles", 50)
for title in titles:
    cnt.info_loop()
    query = """
    SELECT name, contenttype, path, size, pcloud_id
    FROM files 
    WHERE name like '{}.%'
      AND (path like '%tuin%' OR path like '%Motorola%')
    """.format(title)
    res = pclouddb.get_query(query)
    if len(res) == 0:
        no_match.append(title)
    elif len(res) == 1:
        one_match[title] = res[0]
    else:
        multi_match[title] = [rec for rec in res]
cnt.end_loop()

# Summary Report
msg = "{total} distinct titles, {one} with single match, {multiple} with multiple matches, {no} with no matches"\
    .format(total=len(titles), one=len(one_match), multiple=len(multi_match), no=len(no_match))
logging.info(msg)

# Print output reports
# Convert to list of dictionaries for csv DictWriter
no_match_list = [dict(title=title) for title in no_match]
one_match_list = [dict(one_match[title]) for title in one_match]
multi_match_list = []
for title in multi_match:
    for rec in multi_match[title]:
        rec_dict = dict(rec)
        rec_dict['title'] = title
        multi_match_list.append(rec_dict)

fd = cfg['Main']['report_dir']
for fn in ['no_match_list', 'one_match_list', 'multi_match_list']:
    res = eval(fn)
    if len(res) > 0:
        fieldnames = [key for key in res[0].keys()]
        with open(os.path.join(fd, '{}.csv'.format(fn)), 'w', newline='') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames)
            writer.writeheader()
            writer.writerows(eval(fn))
