"""
This script copies the required pictures to the public/original directory.
"""


import csv
import logging
import os
from lib import my_env, pcloud_handler

cfg = my_env.init_env("tuin_migrate", __file__)
pc = pcloud_handler.PcloudHandler(cfg)
logging.info('Start Application')
target_id = "3529968922"

fd = cfg['Main']['report_dir']
fn = 'one_match_list'
li = my_env.LoopInfo("Files handled", 50)
with open(os.path.join(fd, '{}.csv'.format(fn)), 'r', newline='') as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
        li.info_loop()
        file_id = row["pcloud_id"][1:]
        pc.copyfile(fileid=file_id, tofolderid=target_id)
li.end_loop()
