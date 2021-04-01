#!/usr/bin/env python

import os, sys, time, datetime, csv

from astropy.io import fits
import numpy as np

from scxkw.config import REDIS_DB_HOST, REDIS_DB_PORT, CSV_DUMP_PATH
from scxkw.redisutil.typed_db import Redis

def csv_write(rdb, root_path):
    
    today_folder = root_path + '/' + datetime.datetime.utcnow().strftime("%Y%m%d") + '/logging'
    logfile_path = today_folder + '/keywords_log.csv'
    if not os.path.isdir(today_folder):
        os.makedirs(today_folder)

    # Fetch the data we want
    sorted_keys = sorted(rdb.sunion('set:g2:FITS', 'set:g2:WAV', 'set:g2:AON'))

    # Make dictionary of interest
    with rdb.pipeline() as pipe:
        for key in sorted_keys:
            pipe.hget(key, 'value')
        sorted_values = pipe.execute()

    # csv writer object

    with open(logfile_path,'a') as csvlog:
        writer = csv.DictWriter(csvlog, fieldnames=sorted_keys)
        if os.stat(logfile_path).st_size == 0:
            writer.writeheader()
        writer.writerow(sorted_values)


if __name__ == "__main__":

    # ------------------------------------------------------------------
    #            Configure communication with SCExAO's redis
    # ------------------------------------------------------------------
    rdb = Redis(host=REDIS_DB_HOST, port=REDIS_DB_PORT)
    # Is the server alive ?
    try:
        alive = rdb.ping()
        if not alive:
            raise ConnectionError
    except:
        print('Error: can\'t ping redis DB.')
        sys.exit(1)

    try:
        while True:
            csv_write(rdb, CSV_DUMP_PATH)
            time.sleep(10.0)
    except KeyboardInterrupt:
        sys.exit(0)