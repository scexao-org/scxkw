#!/usr/bin/env python

import os, sys, time, datetime, csv

from astropy.io import fits
import numpy as np

from scxkw.config import REDIS_DB_HOST, REDIS_DB_PORT, CSV_DUMP_PATH
from scxkw.redisutil.typed_db import Redis


def csv_write(rdb, root_path):

    today_folder = root_path + '/' + datetime.datetime.utcnow().strftime(
        "%Y%m%d") + '/logging'
    logfile_path = today_folder + '/keywords_log.tsv'
    if not os.path.isdir(today_folder):
        os.makedirs(today_folder)

    # Fetch the data we want
    sorted_keys = sorted(rdb.sunion('set:g2:FITS', 'set:g2:WAV', 'set:g2:AON', 'set:kw:X'))

    # Make dictionary of interest
    with rdb.pipeline() as pipe:
        for key in sorted_keys:
            pipe.hget(key, 'value')
        sorted_values = pipe.execute()

    # Add saving timestamp
    sorted_keys = ['WRITTIME'] + sorted_keys
    sorted_values = [datetime.datetime.utcnow().strftime('%Y%m%d-%H:%M:%S')
                     ] + sorted_values
    data_dict = {k: v for k, v in zip(sorted_keys, sorted_values)}

    # OOOOOOK what happens if the header changed !
    if os.path.isfile(logfile_path):
        with open(logfile_path, 'r') as csvlog:
            reader = csv.DictReader(csvlog, delimiter='\t')
            previous_fields = reader.fieldnames

        diff = set.symmetric_difference(set(sorted_keys), set(previous_fields))
        if len(diff) > 0:
            print('Warning: TSV keys have changed !')
            print('Ambiguous keys (disappeared/appeared):')
            print(diff)
            logfile_bak_path = today_folder + '/keywords_log_' + datetime.datetime.utcnow(
            ).strftime('%H:%M:%S') + '.tsv'
            print('Moving current file to ' + logfile_bak_path +
                  ' and starting a new one.')
            os.rename(logfile_path, logfile_bak_path)

    with open(logfile_path, 'a') as csvlog:
        # csv writer object
        writer = csv.DictWriter(csvlog, fieldnames=sorted_keys, delimiter='\t')
        if os.stat(logfile_path).st_size == 0:
            writer.writeheader()
        writer.writerow(data_dict)


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
