#!/usr/bin/env python

import os, sys, time

from astropy.io import fits
import numpy as np

from scxkw.config import REDIS_DB_HOST, REDIS_DB_PORT, FITS_HEADER_PATH
from scxkw.redisutil.typed_db import Redis


def write_headers(rdb, path):
    # Create the saving directory if it's not there
    if not os.path.isdir(path):
        os.makedirs(path)

    # Fetch the flags !
    file_keys = [k.split(':')[-1] for k in rdb.keys('set:fits:*')]

    # Now get all the keys we need for those flags
    with rdb.pipeline() as pipe:
        for fk in file_keys:
            pipe.smembers('set:fits:' + fk)
        # data_fits_sets is a dict of key: set of 8 char fits keys
        data_fits_sets = {
            fk: data
            for fk, data in zip(file_keys, pipe.execute())
        }

    # Now query all the values !
    kw_keys = list(set.union(*[data_fits_sets[fk] for fk in file_keys]))
    with rdb.pipeline() as pipe:
        for kw_key in kw_keys:
            pipe.hget(kw_key, "value")
            pipe.hget(kw_key, "Description")
        res = pipe.execute()
        # Generate (value, description tuples)
        kw_data = {kw: (val, descr) for kw, val, descr in zip(kw_keys, res[::2], res[1::2])}

    # Now make the dicts on the fly for each file_key, and call the write_one_header
    for file_key in file_keys:
        keyval_dict = {kw: kw_data[kw] for kw in data_fits_sets[file_key]}
        write_one_header(keyval_dict, path, file_key)
    
    return(keyval_dict)


def write_one_header(key_val_dict, folder, name):
    header = fits.Header()
    # Be nice and sort the keys
    for k in sorted(key_val_dict.keys()):
        header[k] = key_val_dict[k]

    # Write to _tmp.fits
    fits.writeto(folder + '/' +  name + '_tmp.fits', np.array([0.0]), header, overwrite=True)
    # Write to _header_dump_tmp.txt
    with open(folder + '/' +  name + '_header_dump_tmp.txt', 'w') as f:
        f.write(str(header))

    # Rename files to final, in hope for atomicity
    os.rename(folder + '/' +  name + '_tmp.fits', folder + '/' +  name + '.fits')
    os.rename(folder + '/' +  name + '_header_dump_tmp.txt', folder + '/' +  name + '_header_dump.txt')

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
            write_headers(rdb, FITS_HEADER_PATH)
            time.sleep(2.0)
    except KeyboardInterrupt:
        sys.exit(0)