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
    file_keys.remove('charis') # set:fits:charis is indicative, we don't want to make this one

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
            pipe.hget(kw_key, "Type")
        res = pipe.execute()
        # Generate (value, description tuples)
        kw_data = {
            kw: (val, descr, fmt)
            for kw, val, descr, fmt in zip(kw_keys, res[::3], res[1::3],
                                           res[2::3])
        }

    # Reformat according to type values!
    # fmt is a valid %-format string stored in the "Type" column of the spreadsheet
    for key in kw_data:
        v, d, fmt = kw_data[key]
        # Some values are None: camera-stream keywords (EXPTIME, FG_SIZE1, ...),
        # and time-keywords (MJD, HST, UTC...) generated upon saving
        if v is not None:
            try:
                if fmt == 'BOOLEAN':
                    v = bool(v)
                elif fmt[-1] == 'd':
                    v = int(fmt % v)
                elif fmt[-1] == 'f':
                    v = float(fmt % v)
                elif fmt[-1] == 's':  # string
                    v = fmt % v
            except:  # Sometime garbage values cannot be formatted properly...
                v = v
                print(f"fits_headers: formatting error on {v}, {d}, {fmt}")

        # Remove the formatter from the dict once used
        kw_data[key] = (v, d)

    # Now make the dicts on the fly for each file_key, and call the write_one_header
    for file_key in file_keys:
        keyval_dict = {kw: kw_data[kw] for kw in data_fits_sets[file_key]}
        write_one_header(keyval_dict, path, file_key)

    return (keyval_dict)


def write_one_header(key_val_dict, folder, name):

    header = fits.Header()

    for k in sorted(key_val_dict.keys()):
        if k in ['BZERO', 'BSCALE']:
            continue
        header[k] = key_val_dict[k]

    hdu = fits.PrimaryHDU(data=np.array([0, 1, 2, 3], dtype=np.float32),
                          header=header)
    # Must be set to not None AFTER creation of the HDU
    # Insert point is in hope to maintain the alphabetical order
    hdu.header.insert('BUNIT',('BSCALE',None,'Real=fits-value*BSCALE+BZERO'))
    hdu.header.insert('BUNIT',('BZERO',None,'Real=fits-value*BSCALE+BZERO'), after=True)
    
    hdul = fits.HDUList(hdus=[hdu])

    # Write to _tmp.fits
    hdul.writeto(folder + '/' + name + '_tmp.fits', overwrite=True)
    # Write to _header_dump_tmp.txt
    with open(folder + '/' + name + '_header_dump_tmp.txt', 'w') as f:
        f.write(str(header))

    # Change permissions to 666
    os.chmod(folder + '/' + name + '_tmp.fits', 0o666)
    os.chmod(folder + '/' + name + '_header_dump_tmp.txt', 0o666)

    # Rename files to final, in hope for atomicity
    os.rename(folder + '/' + name + '_tmp.fits', folder + '/' + name + '.fits')
    os.rename(folder + '/' + name + '_header_dump_tmp.txt',
              folder + '/' + name + '_header_dump.txt')


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
            break
            time.sleep(2.0)
    except KeyboardInterrupt:
        sys.exit(0)
