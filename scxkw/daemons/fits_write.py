#!/usr/bin/env python
from __future__ import annotations

import typing as typ

import os, sys, time
from pathlib import Path

from astropy.io import fits
import numpy as np

from scxkw.config import REDIS_DB_HOST, REDIS_DB_PORT, FITS_HEADER_PATH
from scxkw.redisutil.typed_db import Redis, ScxkwValueType

import logging
logger = logging.getLogger(__name__)

from ..tools import fits_format

def write_headers(rdb, path) -> dict[str, fits.Card]:
    """
    Authors: Vincent Deo, Miles Lucas
    """
    # assert path is a Path
    path = Path(path)
    # Create the saving directory if it's not there
    if not path.is_dir():
        path.mkdir(parents=True)

    # Fetch the flags !
    file_keys = [k.split(':')[-1] for k in rdb.keys('set:fits:*')]
    # set:fits:charis is indicative, we don't want to make this one
    file_keys.remove('charis') 

    # Now get all the keys we need for those flags
    with rdb.pipeline() as pipe:
        for key in file_keys:
            pipe.smembers(f"set:fits:{key}")
        results = pipe.execute()
        # data_fits_sets is a dict of key: set of 8 char fits keys
        data_fits_sets = {k: d for k, d in zip(file_keys, results)}

    # Now query all the values !
    kw_keys = list(set.union(*[data_fits_sets[fk] for fk in file_keys]))
    with rdb.pipeline() as pipe:
        for kw_key in kw_keys:
            pipe.hget(kw_key, "value")
            pipe.hget(kw_key, "Description")
            pipe.hget(kw_key, "Type")
        res = pipe.execute()
        # Generate (value, description tuples)
        
        kw_data: dict[str, tuple[typ.Any, str, str]] = {
            kw: (val, descr, fmt)
            for kw, val, descr, fmt in zip(kw_keys, res[::3], res[1::3],
                                           res[2::3])
        }

    # Reformat according to type values!
    # fmt is a valid %-format string stored in the "Type" column of the spreadsheet
    for key in kw_data:
        value, comment, fmt = kw_data[key]
        kw_data[key] = fits_format.format_values(value, fmt, comment)

    # Now make the dicts on the fly for each file_key, and call the write_one_header
    for file_key in file_keys:
        cards_dict = {kw: kw_data[kw] for kw in data_fits_sets[file_key]}
        write_one_header(cards_dict, path, file_key)

    return cards_dict

def _isnt_structural_keyword(key):
    # Determine if keyword is a structural FITS keyword
    predicate = key in ("SIMPLE", "BITPIX", "BZERO", "BSCALE", "END") or key.startswith("NAXIS")
    return not predicate

def write_one_header(kw_dict: dict[str, fits.Card], folder, name):
    # generate Header card-by-card
    header = fits.Header()
    for k in sorted(filter(_isnt_structural_keyword, kw_dict.keys())):
        value, comment = kw_dict[k]
        header[k] = value, comment

    hdu = fits.PrimaryHDU(data=fits_format.NULL_DATA, header=header)
    # Must be set to not None AFTER creation of the HDU
    # Insert point is in hope to maintain the alphabetical order
    hdu.header.insert("BUNIT", ("BSCALE", None, "Real=fits-value*BSCALE+BZERO"))
    hdu.header.insert("BUNIT", ("BZERO", None, "Real=fits-value*BSCALE+BZERO"), after=True)
    
    # Write to _tmp.fits
    root = Path(folder)
    tmp_path = root / f"{name}_tmp.fits"
    logger.debug(f"Saving data to temporary path {tmp_path}")
    hdu.writeto(tmp_path, overwrite=True)
    # Write to _header_dump_tmp.txt
    with open(root / f"{name}_header_dump_tmp.txt", "w") as fh:
        fh.write(header.tostring())

    # Change permissions to 666
    os.chmod(tmp_path, 0o666)
    os.chmod(root / f"{name}_header_dump_tmp.txt", 0o666)

    # Rename files to final, in hope for atomicity
    os.rename(tmp_path, root / f"{name}.fits")
    os.rename(root / f"{name}_header_dump_tmp.txt",
              root / f"{name}_header_dump.txt")


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
    except Exception:
        print('Error: can\'t ping redis DB.')
        sys.exit(1)

    try:
        # keep trying till it works!!
        while True:
            write_headers(rdb, FITS_HEADER_PATH)
            break
    except KeyboardInterrupt:
        sys.exit(0)
