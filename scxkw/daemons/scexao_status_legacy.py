#!/usr/bin/env python

import os, sys, time

from astropy.io import fits
import numpy as np

from scxkw.config import REDIS_DB_HOST, REDIS_DB_PORT
from scxkw.redisutil.typed_db import Redis

LEGACY_EXEC = '/home/scexao/Instrument-Control-Main/src/SCExAO_status/scexaostatus'

def scexaostatus_legacy_update(rdb):

    # Getall the mapping shm_lookup / fits keys
    keys_shm = rdb.smembers('set:has_shm')

    # Get
    with rdb.pipeline() as pipe:
        for key in keys_shm:
            pipe.hget(key, 'Name in SHM')
            pipe.hget(key, 'value')
            pipe.hget(key, 'color')
        ret = pipe.execute()

    names = {k: v for (k, v) in zip(keys_shm, ret[0::3])}
    values = {k: v for (k, v) in zip(keys_shm, ret[1::3])}
    colors = {k: v for (k, v) in zip(keys_shm, ret[2::3])}

    # Set
    for key in keys_shm:
        name, value, color = names[key], values[key], colors[key]
        if color is None:
            command = LEGACY_EXEC + ' set ' + name + ' "' + str(value) + '"'
            os.system(command)
        else:
            command = LEGACY_EXEC + ' set ' + name + ' "' + str(value) + '" ' + color
            os.system(command)



if __name__ == "__main__":

    # ------------------------------------------------------------------
    #            Configure communication with SCExAO's redis
    # ------------------------------------------------------------------
    
    # Is the server alive ?
    try:
        rdb = Redis(host=REDIS_DB_HOST, port=REDIS_DB_PORT)
        alive = rdb.ping()
        if not alive:
            raise ConnectionError
    except:
        print('Error: can\'t ping redis DB.')
        sys.exit(1)

    try:
        while True:
            scexaostatus_legacy_update(rdb)
            time.sleep(2.0)
    except KeyboardInterrupt:
        sys.exit(0)
