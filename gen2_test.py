#!/usr/bin/env python

import sys

from scxkw.config import *
from scxkw.redisutil.typed_db import Redis


if __name__ == '__main__':

    rdb = Redis(host=REDIS_DB_HOST, port=REDIS_DB_PORT)

    # Is the server alive ?
    try:
        alive = rdb.ping()
        if not alive:
            raise ConnectionError
    except:
        print('Error: can\'t ping redis DB.')
        sys.exit(1)

    # Get the keys that we need to push to G2
    fits_keys_to_push = rdb.smembers('set:g2:SCX')


    # ========================
    # SETTING DUMMY VALUES
    # ========================
    # Problem: the database isn't really used yet... we need to set values to those keys
    # I'm counting value
    with rdb.pipeline() as pipe:
        for n, key in enumerate(fits_keys_to_push):
            pipe.hset(key, 'value', '%d' % n)
        pipe.execute()
    # ========================

    # Now Getting the keys
    with rdb.pipeline() as pipe:
        for n, key in enumerate(fits_keys_to_push):
            pipe.hget(key, 'Gen2 Variable')
            pipe.hget(key, 'value')
        values = pipe.execute()

    dict_to_push = {k: v for k,v in zip(values[::2], values[1::2])}

    print(dict_to_push)

    # ========================
    # NOW PUSH TO GEN2
    # ========================