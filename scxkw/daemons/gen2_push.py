#!/usr/bin/env python

import sys, time

from scxkw.config import REDIS_DB_HOST, REDIS_DB_PORT, GEN2HOST
from scxkw.redisutil.typed_db import Redis

from g2base.remoteObjects import remoteObjects as ro


def gen2_push(rdb, status_obj):
    # Getting the keys - this code is now repeated, while
    # Originally it was outside the while(True) loop

    # WARNING: We must push more than just SCExAO - NIRWFS and RTS23 too.
    fits_keys_to_push: set[str] = rdb.smembers('set:g2:SCX', 'set:g2:AON')

    # Now Getting the keys
    with rdb.pipeline() as pipe:
        for key in fits_keys_to_push:
            pipe.hget(key, 'Gen2 Variable')
            pipe.hget(key, 'value')
        values = pipe.execute()

    # g2key: value
    dict_to_push = {k: v for k, v in zip(values[::2], values[1::2])}
    dict_to_push_scx = {
        k: v
        for k, v in dict_to_push.items() if k.startswith('SCX')
    }
    dict_to_push_aon = {
        k: v
        for k, v in dict_to_push.items()
        if (k.startswith('AON.IWFS') or k.startswith('AON.NRTS'))
    }

    # =========================
    # NOW PUSH TO GEN2
    # ========================

    status_obj.store_table('SCX', dict_to_push_scx)
    status_obj.store_table('AON', dict_to_push_aon)


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

    # ------------------------------------------------------------------
    #                Configure communication with Gen2
    # ------------------------------------------------------------------

    # Do this once, and once only on process startup
    ro.init([GEN2HOST])

    status_obj = ro.remoteObjectProxy('status')

    try:
        while True:
            gen2_push(rdb, status_obj)
            time.sleep(10.0)
    except KeyboardInterrupt:
        sys.exit(0)
