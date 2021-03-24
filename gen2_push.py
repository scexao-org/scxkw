#!/usr/bin/env python

import sys,time

from scxkw.config import *
from scxkw.redisutil.typed_db import Redis
from g2base.remoteObjects import remoteObjects as ro


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

    # Get the keys that we need to push to Gen2
    fits_keys_to_push = rdb.smembers('set:g2:SCX')
    
    # ------------------------------------------------------------------
    #                Configure communication with Gen2
    # ------------------------------------------------------------------
    
    # for testing purposes, please use the simulator
    #gen2host = 'g2sim.subaru.nao.ac.jp'
    # actual summit
    gen2host = 'g2ins1.sum.subaru.nao.ac.jp'
    
    # Do this once, and once only on process startup
    ro.init([gen2host])
    
    stobj = ro.remoteObjectProxy('status')

    while True:
        # Now Getting the keys
        with rdb.pipeline() as pipe:
            for n, key in enumerate(fits_keys_to_push):
                pipe.hget(key, 'Gen2 Variable')
                pipe.hget(key, 'value')
            values = pipe.execute()
                
        dict_to_push = {k: v for k,v in zip(values[::2], values[1::2])}
        
        
        # =========================
        # NOW PUSH TO GEN2
        # ========================
        
        stobj.store_table('SCX', dict_to_push)

        time.sleep(10.0)
