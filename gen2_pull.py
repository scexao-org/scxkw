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

    # Get the keys that we need to pull from Gen2
    fits_keys_to_pull = rdb.sunion('set:g2:FITS','set:g2:WAV','set:g2:AON')
    
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

    # Now Getting the keys
    with rdb.pipeline() as pipe:
        for n, key in enumerate(fits_keys_to_pull):
            pipe.hget(key, 'Gen2 Variable')
            pipe.hget(key, 'value')
        values = pipe.execute()
        
    dict_to_pull = {k: v for k,v in zip(values[::2], values[1::2])}
    
    g2map = rdb.hgetall('map:g2_lookup')
        
    while True:
        # ========================
        # NOW PULL FROM GEN2
        # ========================
        
        pulled_from_gen2 = stobj.fetch(dict_to_pull)
        pulled_for_pipe = {g2map[key]: pulled_from_gen2[key] for key in pulled_from_gen2}

        # ========================
        # SETTING VALUES
        # ========================
        with rdb.pipeline() as pipe:
            for key in pulled_for_pipe:
                pipe.hset(key, 'value', pulled_for_pipe[key])
            
            # ========================
            # WAVEPLATE SPECIFIC KEYS
            # ========================
            
            pipe.hset('POL-ANG1', 'value', 0)
            
            stgps1 = float(pulled_for_pipe['P_STGPS1'])
            if stgps1 == 0:
                pipe.hset('POLARIZ1', 'value', 'NONE')
            elif stgps1 == 56:
                pipe.hset('POLARIZ1', 'value', 'WireGrid(TIR)')
            elif stgps1 == 90:
                pipe.hset('POLARIZ1', 'value', 'WireGrid(NIR)')
            else:
                pipe.hset('POLARIZ1', 'value', 'UNKNOWN')

            stgps2 = float(pulled_for_pipe['P_STGPS2'])
            if stgps2 == 0:
                pipe.hset('RETPLAT1', 'value', 'NONE')
            elif stgps2 == 56:
                pipe.hset('RETPLAT1', 'value', 'HWP(NIR)')
            else:
                pipe.hset('RETPLAT1', 'value', 'UNKNOWN')
                
            stgps3 = float(pulled_for_pipe['P_STGPS3'])
            if stgps3 == 0:
                pipe.hset('RETPLAT2', 'value', 'NONE')
            elif stgps3 == 56:
                pipe.hset('RETPLAT2', 'value', 'HWP(TIR)')
            elif stgps3 == 90:
                pipe.hset('RETPLAT2', 'value', 'QWP(NIR)')
            else:
                pipe.hset('RETPLAT2', 'value', 'UNKNOWN')
            
            pipe.execute()

        time.sleep(10.0)
