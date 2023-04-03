#!/usr/bin/env python

import sys, time
from astropy.coordinates import Angle

from scxkw.config import REDIS_DB_HOST, REDIS_DB_PORT, GEN2HOST
from scxkw.redisutil.typed_db import Redis

from g2base.remoteObjects import remoteObjects as ro

from swmain.hwp.hwpmanager import ask_garde


def gen2_pull(rdb, status_obj):
    # Getting the keys - this code is now repeated, while
    # Originally it was outside the while(True) loop
    fits_keys_to_pull = rdb.sunion('set:g2:FITS', 'set:g2:WAV', 'set:g2:AON')
    with rdb.pipeline() as pipe:
        for key in fits_keys_to_pull:
            pipe.hget(key, 'Gen2 Variable')
            # Why are we getting the values? We're about to overwrite them...
            pipe.hget(key, 'value')
        values = pipe.execute()

    dict_to_pull = {k: v for k, v in zip(values[::2], values[1::2])}

    g2map = rdb.hgetall('map:g2_lookup')

    # ========================
    # NOW PULL FROM GEN2
    # ========================

    pulled_from_gen2 = status_obj.fetch(dict_to_pull)
    pulled_for_pipe = {
        g2map[key]: pulled_from_gen2[key]
        for key in pulled_from_gen2
    }

    # ========================
    # SETTING VALUES
    # ========================

    # Special - we need to re-fetch the IR wollaston position
    wollaston = rdb.hget('X_IRCWOL', 'value')

    with rdb.pipeline() as pipe:
        for key in pulled_for_pipe:
            pipe.hset(key, 'value', pulled_for_pipe[key])

        # =============================
        # FIXING TELESCOPE AND WCS KEYS
        # =============================

        pipe.hset('OBSERVAT', 'value', 'NAOJ    ')
        pipe.hset('INSTRUME', 'value', 'SCExAO  ')
        if 'IN' in wollaston:
            pipe.hset('OBS-MOD', 'value', 'IMAG-PDI')
        else:
            pipe.hset('OBS-MOD', 'value', 'Imaging')

        pipe.hset('RADESYS', 'value', 'FK5     ')
        pipe.hset('TIMESYS', 'value', 'UTC     ')
        pipe.hset('WCS-ORIG', 'value', 'SUBARU')

        # ===================
        # COMPUTE ORIENTATION
        # ===================
        ra = pulled_for_pipe['RA']
        dec = pulled_for_pipe['DEC']
        pad = pulled_for_pipe['D_IMRPAD']
        crval1 = float("%20.8f" % (Angle(ra + "hours").degree))
        crval2 = float("%20.8f" % (Angle(dec + "degrees").degree))
        lonpole = float("%20.1f" % (3.4 - pad))

        # This is actually common to all of SCExAO since we don't really
        # Do off-axis stuff.
        # If extreme high-precision is needed + off-axis pointing.... broken.
        pipe.hset('CRVAL1', 'value', crval1)
        pipe.hset('CRVAL2', 'value', crval2)
        pipe.hset('C2VAL1', 'value', crval1)
        pipe.hset('C2VAL2', 'value', crval2)

        pipe.hset('LONPOLE', 'value', lonpole)

        # ========================
        # WAVEPLATE SPECIFIC KEYS
        # ========================
        pipe.hset('POL-ANG1', 'value', 0)

        POLARIZ1_VALS = {
            0: 'NONE            ',
            56: 'WireGrid(TIR)   ',
            90: 'WireGrid(NIR)   ',
        }
        RETPLAT1_VALS = {
            0: 'NONE            ',
            56: 'HWP(NIR)        ',
        }
        RETPLAT2_VALS = {
            0: 'NONE            ',
            56: 'HWP(TIR)        ',
            90: 'QWP(NIR)        ',
        }
        UKN = 'UNKNOWN         '

        stage1_pos = float(pulled_for_pipe['P_STGPS1'])
        pipe.hset('POLARIZ1', 'value', POLARIZ1_VALS.get(stage1_pos, UKN))

        stage2_pos = float(pulled_for_pipe['P_STGPS2'])
        pipe.hset('RETPLAT1', 'value', RETPLAT1_VALS.get(stage2_pos, UKN))

        stage3_pos = float(pulled_for_pipe['P_STGPS3'])
        pipe.hset('RETPLAT2', 'value', RETPLAT2_VALS.get(stage3_pos, UKN))

        # We do NOT set RET-ANG1/2 from gen2. This is done from direct IRCS feedback.
        # THESE MUST be kept for CHARIS headers in particular.
        val_hwp = ask_garde(hwp_true_qwp_false=True)
        val_qwp = ask_garde(hwp_true_qwp_false=False)
        pipe.hset('RET-ANG1', 'value', val_hwp)
        pipe.hset('RET-ANG2', 'value', val_qwp)

        pipe.execute()


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
            gen2_pull(rdb, status_obj)
            time.sleep(10.0)
    except KeyboardInterrupt:
        sys.exit(0)
