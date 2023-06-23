'''
    scxkw config file

    To be conveniently imported all around the python scripts
'''


# Redis location - needs to be updated in the conf file as well
try:
    import scxconf
    REDIS_DB_HOST = scxconf.REDIS_DB_HOST
    REDIS_DB_PORT = scxconf.REDIS_DB_PORT
except: # Fallback pre-scxconf
    REDIS_DB_HOST = '133.40.161.193'
    REDIS_DB_PORT = 6379

import os
# Where to write the data-less fits headers
try:
    FITS_HEADER_PATH = os.environ['MILK_SHM_DIR'] + '/fits/' # Straight to the tmpfs
except:
    FITS_HEADER_PATH = None

# Where to write the data dump every now and then
CSV_DUMP_PATH = '/home/scexao/logdir/' # + date + csv file name
if not os.environ.get("WHICHCOMP", "0") == "6":
    # scexao2 fallback !
    CSV_DUMP_PATH = '/media/data'

REDIS_CONF_PATH='/home/scexao/src/scxkw/conf/redis_dbconf.conf'
#REDIS_CONF_PATH='/home/vdeo/src/scxkw/conf/redis_dbconf.conf'
# Actually we need to use a TSV instead of a CSV, because the data contains commas...
KEYWORD_CSV_PATH='/home/scexao/src/scxkw/conf/scxkw.tsv'
#KEYWORD_CSV_PATH='/home/vdeo/src/scxkw/conf/scxkw.tsv'


# for testing purposes, please use the simulator
#GEN2HOST = 'g2sim.subaru.nao.ac.jp'
# actual summit
GEN2HOST = 'g2ins1.sum.subaru.nao.ac.jp'

TEST = False
if TEST:
    GEN2PATH_PRELIM = "/tmp/ARCHIVE0"
    GEN2PATH_NODELETE = "/tmp/ARCHIVE1"
    GEN2PATH_OKDELETE = "/tmp/ARCHIVE2"
else:
    GEN2PATH_PRELIM = "/mnt/tier1/PRE_ARCHIVE_DATA/" # <- ARCHIVE in here
    GEN2PATH_NODELETE = "/mnt/tier1/ARCHIVED_DATA/" # <- after sync and deinterleave
    GEN2PATH_OKDELETE = "/mnt/tier1/2_ARCHIVED_DATA/" # <- after frameIDs and fpack, etc.

# streamname: archive letter mapping
CAMIDS = {
    "a_gen2": "SCXB", # Apapane
    "p_gen2": "SCXC", # Palila
    #"first": "F", # FIRST (andor and hamamatsu, careful)
    #"glint": "G", # GLINT
    #"ocam2d": "P", # Reno
    #"ircam1": "R", # Rajni
    #"vcam0": "V", # Vampires
    #"vcam1": "V", # Vampires
    "v_gen2": "VMPA",
    "v_gen2": "VMPA",
}


class MAGIC_BOOL_STR:
    TRUE = '#TRUE#'
    FALSE = '#FALSE#'

    TUPLE = ('#FALSE#', '#TRUE#')

class MAGIC_HW_STR:
    HEIGHT = '#HEIGHT#'
    WIDTH = '#WIDTH#'

def redis_check_enabled():
    try:
        from scxkw.config import REDIS_DB_HOST, REDIS_DB_PORT, GEN2HOST
        from scxkw.redisutil.typed_db import Redis
        RDB = Redis(host=REDIS_DB_HOST, port=REDIS_DB_PORT)
        HAS_REDIS = True
    except:
        RDB = None
        HAS_REDIS = False

    return RDB, HAS_REDIS
