'''
    scxkw config file

    To be conveniently imported all around the python scripts
'''


# Redis location - needs to be updated in the conf file as well
try:
    import scxconf
    REDIS_DB_HOST = scconf.REDIS_DB_HOST
    REDIS_DB_PORT = scconf.REDIS_DB_PORT
except:
    REDIS_DB_HOST = '133.40.161.195'
    REDIS_DB_PORT = 6379

import os
# Where to write the data-less fits headers
try:
    FITS_HEADER_PATH = os.environ['MILK_SHM_DIR'] + '/fits/' # Straight to the tmpfs
except:
    FITS_HEADER_PATH = None

# Where to write the data dump every now and then
CSV_DUMP_PATH = '/mnt/tier0/' # + date + csv file name
if not os.path.ismount(CSV_DUMP_PATH):
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
