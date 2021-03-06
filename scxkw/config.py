'''
    scxkw config file

    To be conveniently imported all around the python scripts
'''

# Redis location - needs to be updated in the conf file as well
REDIS_DB_HOST = '133.40.161.194'
REDIS_DB_PORT = 6379

import os
FITS_LOCATION = os.environ['MILK_SHM_DIR'] # Straight to the tmpfs

REDIS_CONF_PATH='/home/scexao/src/scxkw/conf/redis_dbconf.conf'
#REDIS_CONF_PATH='/home/vdeo/src/scxkw/conf/redis_dbconf.conf'
# Actually we need to use a TSV instead of a CSV, because the data contains commas...
KEYWORD_CSV_PATH='/home/scexao/src/scxkw/conf/scxkw.tsv'
