import sys

from scxkw.redisutil.typed_db import Redis

# TODO: remove the extension of this file (no *.py)
'''
F16: dec base 10 strings up to 16 chars (signed, dot, exponents)
I16: dec base 10 strings up to 16 chars (signed)
A16: string up to 16
etc...
'''

# Load the spreadsheet of death




if __name__ == '__main__':
    # TODO Parse the redis_dbconf file and find the host and port.

    rdb = Redis(host="localhost", port="6379")

    # Is the server alive ?
    if not rdb.ping():
        print('Error: can\'t ping redis DB.')
        sys.exit(1)

    # Load the table
    TABLE_PATH = 'XXX.csv'
    # TODO conf CSV file

    if not find table or other error:
        print('Error: can\'t find keyword description spreadsheet.')
        sys.exit(1)

    with open(TABLE_PATH, 'r') as file:
        file.readline() # Drop the first line
        headers = file.readline('').strip().split(',') # Header line
        all_lines = file.readlines()
    
    sanitized_lines = [l.strip().split('') for l in lines]
    # Find key_col_index 
    for k, s in enumerate(headers):
        if 'FITS' in s:
            key_col_index = k
            break

    dictified_lines = {line[key_col_index]: {subkey: value for (sk, v) in zip(headers, line)} for line in sanitized_lines}
    




    # Parse the table



    # Force a write to disk
