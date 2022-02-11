#!/usr/bin/env python

import pickle
import logging
import socket
import os, sys


import astropy.io.fits as pyfits


def main():
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)-15s %(levelno)-2s %(message)s')

    if len(sys.argv) == 1:
        PORT = 18447
    else:
        PORT = int(sys.argv[1])

    s = socket.create_connection((os.getenv('IP_SC2'), PORT), 5)
    s.send(b'hdr\n')

    allData = b''
    while True:
        chunk = s.recv(1024)
        if not chunk:
            break
        allData = allData + chunk

    rawHdr = pickle.loads(allData)

    hdr = pyfits.Header(rawHdr)
    print('\n'.join([str(l) for l in rawHdr]))


if __name__ == "__main__":
    main()
