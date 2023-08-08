from __future__ import annotations
import typing as typ

import logging

logg = logging.getLogger(__name__)

import os, shutil
from datetime import datetime
from pathlib import Path
from astropy.io import fits
import numpy as np
import time

from .logshim_txt_parser import LogshimTxtParser
from .fix_header import fix_header_times

from .file_obj import MotherOfFileObj


def object_buff_to_uint_buff(object_buff: np.ndarray) -> np.ndarray:
    '''
        object_buff should be a n x 2 object array
        object_buff[:, 0] should contain strings of len < 256
        object_buff[:, 1] should contain an int
    '''
    n = object_buff.shape[0]
    buff_uint8 = np.zeros((n, 264), dtype=np.uint8)
    buff_uint64 = buff_uint8.view(dtype=np.uint64)

    for ii in range(n):
        buff_uint64[ii, :-1] = np.frombuffer(b'%-256s' % object_buff[ii, 0].encode('ascii'), dtype=np.uint64)
        buff_uint64[ii, -1] = object_buff[ii, 1]

    return buff_uint8

def uint_buff_to_object_buff(uint8_buff: np.ndarray) -> np.ndarray:
    '''
        uint_buff should be a n x 264 uint8 array
        the first 256 bytes encode the file name in ASCII
        the last 8 bytes encode an integer.
    '''
    n = uint8_buff.shape[0]
    uint64_buff = uint8_buff.view(dtype=np.uint64)

    obj_arr = np.zeros((n, 2), dtype=object)

    for ii in range(n):
        obj_arr[ii, 0] = uint8_buff[ii, :-8].tobytes().strip().decode('ascii')
        obj_arr[ii, 1] = uint64_buff[ii, -1]

    return obj_arr


class FrameListFitsFileObj(MotherOfFileObj):
    
    def _initial_name_check(self) -> None:
        if not self.full_filepath.is_absolute():
            message = f"MotherOfFileObj::_initial_name_check: not an absolute path - {str(self.full_filepath)}"
            logg.critical(message)
            raise AssertionError(message)

        if not '.fitsframes' in self.full_filepath.suffixes:
            message = f"MotherOfFileObj::_initial_name_check: not .fitsframes[.fz|.gz] - {str(self.full_filepath)}"
            logg.critical(message)
            raise AssertionError(message)
        
    def _write_data_to_disk(self, filename: Path) -> None:

        # typ.Iterable[typ.Tuple[str, int]]
        _data = object_buff_to_uint_buff(self.constr_data)

        # We add an axis to still have the cube length as NAXIS3
        fits.writeto(filename, _data[:, None, :], self.fits_header)

    def get_nframes(self) -> int:
        # Assume logshim format... n_frames = last axis
        _NAXIS3: int = self.fits_header['NAXIS3'] # type: ignore
        return _NAXIS3

    def _ensure_data_loaded(self):
        if self.data is None:
            if self.is_on_disk:
                _data: np.ndarray = fits.getdata(self.full_filepath, memmap=False)
                self.data = uint_buff_to_object_buff(_data)
            else:
                self.data = self.constr_data

    def _merge_data_after(self, other_data):
        np.concatenate((self.data, other_data), axis=0)