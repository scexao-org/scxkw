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

class FitsFileObj(MotherOfFileObj):

    def _initial_name_check(self) -> None:
        if not self.full_filepath.is_absolute():
            message = f"FitsFileObj::_initial_name_check: not an absolute path - {str(self.full_filepath)}"
            logg.critical(message)
            raise AssertionError(message)

        if not '.fits' in self.full_filepath.suffixes:
            message = f"FitsFileObj::_initial_name_check: not .fits - {str(self.full_filepath)}"
            logg.critical(message)
            raise AssertionError(message)
        
    def _write_data_to_disk(self, filename: Path) -> None:
        # Miles: I had to hack this in on 2023/09/14 reducing data in /mnt/tier0/20230914
        self.constr_data = np.asarray(self.constr_data)
        # </Miles>
        logg.warning(
            f'FitsFileObj::write_to_disk - Writing {str(self.full_filepath)} '
            f'({self.get_nframes()}x[{self.constr_data.shape[1]}x{self.constr_data.shape[2]}])'
        )
        fits.writeto(filename, self.constr_data, self.fits_header)

    def get_nframes(self) -> int:
        # Assume logshim format... n_frames = last axis
        _NAXIS3: int = self.fits_header['NAXIS3'] # type: ignore
        return _NAXIS3
    
    def _ensure_data_loaded(self):
        if self.data is None:
            if self.is_on_disk:
                self.data = fits.getdata(self.full_filepath, memmap=False)
            else:
                self.data = self.constr_data

    def _merge_data_after(self, other_data):
        np.concatenate((self.data, other_data), axis=0)


class FitsFzFileObj(MotherOfFileObj):

    HDU_POS = 1 # We're working on the first extension

    def _initial_name_check(self) -> None:
        if not self.full_filepath.is_absolute():
            message = f"FitsFileObj::_initial_name_check: not an absolute path - {str(self.full_filepath)}"
            logg.critical(message)
            raise AssertionError(message)

        if not str(self.full_filepath).endswith('.fits.fz'):
            message = f"FitsFileObj::_initial_name_check: not .fits.fz - {str(self.full_filepath)}"
            logg.critical(message)
            raise AssertionError(message)
        
    def _write_data_to_disk(self, filename: Path) -> None:
        raise NotImplementedError('Illegal on .fits.fz. For now.')

    def get_nframes(self) -> int:
        # Assume logshim format... n_frames = last axis
        _NAXIS3: int = self.fits_header['NAXIS3'] # type: ignore
        return _NAXIS3
    
    def _ensure_data_loaded(self):
        if self.data is None:
            if self.is_on_disk:
                # This will uncompress HDU 1
                self.data = fits.getdata(self.full_filepath, memmap=False)
            else:
                self.data = self.constr_data

    def _merge_data_after(self, other_data):
        raise NotImplementedError('Illegal on .fits.fz. For now.')
