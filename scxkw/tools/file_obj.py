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


class FitsFileObj:

    def __init__(self,
                 fullname: typ.Union[Path, str],
                 on_disk: bool = True,
                 header: typ.Optional[fits.Header] = None,
                 data: typ.Optional[np.ndarray] = None,
                 txt_parser: typ.Optional[LogshimTxtParser] = None) -> None:
        '''
        We're expecting root_path/date/stream_name/stream_timestring.fits[.fz|.gz]
        '''

        self.is_on_disk = on_disk
        self.full_filepath: Path = Path(fullname)

        self.data = None

        self._initial_name_check()

        if on_disk:
            self._initial_existence_check()
        else:
            if not (header is not None and data is not None
                    and txt_parser is not None):
                message = f"FitsFileObj::__init__: not an absolute path - {str(self.full_filepath)}"
                logg.critical(message)
                raise AssertionError(message)

            self.constr_header = header
            self.constr_data = data
            self.constr_txt = txt_parser

        self._initialize_members()

    def _initial_name_check(self):
        if not self.full_filepath.is_absolute():
            message = f"FitsFileObj::_initial_name_check: not an absolute path - {str(self.full_filepath)}"
            logg.critical(message)
            raise AssertionError(message)

        if not '.fits' in self.full_filepath.suffixes:
            message = f"FitsFileObj::_initial_name_check: not .fits[.fz|.gz] - {str(self.full_filepath)}"
            logg.critical(message)
            raise AssertionError(message)

    def _initial_existence_check(self) -> None:

        if not self.full_filepath.is_file():
            message = f"FitsFileObj::_initial_existence_check: does not exist - {str(self.full_filepath)}"
            logg.critical(message)
            raise AssertionError(message)

    def _initialize_members(self) -> None:

        self.file_name: str = self.full_filepath.name

        self.is_compressed: bool = self.full_filepath.suffix in ('.fz', '.gz')
        self.is_archived: bool = self.file_name.startswith(
            'SCX') or self.file_name.startswith('VMP')
        self.archive_key: typ.Optional[str] = None
        if self.is_archived:
            self.archive_key = self.file_name[:4]  # SCXB, VMPA...

        self.stream_from_foldername: str = self.full_filepath.parent.name
        self.date_from_foldername: str = self.full_filepath.parent.parent.name

        self.fullroot_folder: Path = self.full_filepath.parent.parent.parent

        self.stream_name_filename: typ.Optional[str] = None
        self.file_time_filename: typ.Optional[float] = None
        if not self.is_archived:
            self.stream_name_filename = self.file_name.split('_')[0]
            # remove ns (not supported by strptime %f)
            fname_no_decimal = self.full_filepath.stem.split('.')[0]
            frac_seconds = float('0' + self.full_filepath.suffixes[0])

            dt = datetime.strptime(
                self.date_from_foldername + 'T' + fname_no_decimal,
                f'%Y%m%dT{self.stream_name_filename}_%H:%M:%S')

            self.file_time_filename = dt.timestamp() + frac_seconds

        if self.is_on_disk:
            self.file_time_creation: typ.Optional[float] = os.path.getctime(self.full_filepath)

        # File time. If archive-name file, best guess is creation time.
        self.file_time = self.file_time_filename if self.file_time_filename else self.file_time_creation

        self.fits_header: fits.Header = self._locate_fitsheader()

        self.txt_file_path: Path = self.full_filepath.parent / (self.full_filepath.stem + '.txt')

        self.txt_exists, self.txt_file_parser = self._locate_txtparser()

    def _locate_fitsheader(self) -> fits.Header:
        if self.is_on_disk:
            return fits.getheader(self.full_filepath)
        else:
            assert self.constr_header is not None
            return self.constr_header

    def _locate_txtparser(
            self) -> typ.Tuple[bool, typ.Optional[LogshimTxtParser]]:
        if self.is_on_disk:
            txt_exists = self.txt_file_path.is_file()
            txt_file_parser = None
            if txt_exists:
                txt_file_parser = LogshimTxtParser(self.txt_file_path)
            return txt_exists, txt_file_parser
        else:
            assert self.constr_txt is not None
            self.constr_txt.name = str(self.txt_file_path)
            return True, self.constr_txt

    def write_to_disk(self):
        if self.is_on_disk:
            message = f"FitsFileObj::write_to_disk: already exists - {str(self.full_filepath)}"
            logg.critical(message)
            raise AssertionError(message)
        
        assert self.txt_file_parser is not None

        logg.warning(f'FitsFileObj::write_to_disk - Writing {str(self.full_filepath)}')

        os.makedirs(self.full_filepath.parent, exist_ok=True)

        self.txt_file_parser.write_to_disk()

        # Attempt an atomic write that will avoid the typical globs *.fits that we use.
        os.makedirs(self.full_filepath.parent / 'tmp', exist_ok=True)

        tmped_name = self.full_filepath.parent / 'tmp' / self.full_filepath.name
        fits.writeto(tmped_name,
                     self.constr_data, self.fits_header)
        shutil.move(str(tmped_name), self.full_filepath)

        # Remove the tmp dir if empty
        try:
            os.rmdir(self.full_filepath.parent / 'tmp')
        except OSError: # Not empty
            pass

        self.is_on_disk = True

    def check_existence_on_disk(self) -> bool:
        if not self.is_on_disk:
            return False

        ok = self.full_filepath.is_file()
        if self.txt_exists:
            ok = ok and self.txt_file_path.is_file()

        return ok

    def __str__(self) -> str:
        return str(self.full_filepath)

    def __repr__(self) -> str:
        return str(self.full_filepath)

    def add_suffix_to_filename(self, suffix: str) -> None:
        # Big problem here...
        # If we have decimal seconds in the file name... it counts as a suffix.
        # So we need to drop 1 suffix
        assert len(self.full_filepath.suffixes) == 2

        new_name = self.full_filepath.stem + suffix + ''.join(self.full_filepath.suffixes[-1:])
        print(new_name)
        self._rename_in_folder(new_name)

    def _rename_in_folder(self, new_name: str) -> None:

        assert not '/' in new_name

        new_full_filepath = self.full_filepath.parent / new_name
        self._move(new_full_filepath)

    def move_file_to_root(self,
                          new_root_dir: typ.Union[str, Path],
                          allow_makedirs: bool = True) -> None:

        new_root = Path(new_root_dir)
        new_folder = new_root / self.date_from_foldername / self.stream_from_foldername

        self._move_to_new_folder(new_folder, allow_makedirs)

    def move_file_to_streamname(self,
                                stream_name: str,
                                allow_makedirs: bool = True,
                                also_change_filename: bool = False) -> None:

        new_folder = self.fullroot_folder / self.date_from_foldername / stream_name

        self._move_to_new_folder(new_folder, allow_makedirs)

        if also_change_filename:
            _, rest = self.file_name.split('_')
            new_filename = stream_name + '_' + rest
            self._rename_in_folder(new_filename)


    def _move_to_new_folder(self,
                            new_end_dir: Path,
                            allow_makedirs: bool = True) -> None:
        if not allow_makedirs and not new_end_dir.is_dir():
            message = f"FitsFileObj::move_file_by_root: {new_end_dir} does not exist."
            logg.critical(message)
            raise AssertionError(message)

        new_full_filepath = new_end_dir / self.file_name

        self._move(new_full_filepath, allow_makedirs=allow_makedirs)

    def _move(self, new_full_path: Path, allow_makedirs: bool = True) -> None:

        if allow_makedirs and self.is_on_disk:
            os.makedirs(new_full_path.parent, exist_ok=True)

        new_txt_path = new_full_path.parent / (self.full_filepath.stem + '.txt')

        if self.is_on_disk:
            logg.warning(f'FitsFileObj::_move - moving {str(self.full_filepath)}'
                         f' to {new_full_path}')
            shutil.move(str(self.txt_file_path), new_txt_path)
            shutil.move(str(self.full_filepath), new_full_path)

        self.full_filepath = new_full_path

        # Re-initialize internals.
        if self.is_on_disk:
            self._initial_existence_check()
        self._initialize_members()

    def get_nframes(self) -> int:
        return self.fits_header['NAXIS3']  # Assume logshim format...

    def sub_file_nodisk(
        self,
        split_selector: np.ndarray,
    ) -> FitsFileObj:

        assert split_selector.dtype == bool
        assert len(split_selector) == self.get_nframes()

        if self.data is None:
            if self.is_on_disk:
                self.data = fits.getdata(self.full_filepath)
            else:
                self.data = self.constr_data

        assert self.txt_file_parser
        txt_parser = self.txt_file_parser.sub_parser_by_selection(
            'x', split_selector)

        header = self.fits_header.copy()
        header['NAXIS3'] = np.sum(split_selector)

        tstr = fix_header_times(header, txt_parser.fgrab_t_us[0] / 1e6,
                                 txt_parser.fgrab_t_us[-1] / 1e6)

        assert self.data is not None
        subdata = self.data[split_selector]

        full_path = (self.full_filepath.parent / (self.stream_name_filename + '_' + tstr + '.fits'))
        file_obj = FitsFileObj(full_path,
                                 on_disk=False,
                                 header=header,
                                 data=subdata,
                                 txt_parser=txt_parser)
        
        return file_obj

    def get_finish_unixtime_secs(self) -> float:
        if self.txt_exists:
            # Return acqtime from last frame
            assert self.txt_file_parser is not None
            return self.txt_file_parser.fgrab_t_us[-1] / 1e6
        elif 'DATE-OBS' in self.fits_header and 'UT-END' in self.fits_header:
            # Return timestamp from UT-END
            full_tstr = (self.fits_header['DATE-OBS'] + 'T' +
                         self.fits_header['UT-END'])
            message = ('FitsFileObj::get_finish_unixtime_secs - '
                       f'Falling back to header for {self.file_name}.')
            logg.warning(message)
            return datetime.strptime(full_tstr,
                                     '%Y-%m-%dT%H:%M:%S.%f').timestamp()
        else:
            # Return file write time...
            message = (
                'FitsFileObj::get_finish_unixtime_secs - '
                f'falling back to file creation time for {self.file_name}.')
            logg.error(message)
            return self.file_time_creation

    def get_start_unixtime_secs(self) -> float:

        if self.txt_exists:
            # Return acqtime from first frame - 1 exposure
            exp_time = self.fits_header['EXPTIME'] * \
                        self.fits_header['DET-NSMP']
            assert self.txt_file_parser is not None
            return self.txt_file_parser.fgrab_t_us[0] / 1e6 - exp_time
        elif 'DATE-OBS' in self.fits_header and 'UT-STR' in self.fits_header:
            # Return timestamp from UT-START - one exposure
            full_tstr = (self.fits_header['DATE-OBS'] + 'T' +
                         self.fits_header['UT-STR'])
            message = ('FitsFileObj::get_start_unixtime_secs - '
                       f'Falling back to header for {self.file_name}.')
            logg.warning(message)
            return datetime.strptime(full_tstr,
                                     '%Y-%m-%dT%H:%M:%S.%f').timestamp()
        elif self.file_time_filename is not None:  # Return filename
            message = ('FitsFileObj::get_start_unixtime_secs - '
                       f'falling back to filename for {self.file_name}.')
            logg.error(message)
            return self.file_time_filename
        else:
            message = ('FitsFileObj::get_start_unixtime_secs - '
                       f'no can do for {self.file_name}.')
            logg.critical(message)
            raise AssertionError(message)
        
    def delete_from_disk(self):
        
        assert self.is_on_disk

        logg.warning(f'FitsFileObj::delete_from_disk - '
                     f'{self.full_filepath}')
        
        # We move before deleting for atomicity
        extension = str(time.time())
        shutil.move(str(self.txt_file_path), str(self.txt_file_path) + extension)
        shutil.move(str(self.full_filepath), str(self.full_filepath) + extension)
        os.remove(str(self.txt_file_path) + extension)
        os.remove(str(self.full_filepath) + extension)

        self.is_on_disk = False # But we don't have self.const_data as in an originally virtual file.
