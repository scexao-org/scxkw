from __future__ import annotations
import typing as typ

t_Op = typ.Optional

import logging

logg = logging.getLogger(__name__)

import abc
import os, shutil
from datetime import datetime
from pathlib import Path
from astropy.io import fits
import numpy as np
import time

from .logshim_txt_parser import LogshimTxtParser
from .fix_header import fix_header_times

class MotherOfFileObj(abc.ABC):

    HDU_POS = 0

    def __init__(self,
                 fullname: typ.Union[Path, str],
                 on_disk: bool = True,
                 header: t_Op[fits.Header] = None,
                 data: t_Op[typ.Iterable] = None,
                 txt_parser: t_Op[LogshimTxtParser] = None) -> None:
        '''
        We're expecting root_path/date/stream_name/stream_timestring.fits[.fz|.gz]
        '''

        self.is_on_disk = on_disk
        self.full_filepath: Path = Path(fullname)

        # Cannot access data member in this superclass
        self.data: t_Op[np.ndarray] = None

        self._initial_name_check()

        if on_disk:
            self._initial_existence_check()
            # assert: shouldn't be passing arguments if on_disk
            assert (header is None and data is None and txt_parser is None)
            self.constr_header = header
            self.constr_data = data
            self.constr_txt = txt_parser
        else:
            if not (header is not None and data is not None):
                message = f"MotherOfFileObj::__init__: header | data | txt_parser is None - {str(self.full_filepath)}"
                logg.critical(message)
                raise AssertionError(message)

            self.constr_header = header
            self.constr_data = data
            self.constr_txt = txt_parser

        self._initialize_members()

    @abc.abstractmethod
    def _initial_name_check(self) -> None:
        pass

    def _initial_existence_check(self) -> None:

        if not self.full_filepath.is_file():
            message = f"MotherOfFileObj::_initial_existence_check: does not exist - {str(self.full_filepath)}"
            logg.critical(message)
            raise AssertionError(message)

    def _initialize_members(self) -> None:

        self.file_name: str = self.full_filepath.name

        self.is_compressed: bool = ('.fz' in self.full_filepath.suffixes
                                    or '.gz' in self.full_filepath.suffixes)
        self.is_archived: bool = self.file_name.startswith(
            'SCX') or self.file_name.startswith('VMP')

        self.archive_key: t_Op[str] = None

        if self.is_archived:
            self.archive_key = self.file_name[:4]  # SCXB, VMPA...

        self.stream_from_foldername: str = self.full_filepath.parent.name
        self.date_from_foldername: str = self.full_filepath.parent.parent.name

        self.full_rootfolder: Path = self.full_filepath.parent.parent.parent

        self.stream_from_filename: t_Op[str] = None
        self.time_from_filename: t_Op[float] = None
        if not self.is_archived:
            self.stream_from_filename = self.file_name.split('_')[0]
            # remove ns (not supported by strptime %f)
            fname_no_decimal = self.full_filepath.stem.split('.')[0]
            frac_seconds = float('0' + self.full_filepath.suffixes[0])

            dt = datetime.strptime(
                self.date_from_foldername + 'T' + fname_no_decimal,
                f'%Y%m%dT{self.stream_from_filename}_%H:%M:%S')

            self.time_from_filename = dt.timestamp() + frac_seconds

        if self.is_on_disk:
            self.file_time_creation: t_Op[float] = os.path.getctime(
                self.full_filepath)

        # File time. If archive-name file, best guess is creation time.
        self.file_time = self.time_from_filename if self.time_from_filename else self.file_time_creation

        self.fits_header: fits.Header = self._locate_fitsheader()

        _DATE: str = self.fits_header['DATE']  # type: ignore
        self.file_time_header: float = datetime.strptime(
            _DATE, '%Y-%m-%dT%H:%M:%S').timestamp()

        self.txt_file_path: Path = self.full_filepath.parent / (
            self.full_filepath.stem + '.txt')

        self.txt_exists, self.txt_file_parser = self._locate_txtparser()

    def _locate_fitsheader(self) -> fits.Header:
        if self.is_on_disk:
            return fits.getheader(self.full_filepath, self.HDU_POS)
        else:
            assert self.constr_header is not None
            return self.constr_header

    def _locate_txtparser(self) -> typ.Tuple[bool, t_Op[LogshimTxtParser]]:
        if self.is_on_disk:
            txt_exists = self.txt_file_path.is_file()
            txt_file_parser = None
            if txt_exists:
                txt_file_parser = LogshimTxtParser(self.txt_file_path)
            return txt_exists, txt_file_parser
        else:
            if self.constr_txt is not None:
                self.constr_txt.name = str(self.txt_file_path)
                return True, self.constr_txt
            else:
                return False, None
        
    def disown_txt_file(self) -> None:
        self.txt_exists = False
        self.txt_file_parser = None

    def write_to_disk(self, try_flush_ram: bool = False) -> None:
        if self.is_on_disk:
            message = f"MotherOfFileObj::write_to_disk: already exists - {str(self.full_filepath)}"
            logg.critical(message)
            raise AssertionError(message)

        os.makedirs(self.full_filepath.parent, exist_ok=True)

        if self.txt_exists:
            assert self.txt_file_parser is not None
            self.txt_file_parser.write_to_disk()

        # Attempt an atomic write that will avoid the typical globs *.fits that we use.
        os.makedirs(self.full_filepath.parent / 'tmp', exist_ok=True)

        tmped_name = self.full_filepath.parent / 'tmp' / self.full_filepath.name

        self._write_data_to_disk(tmped_name)

        shutil.move(str(tmped_name), self.full_filepath)

        # DO NOT delete the /tmp folder... race condition with other processes!

        self.is_on_disk = True

        if try_flush_ram:
            self._flush_from_ram()

    @abc.abstractmethod
    def _write_data_to_disk(self, filename: Path) -> None:
        pass

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
        # Suffix must include the .
        # If we have decimal seconds in the file name... it counts as a suffix.

        assert suffix.startswith('.')
        assert self.is_compressed is False
        assert self.is_archived is False

        filename_no_suff = self.file_name.split('.')[0]
        suffixes = self.full_filepath.suffixes

        new_name = filename_no_suff + ''.join(
            suffixes[:-1]) + suffix + suffixes[-1]
        
        self.rename_in_folder(new_name)

    def ut_sanitize(self) -> None:

        assert self.stream_from_filename is not None
        assert self.time_from_filename is not None

        if not abs((self.file_time_header - self.time_from_filename) % 86400 -
                   36000) < 2000:
            return

        # Prefer to add back 10 hours to HST to conserve the logic of when
        # the filename is taken relative to the file.
        new_filename = self.stream_from_filename + '_' +\
              datetime.fromtimestamp(self.time_from_filename + 36000).strftime('%H:%M:%S') +\
              ''.join(self.full_filepath.suffixes)

        message = f"MotherOfFileObj::ut_sanitize: {self.file_name} -> {new_filename}"
        logg.warning(message)

        self.rename_in_folder(new_filename)

    def rename_in_folder(self, new_name: str) -> None:

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

        new_folder = self.full_rootfolder / self.date_from_foldername / stream_name

        self._move_to_new_folder(new_folder, allow_makedirs)

        if also_change_filename:
            _, rest = self.file_name.split('_')
            new_filename = stream_name + '_' + rest
            self.rename_in_folder(new_filename)

    def _move_to_new_folder(self,
                            new_end_dir: Path,
                            allow_makedirs: bool = True) -> None:
        if not allow_makedirs and not new_end_dir.is_dir():
            message = f"MotherOfFileObj::move_file_by_root: {new_end_dir} does not exist."
            logg.critical(message)
            raise AssertionError(message)

        new_full_filepath = new_end_dir / self.file_name

        self._move(new_full_filepath, allow_makedirs=allow_makedirs)

    def _move(self, new_full_path: Path, allow_makedirs: bool = True) -> None:

        if allow_makedirs and self.is_on_disk:
            os.makedirs(new_full_path.parent, exist_ok=True)

        new_txt_path = new_full_path.parent / (new_full_path.stem + '.txt')

        if self.is_on_disk:
            logg.warning(
                f'MotherOfFileObj::_move - moving {str(self.full_filepath)}'
                f' to {new_full_path}')
            if self.txt_exists:
                shutil.move(str(self.txt_file_path), new_txt_path)
            shutil.move(str(self.full_filepath), new_full_path)

        self.full_filepath = new_full_path

        # Re-initialize internals.
        if self.is_on_disk:
            self._initial_existence_check()
        self._initialize_members()

    @abc.abstractmethod
    def get_nframes(self) -> int:
        pass

    @abc.abstractmethod
    def _ensure_data_loaded(self) -> None:
        pass

    def _flush_from_ram(self) -> None:
        '''
        This is basically [trying] reversing _ensure_data_loaded.
        But it doesn't really work since there's 99.99% chance that
        the data pointer is still around.
        '''
        if self.data is not None and self.is_on_disk:
            del self.data
            self.data = None
            if self.constr_data:
                del self.constr_data
                self.constr_data = None

    @abc.abstractmethod
    def _merge_data_after(self, other_data):
        pass

    def merge_with_file_after(self, other: MotherOfFileObj) -> MotherOfFileObj:

        # Assert we're merging identical subclass types.
        assert type(self) == type(other)

        self._ensure_data_loaded()
        other._ensure_data_loaded()

        assert (self.txt_file_parser is not None
                and other.txt_file_parser is not None)
        parser = self.txt_file_parser.clone_instance()
        parser.lines += other.txt_file_parser.lines
        parser._init_arrays_from_lines()

        header = self.fits_header.copy()
        header['NAXIS3'] = self.get_nframes() + other.get_nframes()

        tstr = fix_header_times(header, parser.fgrab_t_us[0] / 1e6,
                                parser.fgrab_t_us[-1] / 1e6)

        merge_data = self._merge_data_after(other.data)

        # Instantiate the appropriate subclass
        file_obj = type(self)(self.full_filepath,
                              on_disk=False,
                              header=header,
                              data=merge_data,
                              txt_parser=parser)

        return file_obj

    def rename_from_first_frame(self):
        assert self.stream_from_filename is not None and self.txt_file_parser is not None
        tstr = fix_header_times(self.fits_header,
                                self.txt_file_parser.fgrab_t_us[0] / 1e6,
                                self.txt_file_parser.fgrab_t_us[-1] / 1e6)
        new_name = self.stream_from_filename + '_' + tstr + ''.join(
            self.full_filepath.suffixes[1:])

        self.rename_in_folder(new_name)

    def sub_file_nodisk(self,
                        split_selector: np.ndarray,
                        add_suffix: t_Op[str] = None,
                        keep_name_timestamp: bool = False) -> MotherOfFileObj:

        assert split_selector.dtype == bool
        assert len(split_selector) == self.get_nframes()

        self._ensure_data_loaded()

        assert self.txt_file_parser
        txt_parser = self.txt_file_parser.sub_parser_by_selection(
            'x', split_selector)

        header = self.fits_header.copy()
        header['NAXIS3'] = np.sum(split_selector)

        assert self.data is not None
        subdata = self.data[split_selector]

        # Conserve ALL suffixes except the first one (frac seconds)

        assert self.stream_from_filename is not None
        if keep_name_timestamp:
            # In this case, the full_path is the same as the parent until you add a suffix!!
            full_path = str(self.full_filepath)
        else:
            # We are NOT fixing header times if keep_name_timestamp!!
            # By design so that PDI deinterleaving maintains identical MJDs.
            tstr = fix_header_times(header, txt_parser.fgrab_t_us[0] / 1e6,
                                    txt_parser.fgrab_t_us[-1] / 1e6)
            full_path = (self.full_filepath.parent /
                         (self.stream_from_filename + '_' + tstr +
                          ''.join(self.full_filepath.suffixes[1:])))

        file_obj = type(self)(full_path,
                              on_disk=False,
                              header=header,
                              data=subdata,
                              txt_parser=txt_parser)

        if add_suffix is not None:
            file_obj.add_suffix_to_filename(add_suffix)

        return file_obj

    def get_start_unixtime_secs(self) -> float:

        if self.txt_exists:
            assert self.txt_file_parser is not None
            if ('EXPTIME' in self.fits_header
                    and self.fits_header['EXPTIME'] is not None):
                return (self.txt_file_parser.fgrab_t_us[0] / 1e6 -
                        self.fits_header['EXPTIME'])
            else:
                return self.txt_file_parser.fgrab_t_us[0] / 1e6
        elif 'DATE-OBS' in self.fits_header and 'UT-STR' in self.fits_header:
            # Return timestamp from UT-START - one exposure
            full_tstr = (
                self.fits_header['DATE-OBS'] + 'T' +  # type: ignore
                self.fits_header['UT-STR'])
            message = ('MotherOfFileObj::get_start_unixtime_secs - '
                       f'Falling back to header for {self.file_name}.')
            logg.warning(message)
            return datetime.strptime(full_tstr,
                                     '%Y-%m-%dT%H:%M:%S.%f').timestamp()
        elif self.time_from_filename is not None:  # Return filename
            message = ('MotherOfFileObj::get_start_unixtime_secs - '
                       f'falling back to filename for {self.file_name}.')
            logg.error(message)
            return self.time_from_filename
        else:
            message = ('MotherOfFileObj::get_start_unixtime_secs - '
                       f'no can do for {self.file_name}.')
            logg.critical(message)
            raise AssertionError(message)

    def get_finish_unixtime_secs(self) -> float:
        if self.txt_exists:
            # Return acqtime from last frame
            assert self.txt_file_parser is not None
            return self.txt_file_parser.fgrab_t_us[-1] / 1e6
        elif 'DATE-OBS' in self.fits_header and 'UT-END' in self.fits_header:
            # Return timestamp from UT-END
            full_tstr = (
                self.fits_header['DATE-OBS'] + 'T' +  # type: ignore
                self.fits_header['UT-END'])
            message = ('MotherOfFileObj::get_finish_unixtime_secs - '
                       f'Falling back to header for {self.file_name}.')
            logg.warning(message)
            return datetime.strptime(full_tstr,
                                     '%Y-%m-%dT%H:%M:%S.%f').timestamp()
        else:
            # Return file write time...
            message = (
                'MotherOfFileObj::get_finish_unixtime_secs - '
                f'falling back to file creation time for {self.file_name}.')
            logg.error(message)
            assert self.file_time_creation is not None
            return self.file_time_creation
        
    def edit_header(self, key: str, value):
        '''
        Don't use this for multiple updates... gun' be slow.
        '''
        self.fits_header[key] = value
        if self.is_on_disk:
            with fits.open(self.full_filepath, 'update') as fptr:
                fptr[self.HDU_POS].header[key] = value


    def delete_from_disk(self,
                         try_purge_ram: bool = False,
                         silent_fail: bool = False):
        
        if silent_fail and not self.is_on_disk:
            return

        assert self.is_on_disk

        logg.warning(
            f'MotherOfFileObj::delete_from_disk - '
            f'{self.full_filepath} ({self.get_nframes()}x[{self.fits_header["NAXIS1"]}x{self.fits_header["NAXIS2"]}])'
        )

        # We move before deleting for atomicity
        extension = str(time.time())
        if self.txt_exists:
            shutil.move(str(self.txt_file_path),
                        str(self.txt_file_path) + extension)
        shutil.move(str(self.full_filepath),
                    str(self.full_filepath) + extension)
        if self.txt_exists:
            os.remove(str(self.txt_file_path) + extension)
        os.remove(str(self.full_filepath) + extension)

        self.is_on_disk = False  # But we don't have self.const_data as in an originally virtual file.

        if try_purge_ram:
            # This fobj is now dead
            self._flush_from_ram()
