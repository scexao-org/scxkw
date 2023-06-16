import logging

logg = logging.getLogger(__name__)

import typing as typ

import os
from datetime import datetime
from pathlib import Path
from astropy.io import fits

from .logshim_txt_parser import LogshimTxtParser


class FitsFileObj:

    def __init__(self, fullname: typ.Union[Path, str]) -> None:
        '''
        We're expecting root_path/date/stream_name/stream_timestring.fits[.fz|.gz]
        '''

        self.full_filepath: Path = Path(fullname)

        if not (self.full_filepath.is_file()
                and self.full_filepath.is_absolute()):
            message = f"FitsFileObj::__init__: does not exist / not an absolute path - {fullname}"
            logg.critical(message)
            raise AssertionError(message)

        if not '.fits' in self.full_filepath.suffixes:
            message = f"FitsFileObj::__init__: not .fits[.fz|.gz] - {fullname}"
            logg.critical(message)
            raise AssertionError(message)

        self.file_name: str = self.full_filepath.name

        self.is_compressed: bool = self.full_filepath.suffix in ('.fz', '.gz')
        self.is_archived: bool = self.file_name.startswith(
            'SCX') or self.file_name.startswith('VMP')
        self.archive_key: typ.Optional[str] = None
        if self.is_archived:
            self.archive_key = self.file_name[:4] # SCXB, VMPA...

        self.stream_name_folder: str = self.full_filepath.parent.name
        self.date_folder: str = self.full_filepath.parent.parent.name

        self.fullroot_folder: Path = self.full_filepath.parent.parent.parent

        self.stream_name_filename: typ.Optional[str] = None
        self.file_time_filename: typ.Optional[float] = None
        if not self.is_archived:
            self.stream_name_filename = self.file_name.split('_')[0]
            # remove ns (not supported by strptime %f)
            fname_no_decimal = '.'.join(
                self.full_filepath.stem.split('.')[:-1])
            frac_seconds = float('0.' + self.full_filepath.stem.split('.')[-1])

            dt = datetime.strptime(self.date_folder + '/' + fname_no_decimal,
                                   '%Y%m%d/apapane_%H:%M:%S')

            self.file_time_filename = dt.timestamp() + frac_seconds

        self.file_time_creation = os.path.getctime(self.full_filepath)

        # File time. If archive-name file, best guess is creation time.
        self.file_time = self.file_time_filename if self.file_time_filename else self.file_time_creation

        self.fits_header: fits.Header = fits.getheader(self.full_filepath)

        self.txt_file_path: Path = self.full_filepath.parent / self.full_filepath.stem / '.txt'
        self.txt_exists: bool = self.txt_file_path.is_file()

        self.txt_file_parser: typ.Optional[LogshimTxtParser] = None
        if self.txt_exists:
            self.txt_file_parser = LogshimTxtParser(self.txt_file_path)

    def check_existence(self) -> bool:
        ok = self.full_filepath.is_file()
        if self.txt_exists:
            ok = ok and self.txt_file_path.is_file()

        return ok

    def __str__(self) -> str:
        return str(self.full_filepath)

    def __repr__(self) -> str:
        return str(self.full_filepath)