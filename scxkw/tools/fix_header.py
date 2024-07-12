'''
    Utilities to fix FITS headers post-hoc
'''
from __future__ import annotations

import typing as typ

from astropy.io import fits
from astropy.time import Time as AstroTime  # astropy >= 5 to work with recent numpy.
from tqdm import tqdm
from pathlib import Path

from scxkw import config as cfg
from scxkw.redisutil import redis_util as rdbutil
from scxkw.redisutil.typed_db import Redis
from scxkw.redisutil.type_cast import scalar_cast

import glob
from datetime import datetime, timezone, timedelta

from .fits_format import format_values


def fix_header_times(header: fits.Header, start_time_unix: float,
                     end_time_unix: float) -> str:
    '''
    NB: returns the start time strftime
    NB: not fixing MJD
    '''

    ut_start = datetime.fromtimestamp(start_time_unix).astimezone(timezone.utc)
    ut_end = datetime.fromtimestamp(end_time_unix).astimezone(timezone.utc)
    ut_mid = datetime.fromtimestamp(
        (end_time_unix + start_time_unix) / 2.).astimezone(timezone.utc)

    hst_zone = timezone(timedelta(hours=-10))  # FIXME hard set HST.
    hst_start = ut_start.astimezone(hst_zone)
    hst_end = ut_end.astimezone(hst_zone)
    hst_mid = ut_mid.astimezone(hst_zone)

    header['UT-STR'] = ut_start.strftime('%H:%M:%S.%f')
    header['UT-END'] = ut_end.strftime('%H:%M:%S.%f')
    header['UT'] = ut_mid.strftime('%H:%M:%S.%f')

    header['HST-STR'] = hst_start.strftime('%H:%M:%S.%f')
    header['HST-END'] = hst_end.strftime('%H:%M:%S.%f')
    header['HST'] = hst_mid.strftime('%H:%M:%S.%f')

    header['MJD-STR'] = AstroTime(ut_start).mjd
    header['MJD-END'] = AstroTime(ut_end).mjd
    header['MJD'] = AstroTime(ut_mid).mjd

    return ut_start.strftime('%H:%M:%S.%f')


class CSVTableLookup:
    header_row: list[str] = None
    header_col: list[str] = None
    dict_by_row: dict[datetime, list[str]] = None
    dict_by_col: dict[str, list[str]] = None

    def __init__(self, filename: str):
        with open(filename, 'r') as file:
            lines = [l.rstrip().split('\t') for l in file.readlines()]

        self.header_row = lines[0][1:]

        self.header_col = [
            datetime.strptime(l[0], '%Y%m%d-%H:%M:%S') for l in lines[1:]
        ]

        self.dict_by_row = {
            dtime: [scalar_cast(ll) for ll in l[1:]]
            for (dtime, l) in zip(self.header_col, lines[1:])
        }

        self.dict_by_col = {}
        for kwidx, kw in enumerate(self.header_row):
            self.dict_by_col[kw] = [
                scalar_cast(l[kwidx + 1]) for l in lines[1:]
            ]

    def find_just_before(self, time: datetime):
        if time >= self.header_col[-1]:
            return self.header_col[-1]
        if time < self.header_col[
                0]:  # We'd really need the CSV from the day before...
            return self.header_col[0]

        # Dich research
        n_rows = len(self.header_col)
        upper = n_rows - 1
        lower = 0
        current = (upper + lower) // 2
        go = True
        while go:
            if self.header_col[current] > time:
                higher = current
                current = (lower + current) // 2
            elif self.header_col[current + 1] <= time:
                lower = current
                current = (higher + current + 1) // 2
            else:
                go = False

        if not (self.header_col[current] <= time
                and self.header_col[current + 1] > time):
            raise AssertionError('TIFU dicho search.')

        return self.header_col[current]

    def reformat_using_format_from_redis(self):
        self.formats = rdbutil.get_formats_for_keys(set(self.header_row))

        # Values comme as strings.

        for row_id in self.dict_by_row:
            self.dict_by_row[row_id] = [
                format_values(val, fmt)[0]
                for (val, fmt) in zip(self.dict_by_row[row_id], self.formats)
            ]
        for col_id in self.dict_by_col:
            self.dict_by_col[col_id] = [
                format_values(val, self.formats[col_id])[0]
                for val in self.dict_by_col[col_id]
            ]


def reformat_file(filename: str,
                  fmt_dict: dict[str, str],
                  hdu_number: int = 0):
    fix_file(filename, {}, fmt_dict, hdu_number=hdu_number)


def fix_file(filename: str,
             new_keyvals: dict[str, str | tuple[str, str]],
             fmt_dict: dict[str, str] | None = None,
             supersede: bool = False,
             hdu_number: int = 0) -> None:
    '''
        Fix the FITS header of file <file> using the FITS keys in <new_keyvals>.

        <new_keyvals> may be tuples(value, comment)
        <supersede>: Supersede existing keys in the fits header.

        Comments will ALWAYS be superseded.
    '''

    with fits.open(filename, 'update') as f:
        hdu = f[hdu_number]
        header = hdu.header
        existing_keys = set(header)
        new_keys = set(new_keyvals)

        if not supersede:
            update_keys = new_keys.difference(existing_keys)
        else:
            update_keys = new_keys

        for key in update_keys:
            header[key] = new_keyvals[key]

        # Force update all comments
        for key in new_keys:
            if isinstance(new_keyvals[key], tuple):
                header[key] = (header[key], new_keyvals[key][1])

        # Perform a reformatting of all keys
        if fmt_dict is not None:
            for key in header:
                if key in fmt_dict:
                    val = header[key]
                    if type(val) is str:
                        val = scalar_cast(val)
                    # Future ref: the only diff between Redis' ScxkwValueType and
                    # fits_format T_kwValue_pre is the complex type.
                    assert not isinstance(val, complex)
                    formattable_val, _ = format_values(val, fmt_dict[key], non_equalizable_formattables = True)
                    header[key] = formattable_val


def reformat_all_files(folder: str | Path):
    key_list = rdbutil.get_all_uppercase_keys_from_redis()
    formats = rdbutil.get_formats_for_keys(key_list)

    # List the fits files that need fixing
    path = Path(folder)
    filenames = list(sorted(path.glob("*.fits*")))
    for fname in tqdm(filenames):
        abs_path = str(fname.absolute())
        hdu = 1 if abs_path.endswith(".fz") else 0
        reformat_file(abs_path, formats, hdu_number=hdu)


def fix_all_files(root_folder: str,
                  csv_root_folder: str,
                  ut_date: str,
                  stream: str,
                  keyword_filter_set: str = "set:fits:apapane",
                  extra_keywords: dict[str, tuple[typ.Any, str]] = {}):

    csv_table_path = f'{csv_root_folder}/{ut_date}/logging/keywords_log.tsv'
    csv_mapping = CSVTableLookup(csv_table_path)
    #csv_mapping.reformat_using_format_from_redis()

    # Get the keys from redis and restrict them to what's
    # available in the tsv file.
    key_set = rdbutil.get_keys_from_redis(keyword_filter_set).intersection(
        set(csv_mapping.header_row))

    # Essentially, get all the possible comments
    comment_dict = rdbutil.get_comments_for_keys(set(csv_mapping.header_row))
    formats = rdbutil.get_formats_for_keys(set(csv_mapping.header_row))

    # List the fits files that need fixing
    fits_regex = f'{root_folder}/{ut_date}/{stream}/*.fits'
    filenames = glob.glob(fits_regex)
    filenames.sort()

    for fname in tqdm(filenames):
        with fits.open(fname, 'readonly') as f:
            fits_time = datetime.strptime(f[0].header['DATE'],
                                          '%Y-%m-%dT%H:%M:%S')

        jit_csv_time = csv_mapping.find_just_before(fits_time)

        val_source = {
            key: (value, comment_dict[key])
            for (key, value) in zip(csv_mapping.header_row,
                                    csv_mapping.dict_by_row[jit_csv_time])
        }
        keyvals = {key: val_source[key] for key in key_set}

        keyvals.update(extra_keywords)

        # One more issues: the values that came from the CSV are ALL strings.
        # We need to reformat them per redis Type fields.

        fix_file(fname, keyvals, fmt_dict=formats)

    return csv_mapping

