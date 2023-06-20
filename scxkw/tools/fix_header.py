'''
    Utilities to fix FITS headers post-hoc
'''

from typing import Dict, Union, Set, List, Tuple, Any
from astropy.io import fits
from astropy.time import Time as AstroTime # astropy >= 5 to work with recent numpy.

from scxkw.config import REDIS_DB_HOST, REDIS_DB_PORT, FITS_HEADER_PATH
from scxkw.redisutil.typed_db import Redis

from scxkw.redisutil.type_cast import scalar_cast

import glob
from datetime import datetime, timezone, timedelta

def fix_header_times(header: fits.Header,
                     start_time_unix: float, end_time_unix: float) -> str:
    '''
    NB: returns the start time strftime
    NB: not fixing MJD
    '''

    ut_start = datetime.fromtimestamp(start_time_unix).astimezone(timezone.utc)
    ut_end = datetime.fromtimestamp(end_time_unix).astimezone(timezone.utc)
    ut_mid = datetime.fromtimestamp((end_time_unix + start_time_unix) / 2.).astimezone(timezone.utc)
    
    hst_zone = timezone(timedelta(hours=-10))
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


class CSV_table_lookup:
    header_row: List[str] = None
    header_col: List[str] = None
    dict_by_row: Dict[datetime, List[str]] = None
    dict_by_col: Dict[str, List[str]] = None

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
            self.dict_by_col[kw] = [scalar_cast(l[kwidx + 1]) for l in lines[1:]]

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
        self.formats = get_formats_for_keys(set(self.header_row))

        # Values comme as strings.

        for row_id in self.dict_by_row:
            self.dict_by_row[row_id] = [format_value(val, fmt) for (val, fmt) in zip(self.dict_by_row[row_id], self.formats)]
        for col_id in self.dict_by_col:
            self.dict_by_col[col_id] = [format_value(val, self.formats[col_id]) for val in self.dict_by_col[col_id]]

def format_value(value: Any, fmt: str):
    # Code copy-pasted from fits_write.py
    # Some values are None: camera-stream keywords (EXPTIME, FG_SIZE1, ...),
    # and time-keywords (MJD, HST, UTC...) generated upon saving
    if value is not None:
        try:
            if fmt == 'BOOLEAN':
                value = bool(value)
            elif fmt[-1] == 'd':
                value = int(fmt % value)
            elif fmt[-1] == 'f':
                value = float(fmt % value)
            elif fmt[-1] == 's':  # string
                value = fmt % value
        except:  # Sometime garbage values cannot be formatted properly...
            value = value
            print(f"fits_headers: formatting error on {value}, {fmt}")
            import pdb; pdb.set_trace()
    
    return value


def fix_file(filename: str,
             new_keyvals: Dict[str, Union[str, Tuple[str, str]]],
             fmt_dict: Dict[str, str] = None,
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
                    header[key] = format_value(val, fmt_dict[key])


def get_keys_from_redis(keyword_set: str) -> Set[str]:
    with Redis(host=REDIS_DB_HOST, port=REDIS_DB_PORT) as rdb:
        s = rdb.smembers(keyword_set)
    return s

def get_comments_for_keys(key_set: Set[str]) -> Dict[str, str]:
    return get_field_for_keys("Description", key_set)

def get_formats_for_keys(key_set: Set[str]) -> Dict[str, str]:
    return get_field_for_keys("Type", key_set)

def get_field_for_keys(field: str, key_set: Set[str]) -> Dict[str, Any]:
    key_list = list(key_set)

    with Redis(host=REDIS_DB_HOST, port=REDIS_DB_PORT) as rdb:
        with rdb.pipeline() as pipe:
            for key in key_list:
                pipe.hget(key, field)
            all_comments = pipe.execute()

        key_comment_dict = {key: comment for (key, comment) in zip(key_list, all_comments)}
    
    return key_comment_dict


def fix_all_files(root_folder: str,
                  csv_root_folder: str,
                  ut_date: str,
                  stream: str,
                  keyword_filter_set: str = "set:fits:buffy",
                  extra_keywords: Dict[str, Tuple[Any, str]] = {}):

    csv_table_path = f'{csv_root_folder}/{ut_date}/logging/keywords_log.tsv'
    csv_mapping = CSV_table_lookup(csv_table_path)
    #csv_mapping.reformat_using_format_from_redis()

    # Get the keys from redis and restrict them to what's
    # available in the tsv file.
    key_set = get_keys_from_redis(keyword_filter_set).intersection(
        set(csv_mapping.header_row))

    # Essentially, get all the possible comments
    comment_dict = get_comments_for_keys(set(csv_mapping.header_row))
    formats = get_formats_for_keys(set(csv_mapping.header_row))

    # List the fits files that need fixing
    fits_regex = f'{root_folder}/{ut_date}/{stream}/*.fits'
    filenames = glob.glob(fits_regex)
    filenames.sort()

    from tqdm import tqdm
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

        fix_file(fname, keyvals, fmt_dict = formats)

    return csv_mapping

BUFFY_SPECIAL_KW = {
        'CDELT1': (4.5e-6, 'X Scale projected on detector (#/pix)'),
        'CDELT2': (4.5e-6, 'Y Scale projected on detector (#/pix)'),
        'C2ELT1': (4.5e-6, 'X Scale projected on detector (#/pix)'),
        'C2ELT2': (4.5e-6, 'Y Scale projected on detector (#/pix)'),
        'CTYPE1': ('RA--TAN   ', 'Pixel coordinate system'),
        'CTYPE2': ('DEC--TAN  ', 'Pixel coordinate system'),
        'C2YPE1': ('RA--TAN   ', 'Pixel coordinate system'),
        'C2YPE2': ('DEC--TAN  ', 'Pixel coordinate system'),
        'CUNIT1': ('DEGREE    ', 'Units used in both CRVAL1 and CDELT1'),
        'CUNIT2': ('DEGREE    ', 'Units used in both CRVAL2 and CDELT2'),
        'C2NIT1': ('DEGREE    ', 'Units used in both C2VAL1 and C2ELT1'),
        'C2NIT2': ('DEGREE    ', 'Units used in both C2VAL2 and C2ELT2'),

        # Those will change with cropmode
        'CRPIX1': ( 40., 'Reference pixel in X (pixel)'),
        'CRPIX2': ( 80., 'Reference pixel in Y (pixel)'),
        'C2PIX1': (120., 'Reference pixel in X (pixel)'),
        'C2PIX2': ( 80., 'Reference pixel in Y (pixel)'),

        'CD1_1': (4.5e-6, 'Pixel coordinate translation matrix'),
        'CD1_2': (    0., 'Pixel coordinate translation matrix'),
        'CD2_1': (    0., 'Pixel coordinate translation matrix'),
        'CD2_2': (4.5e-6, 'Pixel coordinate translation matrix'),

        'CRVAL1': (0.0, 'Physical value of the reference pixel X'),
        'CRVAL2': (0.0, 'Physical value of the reference pixel Y'),
        'C2VAL1': (0.0, 'Physical value of the reference pixel X'),
        'C2VAL2': (0.0, 'Physical value of the reference pixel Y'),

        'INSTRUME': ('SCExAO              ', 'Instrument name'),
        'LONPOLE' : (0.0, 'The North Pole of standard system (deg)'),

        'OBS-MOD': ('Imaging', 'Observation Mode'),
        'TIMESYS': ('UTC     ', 'Time System used in the header'),
        'RADESYS': ('FK5     ', 'The equatorial coordinate system'),

        'POL-ANG1': (0.0, 'Position angle of first polarizer (deg)'),
        'POLARIZ1': ('NONE            ', 'Identifier of first polarizer'),
        'RET-ANG2': (0.0,'Position angle of second retarder plate (deg)'),
        'RETPLAT1': ('NONE            ','Identifier of first retarder plate'),
        'RETPLAT2': ('NONE            ','Identifier of second retarder plate'),
        'WCS-ORIG': ('SUBARU', 'Origin of the WCS value'),

}