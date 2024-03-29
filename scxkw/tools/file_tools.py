from __future__ import annotations
import typing as typ

import logging

logg = logging.getLogger(__name__)

import glob
import pathlib

from .fits_file_obj import FitsFileObj
from .framelist_file_obj import FrameListFitsFileObj

if typ.TYPE_CHECKING:
    StrPath = typ.Union[str, pathlib.Path]


def convert_to_filelist_obj(fobj: FitsFileObj) -> FrameListFitsFileObj:
    # replace the last occurence of '.fits' with '.fitsframes'
    flist_path = '.fitsframes'.join(str(fobj.full_filepath).rsplit('.fits', 1))

    n_frames = fobj.get_nframes()

    import numpy as np
    flist_data = np.zeros((n_frames, 2), dtype=object)
    flist_data[:, 0] = str(fobj.full_filepath)
    flist_data[:, 1] = np.arange(n_frames).astype(np.uint64)

    flist_fobj = FrameListFitsFileObj(flist_path,
                                      on_disk=False,
                                      header=fobj.fits_header,
                                      data=flist_data,
                                      txt_parser=fobj.txt_file_parser)

    return flist_fobj


def consolidate_framelist_to_fits(
        fobj: FrameListFitsFileObj,
        seek_dict: typ.Optional[typ.Dict[str, FitsFileObj]] = None) -> FitsFileObj:

    fobj._ensure_data_loaded()
    
    import numpy as np

    assert (fobj.data is not None and isinstance(fobj.data, np.ndarray)
            and fobj.data.dtype == object)
    
    required_fits_files = set(fobj.data[:, 0])

    useful_dict: typ.Dict[str, FitsFileObj] = {}

    for req_ffo_str in required_fits_files:
        if seek_dict is not None and req_ffo_str in seek_dict:
            useful_dict[req_ffo_str] = seek_dict[req_ffo_str]
        else:
            useful_dict[req_ffo_str] = FitsFileObj(req_ffo_str)
        useful_dict[req_ffo_str]._ensure_data_loaded()

    fits_path = '.fits'.join(str(fobj.full_filepath).rsplit('.fitsframes', 1))

    # The fact that data is a list an not a single block ndarray might help with speed.
    # Astropy tolerates it (because fits.writeto is called with header != None)
    data = [useful_dict[filename].data[kk] for filename, kk in fobj.data]

    real_fits_obj = FitsFileObj(fits_path,
                                on_disk=False,
                                header=fobj.fits_header,
                                data=data,
                                txt_parser=fobj.txt_file_parser)
    
    return real_fits_obj

    # WARNING! As soon as you write FITS files in vgen2,
    # The patrol daemon is gonna get frameids for them.


def get_fullpath_no_compextension(strpath: StrPath) -> str:
    path = pathlib.Path(strpath)
    if path.suffix == '.fits':
        return str(path.parent / path.name)
    elif path.suffix in ['.fz', '.gz']:
        return str(path.parent / path.stem)

    raise AssertionError(
        'get_name_no_compextension - Need a fits, fits.fz, fits.gz file.')


def make_fileobjs_from_globs(
        pos_globs: typ.List[str],
        neg_globs: typ.List[str],
        sort_by_time: bool = True,
        type_to_use: type = FitsFileObj) -> typ.List[FitsFileObj]:

    filename_set: typ.Set[str] = set()
    for pp in pos_globs:
        filename_set = filename_set.union(set(glob.glob(pp)))

    for nn in neg_globs:
        filename_set.difference_update(set(glob.glob(nn)))

    file_obj_list = make_fileobjs_from_filenames(list(filename_set),
                                                 sort_by_time,
                                                 type_to_use=type_to_use)

    return file_obj_list


def separate_compression_dups(
        filename_list: typ.Iterable[StrPath],
        filename_fzgz_list: typ.Iterable[StrPath]) -> typ.List[str]:

    set_uncomp_paths = {str(fname) for fname in filename_list}
    set_comp_paths = {
        get_fullpath_no_compextension(fname)
        for fname in filename_fzgz_list
    }
    '''
    This symmetric difference uses the magic of the FzGzAgnostic hash function...
    We could have made that WAY simpler.
    '''
    not_comp_only = set_uncomp_paths.difference(set_comp_paths)

    list_uncomp = [str(p) for p in not_comp_only]

    return list_uncomp


def make_fileobjs_from_filenames(
        filename_list: typ.List[str],
        sort_by_time: bool = True,
        type_to_use: type = FitsFileObj) -> typ.List[FitsFileObj]:
    '''
        Turn a list of raw file names into a list of fileobjects
        
        Object based rewrite of former archive_monitor_process_filename
            from scxkw.daemons.g2archiving
    '''

    from tqdm import tqdm
    file_obj_list = [type_to_use(filename) for filename in tqdm(filename_list)]

    if sort_by_time:
        file_obj_list.sort(key=lambda fobj: fobj.file_time)
    else:
        file_obj_list.sort(key=lambda fobj: fobj.full_filepath)

    return file_obj_list


def dump_headers_to_tsv(file_list: typ.Iterable[FitsFileObj],
                        tsv_path: StrPath):
    all_keys: typ.Set[str] = set()

    for fobj in file_list:
        all_keys.update(fobj.fits_header.keys())

    all_keys.remove('COMMENT')

    all_keys_list = list(all_keys)
    all_keys_list.sort()  # alphabetical

    path_save = pathlib.Path(tsv_path)
    assert (path_save.parent.exists() and not path_save.exists()
            and path_save.suffix == '.tsv')

    line = 'FNAME\tFULLPATH\t'
    line += '\t'.join(all_keys_list)
    line += '\n'

    file_lines = [line]

    for fobj in file_list:
        line = f'{fobj.file_name}\t{fobj.full_filepath}'

        for key in all_keys_list:
            line += '\t' + str(fobj.fits_header.get(key, 'NULL'))

        line += '\n'
        file_lines += [line]

    with open(path_save, 'w') as tsvfile:
        # Header row.
        tsvfile.writelines(file_lines)
