from __future__ import annotations
import typing as typ

import logging

logg = logging.getLogger(__name__)

import glob
import pathlib

from .file_obj import FitsFileObj

if typ.TYPE_CHECKING:
    StrPath = typ.Union[str, pathlib.Path]

def get_name_no_compextension(strpath: StrPath) -> str:
    path = pathlib.Path(strpath)
    if path.suffix == '.fits':
        return path.name
    elif path.suffix in ['.fz', '.gz']:
        return path.stem
    
    raise AssertionError('get_name_no_compextension - Need a fits, fits.fz, fits.gz file.')


def make_fileobjs_from_globs(
        pos_globs: typ.List[str],
        neg_globs: typ.List[str],
        sort_by_time: bool = True) -> typ.List[FitsFileObj]:

    filename_set: typ.Set[str] = set()
    for pp in pos_globs:
        filename_set = filename_set.union(set(glob.glob(pp)))

    for nn in neg_globs:
        filename_set.intersection_update(set(glob.glob(nn)))

    file_obj_list = make_fileobjs_from_filenames(list(filename_set),
                                                 sort_by_time)

    return file_obj_list


def separate_compression_dups(
    filename_list: typ.Iterable[StrPath],
    filename_fzgz_list: typ.Iterable[StrPath]
) -> typ.List[str]:

    list_pairs: typ.List[typ.Tuple[str, str]] = []

    set_uncomp_paths = {str(fname) for fname in filename_list}
    set_comp_paths = {get_name_no_compextension(fname) for fname in filename_fzgz_list}
    '''
    This symmetric difference uses the magic of the FzGzAgnostic hash function...
    We could have made that WAY simpler.
    '''
    not_comp_only = set_uncomp_paths.difference(set_comp_paths)

    list_uncomp = [str(p) for p in not_comp_only]

    return list_uncomp


def make_fileobjs_from_filenames(
        filename_list: typ.List[str],
        sort_by_time: bool = True) -> typ.List[FitsFileObj]:
    '''
        Turn a list of raw file names into a list of fileobjects
        
        Object based rewrite of former archive_monitor_process_filename
            from scxkw.daemons.g2archiving
    '''

    file_obj_list = [FitsFileObj(filename) for filename in filename_list]

    if sort_by_time:
        file_obj_list.sort(key=lambda fobj: fobj.file_time)
    else:
        file_obj_list.sort(key=lambda fobj: fobj.full_filepath)

    return file_obj_list
