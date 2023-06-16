from __future__ import annotations
import typing as typ

import logging
logg = logging.getLogger(__name__)


from .file_obj import FitsFileObj


def make_fileobjs_from_filenames(filename_list: typ.List[str],
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




