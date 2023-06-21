from __future__ import annotations
import typing as typ

import logging

logg = logging.getLogger(__name__)

import time
import numpy as np

from scxkw.config import GEN2PATH_PRELIM

from .file_obj import FitsFileObj
T_FFO = FitsFileObj
OpT_FFO = typ.Optional[FitsFileObj]

OTHER_INDEX = lambda n: 3 - n # 2 -> 1, 1 -> 2


class VampiresSynchronizer:

    def __init__(self) -> None:
        self.queue1: typ.List[T_FFO] = []
        self.queue2: typ.List[T_FFO] = []
        self.queue_dict_p: typ.Dict[int, typ.List[T_FFO]] = {1: self.queue1,
                                                                   2: self.queue2}

        self.last_time_q1: float = 0.0
        self.last_time_q2: float = 0.0


        self.seen_before: typ.Set[str] = set() # TODO sanitize from very old files to avoid growing indefinitely.

    def feed_file_objs(self, file_objs: typ.Iterable[T_FFO]):
        # We make sets to avoid the queues growing forever with repeated calls that pushes the same file over and over.
        qdict1 = {str(file.full_filepath): file for file in self.queue1}
        qdict2 = {str(file.full_filepath): file for file in self.queue2}

        for file_obj in file_objs:
            logg.info(f'VampiresSynchronizer::feed_file_objs - file {file_obj.file_name}')
            if file_obj.stream_from_foldername == 'vcam1':
                qdict1[str(file_obj.full_filepath)] = file_obj
            elif file_obj.stream_from_foldername == 'vcam2':
                qdict2[str(file_obj.full_filepath)] = file_obj
            else:
                message = (f'VampiresSynchronizer::feed_file_objs - '
                           f'invalid stream {file_obj.stream_from_foldername}')
                logg.critical(message)
                raise ValueError(message)
            self.seen_before.add(str(file_obj))
        
        # Re-sort queues by time/name
        self.queue1 = list([qdict1[fn] for fn in qdict1])
        self.queue2 = list([qdict2[fn] for fn in qdict2])
        self.queue_dict_p: typ.Dict[int, typ.List[T_FFO]] = {1: self.queue1,
                                                             2: self.queue2}
        self.queue1.sort(key=lambda fobj: fobj.get_start_unixtime_secs())
        self.queue2.sort(key=lambda fobj: fobj.get_start_unixtime_secs())

    def find_pop_earliest_file(self) -> typ.Tuple[typ.Optional[T_FFO], int]:
        if len(self.queue1) == 0 and len(self.queue2) == 0:
            return None, 0

        if len(self.queue1) > 0 and len(self.queue2) == 0:
            return self.queue1.pop(0), 1

        if len(self.queue1) == 0 and len(self.queue2) > 0:
            return self.queue2.pop(0), 2
        
        time_q1 = self.queue1[0].get_start_unixtime_secs()
        time_q2 = self.queue2[0].get_start_unixtime_secs()
        if time_q1 < time_q2:
            file, idx = self.queue1.pop(0), 1
        else:
            file, idx = self.queue2.pop(0), 2

        if not file.check_existence_on_disk():
            logg.error(f'VampiresSynchronizer::find_pop_earliest_file - '
                       f'{file} not on disk.')
            return self.find_pop_earliest_file() # reCURSE.
        
        return file, idx

    def process_queue_oneshot(self) -> bool:
        '''
            Returns False if nothing has been done i.e. queues are empty or we have
            only one queue but the timeout has not elapsed yet.
            Super not thread safe, we should not requeue files that are already in the
            queue, because they eventually get deleted.
        '''

        earliest_file, v_idx = self.find_pop_earliest_file() # POPPED
        if earliest_file is None: # Both queues empty.
            return False
        
        v_other_idx: int = OTHER_INDEX(v_idx)

        # We've already found the earlier file
        time_finish_file = earliest_file.get_finish_unixtime_secs()
        time_now = time.time() # It's the time now, but we assume really that we've
        # just rescanned the drive for files.

        if self.is_trivial_solo_vamp_file(earliest_file):
            logg.info(f'VampiresSynchronizer::process_queue_oneshot - '
                      f'{earliest_file.file_name} is trivial vsolo.')
            earliest_file.move_file_to_streamname(f'vcamsolo{v_idx}')
            return True

        if len(self.queue_dict_p[v_other_idx]) == 0:
            to_match_file = None
        else:
            to_match_file = self.queue_dict_p[v_other_idx][0] # NOT POPPED

        # to_match_file that starts without an overlap.
        if to_match_file is not None and to_match_file.get_start_unixtime_secs() > time_finish_file:
            logg.warning(f'VampiresSynchronizer::process_queue_oneshot - '
                         f'{earliest_file.file_name} has no temporally overlapping file.')
            earliest_file.move_file_to_streamname(f'vcamsolo{v_idx}')
            return True

        # no to_match_file and > 1 min elapsed.
        if to_match_file is None:
            if time_now - time_finish_file > 60.0:
                logg.warning(f'VampiresSynchronizer::process_queue_oneshot - '
                            f'{earliest_file.file_name} has no other stream file for 1 minute.')
                earliest_file.move_file_to_streamname(f'vcamsolo{v_idx}')
                return True
            else:
                logg.warning(f'VampiresSynchronizer::process_queue_oneshot - '
                            f'{earliest_file.file_name} has no other stream file for now.')
                # We need to re-queue
                self.queue_dict_p[v_idx].insert(0, earliest_file)
                return False
        '''
        We now have a guaranteed temporal overlap between earliest_file and to_match_file.
        '''
        # We pop the to_merge_file for good, it's gonna be affected.
        _ = self.queue_dict_p[v_other_idx].pop(0)

        # Could be that the to_match_file is a single frame OR has no EXTTRIG
        if self.is_trivial_solo_vamp_file(to_match_file):
            logg.info(f'VampiresSynchronizer::process_queue_oneshot - '
                      f'{to_match_file.file_name} is trivial vsolo.')
            to_match_file.move_file_to_streamname(f'vcamsolo{v_other_idx}')
            self.queue_dict_p[v_idx].insert(0, earliest_file)
            return True

        if v_idx == 1:
            file_v1, file_v2 = earliest_file, to_match_file
        else:
            file_v1, file_v2 = to_match_file, earliest_file

        (r1, r2, fobj_merge_1, fobj_merge_2, fobj_remainder_1, fobj_remainder_2) = \
            resync_two_files(file_v1, file_v2)
        # Put the remainders back in the queues, at the head.

        # I don't want to be bothered with files with very low dimensionality.
        if fobj_merge_1 is not None and fobj_merge_1.get_nframes() >= 2:
            fobj_merge_1.move_file_to_streamname('vsync', also_change_filename=True)
            fobj_merge_1.add_suffix_to_filename('.cam1')
            fobj_merge_1.write_to_disk()
        if fobj_merge_2 is not None and fobj_merge_2.get_nframes() >= 2:            
            fobj_merge_2.move_file_to_streamname('vsync', also_change_filename=True)
            fobj_merge_2.add_suffix_to_filename('.cam2')
            fobj_merge_2.write_to_disk()

        # At this point there may be a name conflict between original files and remainder files.
        file_v1.delete_from_disk()
        file_v2.delete_from_disk()

        # So, another problem is now that if files overlap but do not sync, and we requeue them,
        # It causes an infinite loop...

        if fobj_remainder_1 is not None and str(fobj_remainder_1) in self.seen_before:
            fobj_remainder_1.move_file_to_streamname(f'vcamsolo1')
            b = save_to_disk_if(fobj_remainder_1, 2, r1 < 0.95)
            logg.debug(f'A branch: {r1} - {fobj_remainder_1} - {b}')
        elif save_to_disk_if(fobj_remainder_1, 2, r1 < 0.95):
            assert fobj_remainder_1 is not None # mypy
            self.feed_file_objs([fobj_remainder_1])
            logg.debug(f'B branch: {r2} - {fobj_remainder_2}')

        if fobj_remainder_2 is not None and str(fobj_remainder_2) in self.seen_before:
            fobj_remainder_2.move_file_to_streamname(f'vcamsolo2')
            b = save_to_disk_if(fobj_remainder_2, 2, r2 < 0.95)
            logg.debug(f'C branch: {r2} - {fobj_remainder_2} - {b}')
        elif save_to_disk_if(fobj_remainder_2, 2, r2 < 0.95):
            logg.debug(f'D branch: {r2} - {fobj_remainder_2}')
            assert fobj_remainder_2 is not None # mypy
            self.feed_file_objs([fobj_remainder_2])

        return True

    def is_trivial_solo_vamp_file(self, file: T_FFO):
        # We can only synchro if the cameras are in exttrig
        return (not file.fits_header['EXTTRIG'] or
                file.get_nframes() <= 1)

def save_to_disk_if(file_obj: OpT_FFO, min_frames: int = 2, misc_condition: bool = True):
        if (file_obj is not None and misc_condition and file_obj.get_nframes() >= min_frames):
            file_obj.write_to_disk()
            return True
        elif file_obj is not None:
            logg.warning(f'save_to_disk_if - '
                         f'ditching file {file_obj.file_name}')
        else:
            logg.warning(f'save_to_disk_if - file is None')
        return False

def resync_two_files(file_v1: T_FFO, file_v2: T_FFO) -> \
        typ.Tuple[float, float, OpT_FFO, OpT_FFO, OpT_FFO, OpT_FFO]:


    assert (file_v1.txt_file_parser is not None and
            file_v2.txt_file_parser is not None)
    timings_v1 = file_v1.txt_file_parser.fgrab_t_us
    timings_v2 = file_v2.txt_file_parser.fgrab_t_us

    common_arr_v1, common_arr_v2 = sync_timing_arrays(timings_v1, timings_v2)

    # Fraction of successfully synced up frames
    ratio1 = np.sum(common_arr_v1) / len(common_arr_v1)
    ratio2 = np.sum(common_arr_v2) / len(common_arr_v2)

    file_solo_1 = file_v1.sub_file_nodisk(~common_arr_v1) if ratio1 < 1.0 else None
    file_solo_2 = file_v2.sub_file_nodisk(~common_arr_v2) if ratio2 < 1.0 else None
    file_common_1 = file_v1.sub_file_nodisk(common_arr_v1) if ratio1 > 0.0 else None
    file_common_2 = file_v2.sub_file_nodisk(common_arr_v2) if ratio2 > 0.0 else None

    return (ratio1, ratio2, file_common_1, file_common_2, file_solo_1, file_solo_2)





import numba
@numba.jit # JIT is particularly efficient on that type of branch-loop code.
def sync_timing_arrays(time_v1: np.ndarray, time_v2: np.ndarray, tolerance_us = 80) -> \
        typ.Tuple[np.ndarray, np.ndarray]:

    n1, n2 = len(time_v1), len(time_v2)
    common_array_v1 = np.zeros(len(time_v1), np.bool_)
    common_array_v2 = np.zeros(len(time_v2), np.bool_)


    # Alright... double ladder loop to find pairs.
    k1, k2 = 0, 0

    while k1 < n1 and k2 < n2:
        earliest = 1 if time_v1[k1] < time_v2[k2] else 2

        time_1 = time_v1[k1]
        time_2 = time_v2[k2]

        if abs(time_1 - time_2) < tolerance_us:
            common_array_v1[k1] = True
            common_array_v2[k2] = True
            k1 += 1
            k2 += 1
        else:
            # Contender time is more than tolerance_s too late:
            if earliest == 1:
                k1 += 1
            else:
                k2 += 1

    return common_array_v1, common_array_v2
        




