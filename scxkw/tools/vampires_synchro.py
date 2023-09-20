from __future__ import annotations
import typing as typ

import logging

logg = logging.getLogger(__name__)
logg.setLevel(logging.DEBUG)

import time
import numpy as np

from scxkw.config import GEN2PATH_PRELIM

from .file_obj import MotherOfFileObj as MFFO

OpT_MFFO = typ.Optional[MFFO]

OTHER_INDEX = lambda n: 3 - n  # 2 -> 1, 1 -> 2

STR_VCAM1 = 'vcam1'
STR_VCAM2 = 'vcam2'
STR_VSYNC = 'vsync'
STR_VBAD = 'vbad'
STRFMT_VSOLO = 'vsolo%d'

U_SYNC_KEY = 'U_SYNC'

VAMPIRES_USEC_TOLERANCING = 400


class VampiresSynchronizer:

    def __init__(self, tolerancing_us: int = VAMPIRES_USEC_TOLERANCING, auto_tolerancing: bool = False) -> None:

        self.queue1: typ.List[MFFO] = []
        self.queue2: typ.List[MFFO] = []
        self.queue_dict_p: typ.Dict[int, typ.List[MFFO]] = {
            1: self.queue1,
            2: self.queue2
        }

        self.last_time_q1: float = 0.0
        self.last_time_q2: float = 0.0

        self.tolerance_usec = tolerancing_us
        self.auto_tol = auto_tolerancing

        self.seen_before: typ.Set[str] = set(
        )  # TODO sanitize from very old files to avoid growing indefinitely.

        self.out_files: typ.Dict[int, OpT_MFFO] = {1: None, 2: None}
        self.out_queues: typ.Dict[int, typ.List[MFFO]] = {1: [], 2: []}

    def feed_file_objs(self, file_objs: typ.Iterable[MFFO]):
        # We make sets to avoid the queues growing forever with repeated calls that pushes the same file over and over.
        qdict1 = {str(file.full_filepath): file for file in self.queue1}
        qdict2 = {str(file.full_filepath): file for file in self.queue2}

        for file_obj in file_objs:
            logg.info(
                f'VampiresSynchronizer::feed_file_objs - file {file_obj.file_name}'
            )
            if file_obj.stream_from_foldername == STR_VCAM1:
                qdict1[str(file_obj.full_filepath)] = file_obj
            elif file_obj.stream_from_foldername == STR_VCAM2:
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
        self.queue_dict_p: typ.Dict[int, typ.List[MFFO]] = {
            1: self.queue1,
            2: self.queue2
        }
        self.queue1.sort(key=lambda fobj: fobj.get_start_unixtime_secs())
        self.queue2.sort(key=lambda fobj: fobj.get_start_unixtime_secs())

    def process_queues(self, throtle_down: int = 30) -> bool:
        status = True
        for _ in range(throtle_down):
            status = self.process_queue_oneshot()
            self.flush_out_queues()
            if not status:
                return False
            
        return True

    def flush_out_queues(self) -> None:
        while self.process_out_queue_oneshot(1):
            pass
        while self.process_out_queue_oneshot(2):
            pass

    def process_out_queue_oneshot(self, idx: int) -> bool:

        # No files
        file = self.out_files[idx]
        if file is None:
            if len(self.out_queues[idx]) == 0:
                return False
            else:
                file = self.out_queues[idx].pop(0)
                self.out_files[idx] = file

        # File is big enough by itself
        assert file is not None
        if (file.get_finish_unixtime_secs() - file.get_start_unixtime_secs() >
                10.0):
            assert file.txt_file_parser is not None

            # Offset by 1 EXPTIME, the same way as for get_start_unixtime
            # This matters for exposures.... well, longer than 10 sec.
            selector = (file.txt_file_parser.fgrab_t_us / 1e6 -
                        file.fits_header['EXPTIME'] -
                        file.get_start_unixtime_secs()) < 10.0
            if sum(selector) > 0:
                file_0 = file.sub_file_nodisk(selector)

                assert file_0.stream_from_foldername == 'vsync' and '.cam' in file_0.file_name  # TODO remove once confident
                file_0.write_to_disk(try_flush_ram = True)

            if sum(~selector) > 0:
                file_1 = file.sub_file_nodisk(~selector)
                self.out_files[idx] = file_1
                # Must return false if this file is ALL the frames. We've done nothing really.
                # Otherwise this creates an infinite loop.
                if sum(selector) == 0:
                    return False
            else:
                self.out_files[idx] = None

            return True

        now = time.time()

        # There is no next file AND the input queues are empty AND we've waited 30 seconds
        if (len(self.out_queues[idx]) == 0 and len(self.queue1) == 0
                and len(self.queue2) == 0
                and (now - file.get_finish_unixtime_secs()) > 30.0):
            assert file.stream_from_foldername == 'vsync' and '.cam' in file.file_name  # TODO remove once confident
            file.write_to_disk(try_flush_ram = True)
            self.out_files[idx] = None

            return True

        # There is a next file

        if len(self.out_queues[idx]) > 0:
            next_file = self.out_queues[idx].pop(0)

            # and some critical parameters have changed OR
            # and there's 3 second gap
            if (next_file.fits_header['EXPTIME'] != file.fits_header['EXPTIME']
                    or next_file.fits_header['NAXIS1'] !=
                    file.fits_header['NAXIS1']
                    or next_file.fits_header['NAXIS2'] !=
                    file.fits_header['NAXIS2']
                    or next_file.fits_header['RET-ANG1'] !=
                    file.fits_header['RET-ANG1']
                    or next_file.get_start_unixtime_secs() -
                    file.get_finish_unixtime_secs() > 3.0):
                assert file.stream_from_foldername == 'vsync' and '.cam' in file.file_name  # TODO remove once confident
                file.write_to_disk(try_flush_ram = True)
                self.out_files[idx] = next_file
                return True

            # and there's no big gap - FIXME TODO try if possible to split before merging.
            self.out_files[idx] = file.merge_with_file_after(next_file)

            return True

        # We have a file, it's not big enough, but the timeout has not elapsed,
        # and we have no next file...
        return False

    def find_pop_earliest_file(self) -> typ.Tuple[typ.Optional[MFFO], int]:
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
            return self.find_pop_earliest_file()  # reCURSE.

        return file, idx

    def process_queue_oneshot(self) -> bool:
        '''
            Returns False if nothing has been done i.e. queues are empty or we have
            only one queue but the timeout has not elapsed yet.
            Super not thread safe, we should not requeue files that are already in the
            queue, because they eventually get deleted.
        '''

        earliest_file, v_idx = self.find_pop_earliest_file()  # POPPED
        if earliest_file is None:  # Both queues empty.
            return False

        v_other_idx: int = OTHER_INDEX(v_idx)

        # We've already found the earlier file
        time_finish_file = earliest_file.get_finish_unixtime_secs()
        time_now = time.time(
        )  # It's the time now, but we assume really that we've
        # just rescanned the drive for files.

        if self.is_bad_file(earliest_file):
            logg.error(
                f'VampiresSynchronizer::process_queue_oneshot - BAD FILE')
            earliest_file.move_file_to_streamname(STR_VBAD)
            return True

        if self.is_trivial_solo_vamp_file(earliest_file):
            logg.info(f'VampiresSynchronizer::process_queue_oneshot - '
                      f'{earliest_file.file_name} is trivial vsolo.')
            earliest_file.edit_header(U_SYNC_KEY, False)
            earliest_file.move_file_to_streamname(STRFMT_VSOLO % v_idx)
            return True

        if len(self.queue_dict_p[v_other_idx]) == 0:
            to_match_file = None
        else:
            to_match_file = self.queue_dict_p[v_other_idx][0]  # NOT POPPED

        # to_match_file that starts without an overlap.
        if to_match_file is not None and to_match_file.get_start_unixtime_secs(
        ) > time_finish_file:
            logg.warning(
                f'VampiresSynchronizer::process_queue_oneshot - '
                f'{earliest_file.file_name} has no temporally overlapping file.'
            )
            earliest_file.edit_header(U_SYNC_KEY, False)
            earliest_file.move_file_to_streamname(STRFMT_VSOLO % v_idx)
            return True

        # no to_match_file and > 1 min elapsed.
        if to_match_file is None:
            if time_now - time_finish_file > 60.0:
                logg.warning(
                    f'VampiresSynchronizer::process_queue_oneshot - '
                    f'{earliest_file.file_name} has no other stream file for 1 minute.'
                )
                earliest_file.edit_header(U_SYNC_KEY, False)
                earliest_file.move_file_to_streamname(STRFMT_VSOLO % v_idx)
                return True
            else:
                logg.warning(
                    f'VampiresSynchronizer::process_queue_oneshot - '
                    f'{earliest_file.file_name} has no other stream file for now.'
                )
                # We need to re-queue
                self.queue_dict_p[v_idx].insert(0, earliest_file)
                return False
        '''
        We now have a guaranteed temporal overlap between earliest_file and to_match_file.
        '''
        # We pop the to_merge_file for good, it's gonna be affected.
        _ = self.queue_dict_p[v_other_idx].pop(0)

        # Could be a badfile
        if self.is_bad_file(to_match_file):
            logg.error(
                f'VampiresSynchronizer::process_queue_oneshot - BAD FILE')
            to_match_file.move_file_to_streamname(STR_VBAD)
            self.queue_dict_p[v_idx].insert(0, earliest_file)  # Re-enqueue
            return True
        # Could be that the to_match_file is a single frame OR has no EXTTRIG
        if self.is_trivial_solo_vamp_file(to_match_file):
            logg.info(f'VampiresSynchronizer::process_queue_oneshot - '
                      f'{to_match_file.file_name} is trivial vsolo.')
            to_match_file.edit_header(U_SYNC_KEY, False)
            to_match_file.move_file_to_streamname(STRFMT_VSOLO % v_other_idx)
            self.queue_dict_p[v_idx].insert(0, earliest_file)  # Re-enqueue
            return True

        if v_idx == 1:
            file_v1, file_v2 = earliest_file, to_match_file
        else:
            file_v1, file_v2 = to_match_file, earliest_file

        (r1, r2, fobj_merge_1, fobj_merge_2, fobj_remainder_1, fobj_remainder_2) = \
            resync_two_files(file_v1, file_v2, self.tolerance_usec, self.auto_tol)
        # Put the remainders back in the queues, at the head.

        # I don't want to be bothered with files with very low dimensionality.
        # Actually fobj_merge_1 and fobj_merge_2 exist both or None, and must have the same number of frames...

        if fobj_merge_1 is not None:
            assert fobj_merge_2 is not None
            assert (fobj_merge_1.txt_file_parser is not None
                    and fobj_merge_2.txt_file_parser is not None)
            # Force synced timings on merged files.
            timings = 0.5 * (fobj_merge_1.txt_file_parser.fgrab_t_us +
                             fobj_merge_2.txt_file_parser.fgrab_t_us)
            fobj_merge_1.txt_file_parser.fgrab_t_us = timings
            fobj_merge_2.txt_file_parser.fgrab_t_us = timings
            fobj_merge_1.txt_file_parser.logshim_t_us = timings
            fobj_merge_2.txt_file_parser.logshim_t_us = timings
            fobj_merge_1.txt_file_parser._regenerate_lines_from_arrays()
            fobj_merge_2.txt_file_parser._regenerate_lines_from_arrays()

            fobj_merge_1.rename_from_first_frame()
            fobj_merge_2.rename_from_first_frame()
            fobj_merge_1.move_file_to_streamname(STR_VSYNC,
                                                 also_change_filename=True)
            fobj_merge_2.move_file_to_streamname(STR_VSYNC,
                                                 also_change_filename=True)
            fobj_merge_1.edit_header(U_SYNC_KEY, True)
            fobj_merge_2.edit_header(U_SYNC_KEY, True)
            fobj_merge_1.add_suffix_to_filename('.cam1')
            fobj_merge_2.add_suffix_to_filename('.cam2')

            self.out_queues[1].append(fobj_merge_1)
            self.out_queues[2].append(fobj_merge_2)

        # At this point there may be a name conflict between original files and remainder files.
        file_v1.delete_from_disk(try_purge_ram=True, silent_fail=True)
        file_v2.delete_from_disk(try_purge_ram=True, silent_fail=True)

        # So, another problem is now that if files overlap but do not sync, and we requeue them,
        # It causes an infinite loop...

        if fobj_remainder_1 is not None and str(
                fobj_remainder_1) in self.seen_before:
            fobj_remainder_1.edit_header(U_SYNC_KEY, False)
            fobj_remainder_1.move_file_to_streamname(STRFMT_VSOLO % 1)
            b = save_to_disk_if(fobj_remainder_1, 1, r1 < 0.95)
            logg.debug(f'A branch: {r1} - {fobj_remainder_1} - {b}')
        elif save_to_disk_if(fobj_remainder_1, 1, r1 < 0.95):
            assert fobj_remainder_1 is not None  # mypy
            self.feed_file_objs([fobj_remainder_1])
            logg.debug(f'B branch: {r1} - {fobj_remainder_1}')

        if fobj_remainder_2 is not None and str(
                fobj_remainder_2) in self.seen_before:
            fobj_remainder_2.edit_header(U_SYNC_KEY, False)
            fobj_remainder_2.move_file_to_streamname(STRFMT_VSOLO % 2)
            b = save_to_disk_if(fobj_remainder_2, 1, r2 < 0.95)
            logg.debug(f'C branch: {r2} - {fobj_remainder_2} - {b}')
        elif save_to_disk_if(fobj_remainder_2, 1, r2 < 0.95):
            assert fobj_remainder_2 is not None  # mypy
            self.feed_file_objs([fobj_remainder_2])
            logg.debug(f'D branch: {r2} - {fobj_remainder_2}')

        return True

    def is_trivial_solo_vamp_file(self, file: MFFO) -> bool:
        # We can only synchro if the cameras are in exttrig
        return file.fits_header['EXTTRIG'] is False

    def is_bad_file(self, file: MFFO) -> bool:
        # We need EXPTIME and DET-NSMP because they allow to calculate the start and end
        # of a single frame file properly and give it some temporal "thickness"
        return  (file.txt_file_parser is None or
                file.get_nframes() != len(file.txt_file_parser.fgrab_t_us)
                or file.fits_header is None
                or not 'EXPTIME' in file.fits_header
                or file.fits_header['EXPTIME'] is None)


def save_to_disk_if(file_obj: OpT_MFFO,
                    min_frames: int = 1,
                    misc_condition: bool = True):
    if (file_obj is not None and misc_condition
            and file_obj.get_nframes() >= min_frames):
        file_obj.write_to_disk()
        return True
    elif file_obj is not None:
        logg.warning(
            f'save_to_disk_if - '
            f'ditching file {file_obj.file_name} ({file_obj.get_nframes()} frames @ EXPTIME {file_obj.fits_header["EXPTIME"]})'
        )
    else:
        logg.warning(f'save_to_disk_if - file is None')
    return False

def find_sync_tol_for_vamp(file: MFFO) -> int:
    # Don't assume NAXIS1 and NAXIS2 haven't been messed up by the
    # fitsframes files etc...
    size_rows = file.fits_header['PRD-RNG2']
    readout_mode_slow = file.fits_header['U_DETMOD'].strip().lower() == 'slow'
    slow_factor = (1.0, 2.5)[readout_mode_slow]
    
    prow_allowed = 0.434 # 434 nsec per row.

    return int(slow_factor * size_rows * prow_allowed)



def resync_two_files(file_v1: MFFO, file_v2: MFFO,
                     tolerance_usec: int = VAMPIRES_USEC_TOLERANCING,
                       auto_tolerance_us: bool = False) -> \
        typ.Tuple[float, float, OpT_MFFO, OpT_MFFO, OpT_MFFO, OpT_MFFO]:

    assert (file_v1.txt_file_parser is not None
            and file_v2.txt_file_parser is not None)
    timings_v1 = file_v1.txt_file_parser.fgrab_t_us
    timings_v2 = file_v2.txt_file_parser.fgrab_t_us

    
    usec_tol = min(find_sync_tol_for_vamp(file_v1), find_sync_tol_for_vamp(file_v2)) if auto_tolerance_us else tolerance_usec

    common_arr_v1, common_arr_v2 = sync_timing_arrays(
        timings_v1, timings_v2, tolerance_us=usec_tol)

    # Fraction of successfully synced up frames
    ratio1 = np.sum(common_arr_v1) / len(common_arr_v1)
    ratio2 = np.sum(common_arr_v2) / len(common_arr_v2)

    file_solo_1 = file_v1.sub_file_nodisk(
        ~common_arr_v1) if ratio1 < 1.0 else None
    file_solo_2 = file_v2.sub_file_nodisk(
        ~common_arr_v2) if ratio2 < 1.0 else None
    file_common_1 = file_v1.sub_file_nodisk(
        common_arr_v1) if ratio1 > 0.0 else None
    file_common_2 = file_v2.sub_file_nodisk(
        common_arr_v2) if ratio2 > 0.0 else None

    return (ratio1, ratio2, file_common_1, file_common_2, file_solo_1,
            file_solo_2)


import numba


@numba.jit  # JIT is particularly efficient on that type of branch-loop code.
def sync_timing_arrays(time_v1: np.ndarray, time_v2: np.ndarray,
                       tolerance_us: int = VAMPIRES_USEC_TOLERANCING,
                       strict=False) -> \
        typ.Tuple[np.ndarray, np.ndarray]:

    n1, n2 = len(time_v1), len(time_v2)
    common_array_v1 = np.zeros(len(time_v1), np.bool_)
    common_array_v2 = np.zeros(len(time_v2), np.bool_)

    # Alright... double ladder loop to find pairs.
    k1, k2 = 0, 0

    current_tolerance_us = tolerance_us

    while k1 < n1 and k2 < n2:
        earliest = 1 if time_v1[k1] < time_v2[k2] else 2

        time_1 = time_v1[k1]
        time_2 = time_v2[k2]

        if abs(time_1 - time_2) < tolerance_us:
            common_array_v1[k1] = True
            common_array_v2[k2] = True
            k1 += 1
            k2 += 1
            if not strict:
                current_tolerance_us = min(2.5 * tolerance_us,
                                           1.1 * current_tolerance_us)
        else:
            if not strict and abs(time_1 - time_2) < current_tolerance_us:
                common_array_v1[k1] = True
                common_array_v2[k2] = True
                k1 += 1
                k2 += 1
                current_tolerance_us = max(tolerance_us,
                                           0.9 * current_tolerance_us)
            else:
                # Contender time is more than tolerance_s too late:
                if earliest == 1:
                    k1 += 1
                else:
                    k2 += 1
                current_tolerance_us = tolerance_us

    return common_array_v1, common_array_v2


'''
TESTING


from camstack.cams.simulatedcam import SimulatedCam
cam = SimulatedCam('vcam1', 'vcam1', mode_id=(128, 128))
cam._set_formatted_keyword('EXTTRIG', True)



from pyMilk.interfacing.shm import SHM
v1 = SHM('vcam1')
v2 = SHM('vcam2', v1.get_data())
while True:
    d = v1.get_data(True)
    k = v1.get_keywords()
    v2.set_keywords(k)
    v2.set_data(d)


import time
from scxkw.daemons.gen2_archiving import VampiresSynchronizer, synchronize_vampires_files
syncer = VampiresSynchronizer()
while True:
    time.sleep(5.0)
    synchronize_vampires_files(sync_manager=syncer)

milk-logshim vcam1 200 /tmp/ARCHIVE1/20230620/vcam1/
milk-logshim vcam2 250 /tmp/ARCHIVE1/20230620/vcam2/




MORE TESTING
import os
from scxkw.tools import file_tools
v1_fobjs = file_tools.make_fileobjs_from_globs([os.path.abspath('./vcam1/*.fits')], [])
v2_fobjs = file_tools.make_fileobjs_from_globs([os.path.abspath('./vcam2/*.fits')], [])
from scxkw.tools.vampires_synchro import VampiresSynchronizer
syncer = VampiresSynchronizer()
syncer.feed_file_objs(v1_fobjs)
syncer.feed_file_objs(v2_fobjs)
'''


'''s
# FIXME this poses issue per deleting the txt files maybe too early?
import os, psutil
from scxkw.tools import file_tools
from scxkw.tools.framelist_file_obj import FrameListFitsFileObj
v1_fobjs = file_tools.make_fileobjs_from_globs(['/mnt/tier1/ARCHIVED_DATA/20230730/vcam1/*.fits'], [])
for fo in v1_fobjs:
    fo.ut_sanitize()
v2_fobjs = file_tools.make_fileobjs_from_globs(['/mnt/tier1/ARCHIVED_DATA/20230730/vcam2/*.fits'], [])
for fo in v2_fobjs:
    fo.ut_sanitize()
FrameListFitsFileObj.DEBUG = False
v1_fobjs = [file_tools.convert_to_filelist_obj(fo) for fo in v1_fobjs if fo.txt_exists] # txtbak copy before doing this?
v2_fobjs = [file_tools.convert_to_filelist_obj(fo) for fo in v2_fobjs if fo.txt_exists]
#FrameListFitsFileObj.DEBUG = True
[fo.write_to_disk() for fo in v1_fobjs]
[fo.write_to_disk() for fo in v2_fobjs]
from scxkw.tools.vampires_synchro import VampiresSynchronizer
syncer = VampiresSynchronizer()
syncer.feed_file_objs(v1_fobjs)
syncer.feed_file_objs(v2_fobjs)
'''
