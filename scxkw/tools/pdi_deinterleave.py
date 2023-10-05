from __future__ import annotations
import typing as typ
from typing import Optional as t_Op

import logging

logg = logging.getLogger(__name__)

import numpy as np
import subprocess as sproc
from astropy.io import fits
from enum import IntEnum

from .logshim_txt_parser import LogshimTxtParser

if typ.TYPE_CHECKING:
    from .file_obj import MotherOfFileObj as MFFO
    OpT_FFO = typ.Optional[MFFO]

class PDIJobCodeEnum(IntEnum):
    ALREADY_RUNNING = -3
    NOFILE = -2
    TOOMANY = -1
    STARTED = 0
    SUCCESS = 1
    NOTHING = 2


class SyncPDIDeintManager:
    def __init__(self) -> None:
        pass

    def run_pdi_deint_job(self, file_obj: MFFO, new_stream_name: str) -> PDIJobCodeEnum:
        do_deint = deinterleave_filechecker([file_obj])[0]

        if not do_deint:
            return PDIJobCodeEnum.NOTHING
        
        _DETECTOR: str = file_obj.fits_header['DETECTOR'] # type: ignore
        ir_true_vis_false = _DETECTOR in ['CRED1 - APAPANE', 'CRED2 - PALILA']

        deint_files = deinterleave_file(file_obj,
                                        ir_true_vis_false=ir_true_vis_false,
                                        flc_jitter_us_hint = None,
                                        write_to_disk=False)
    
        for fkey in deint_files:
            deint_files[fkey].move_file_to_streamname(new_stream_name)
            deint_files[fkey].write_to_disk()

        file_obj.delete_from_disk()

        return PDIJobCodeEnum.SUCCESS


class AsyncPDIDeintJobManager:
    MAX_CONCURRENT_JOBS = 15

    def __init__(self) -> None:
        self.pending_jobs: typ.Dict[str, sproc.Popen] = {}

        # Now check for active fpack jobs that this manager didn't launch.
        running_pdi_str = sproc.run(
            'ps -eo args | egrep ^scxkw-pdideint', shell=True,
            capture_output=True).stdout.decode('utf8').strip()
        if running_pdi_str == '':
            running_pdi = []
        else:
            running_pdi = running_pdi_str.split('\n')

        if len(running_pdi) > 0:
            logg.error('PDIDeintJobManager::__init__ - Running PDI deint jobs:')
            logg.error(str(running_pdi))
            raise AssertionError(
                'There are running scxkw-pdideinterleave jobs on the system. '
                'It is bad juju to instantiate a PDIDeintJobManager now.')

    def run_pdi_deint_job(self, file_obj: MFFO, new_stream_name: str) -> PDIJobCodeEnum:
        if not file_obj.check_existence_on_disk():
            logg.error(f'PDIDeintJobManager: file {file_obj} does not exist.')
            return PDIJobCodeEnum.NOFILE
        if len(self.pending_jobs) == self.MAX_CONCURRENT_JOBS:
            logg.error(f'PDIDeintJobManager: max allowed ({self.MAX_CONCURRENT_JOBS})'
                       'deint jobs already running at the same time.')
            return PDIJobCodeEnum.TOOMANY
        if str(file_obj.full_filepath) in self.pending_jobs:
            return PDIJobCodeEnum.ALREADY_RUNNING

        proc = deinterleave_start_job_async(str(file_obj.full_filepath), new_stream_name, keep_original=False)

        self.pending_jobs[str(file_obj.full_filepath)] = proc

        return PDIJobCodeEnum.STARTED

    def refresh_running_jobs(self) -> None:
        # job.poll() is None if process is still running.
        self.pending_jobs = {
            filename: self.pending_jobs[filename]
            for filename in self.pending_jobs
            if self.pending_jobs[filename].poll() is None
        }

def deinterleave_filechecker(file_list: typ.List[MFFO]) -> typ.List[bool]:
    '''
        return False for files that need NOT to be deinterleaved
        return True for files that have to be deinterleaved
    '''

    statuses: typ.List[bool] = [False for n in range(len(file_list))]

    for kk, file in enumerate(file_list):
        header = fits.getheader(file.full_filepath)

        detector = header['DETECTOR']

        valid_fpdi_file = (detector in ['CRED1 - APAPANE', 'CRED2 - PALILA']
                           and header['X_IFLCST'] == 'ON' and header['EXTTRIG']
                           and header['X_IFLCAB'] == 'NA')
        valid_vpdi_file = (detector in ['VCAM1 - OrcaQ', 'VCAM2 - OrcaQ']
                           and header['EXTTRIG'] is True
                           and header['U_FLCEN'] is True
                           and header['U_FLCST'] == 'IN'
                           and (header['U_FLC'] is None or header['U_FLC'] == 'NA'))
        
        #if valid_vpdi_file:
        #    print(file.full_filepath)

        if valid_fpdi_file or valid_vpdi_file:
            statuses[kk] = True
            logg.info(f'deinterleave_filechecker: valid {file.full_filepath}')

    return statuses


def deinterleave_start_job_async(file_name: str,
                                 new_stream_name: str,
                                 keep_original: bool = True,) -> sproc.Popen:
    cmd = f'scxkw-pdideinterleave {file_name}' + ('', ' --keep')[keep_original] + \
            ('' if new_stream_name is None else f' --dstream={new_stream_name}')

    return sproc.Popen(cmd.split(' '))


def deinterleave_file(file_obj: MFFO, *,
                      ir_true_vis_false: bool = True,
                      flc_jitter_us_hint: t_Op[int] = None,
                      write_to_disk: bool = False):
    '''
    This file will carry the header on to the split files.
    BUT it will not bother checking the header is sane.
    This should be done upstream and the relevant stuff is to be passed in the parameter.
    '''

    file_obj._ensure_data_loaded()
    assert (file_obj.is_compressed is False and
            file_obj.is_archived is False and
            file_obj.txt_file_parser is not None)

    key = ('U_TRIGJT', 'X_IFLCJT')[ir_true_vis_false]
    if flc_jitter_us_hint is None:
        flc_jitter_us: int = file_obj.fits_header[key] # type: ignore
    else:
        flc_jitter_us = flc_jitter_us_hint

    flc_state, _ = deinterleave_compute(file_obj.txt_file_parser.fgrab_dt_us, flc_jitter_us, True)

    key_flc_state = ('U_FLC', 'X_IFLCAB')[ir_true_vis_false]
    ret_files: typ.Dict[str, MFFO] = {}
    # A is -1 and B is +1 and that MUST NOT CHANGE for VAMPIRES
    # For FPDI, we can figure it out...
    for flcval, key in zip([-1, 0, 1], ['A', 'D', 'B']):
        
        n = np.sum(flc_state == flcval)
        if n < 2:
            continue

        #keep_name_timestamp: in this case, the full_path is the same as the parent until you add a suffix!!
        subfile = file_obj.sub_file_nodisk(flc_state == flcval, add_suffix = '.' + key, keep_name_timestamp=True)

        subfile.fits_header[key_flc_state] = '%-16.16s' % key

        if write_to_disk:
            logg.info(f'deinterleave_file: writing {subfile.full_filepath}')
            subfile.write_to_disk()

        ret_files[key] = subfile

    return ret_files

    

    


DataOpTxt = typ.Tuple[np.ndarray, t_Op[LogshimTxtParser]]
def deinterleave_data(data: np.ndarray, dt_jitter_us: int, txt_parser: LogshimTxtParser) -> typ.List[DataOpTxt]:
    '''
        Deinterleave a data cube based on a logshim txt parser object.

        THIS NEVER GETS CALLED IN THE PIPELINE (SEPT 2023)
    '''
    
    n_frames = data.shape[0] + 1
    default_hamming_size = 30

    if n_frames <= 2: # Can't deinterleave with only one or two frames
        flc_state = flc_state = np.zeros(n_frames, np.int32)
    elif n_frames <= 2 * default_hamming_size + 1:
        raise NotImplementedError('deinterleave_compute_small not impl.')
        flc_state, _ = deinterleave_compute_small(txt_parser.fgrab_dt_us, dt_jitter_us)
    else:
        flc_state, _ = deinterleave_compute(txt_parser.fgrab_dt_us, dt_jitter_us, True)

    data_a = data[flc_state == -1]
    data_b = data[flc_state == 1]
    data_garbage = data[flc_state == 0]

    n_a = len(data_a)
    n_b = len(data_b)
    n_g = len(data_garbage)

    if n_a > 0:
        parser_a = txt_parser.sub_parser_by_selection('A', flc_state == -1)
    else:
        parser_a = None
    if n_b > 0:
        parser_b = txt_parser.sub_parser_by_selection('B', flc_state == 1)
    else:
        parser_b = None
    if n_g > 0:
        parser_garbage = txt_parser.sub_parser_by_selection('D', flc_state == 0)
    else:
        parser_garbage = None

    return [(data_a, parser_a), (data_b, parser_b), (data_garbage, parser_garbage)]



def deinterleave_compute(dt_array: np.ndarray,
                         dt_jitter_us: int,
                         enforce_pairing: bool, *,
                         hamm_size: t_Op[int] = None,
                         clip_vals: t_Op[typ.Tuple[float,float]] = None,
                         corr_trust_margin: float = 0.4
                         ) -> typ.Tuple[np.ndarray, np.ndarray | None]:
    '''
        Validate the FLC state from an array of timing deltas

        if hamm_size is None, auto hamming
        if hamm_size is int and the buffer is too small, it will crash
    '''

    if hamm_size is not None and len(dt_array) <= 2 * hamm_size:
        raise ValueError(f'deinterleave_compute:: hamm_size = {hamm_size} and len(dt) = {len(dt_array)} <= 2*hamm_size.')

    n_frames = len(dt_array) + 1

    if hamm_size is None:
        # Gymnastics to enforce parity
        hamm_size = min(12, max(2, (len(dt_array) + 3) // 4 * 2))

    assert hamm_size % 2 == 0

    # Too short!
    if len(dt_array) < 2: # No can do.
        return np.zeros(n_frames, np.int32), None

    if len(dt_array) <= 2 * hamm_size: # Trivial deint mode...
        mean_odd = np.mean(dt_array[::2]) # even indices of dt but odd frames of the file!
        mean_even = np.mean(dt_array[1::2])
        result = np.arange(n_frames, dtype=np.int32) % 2 * 2 - 1
        print(f'Odd/Even: {mean_odd:.1f} {mean_even:.1f} [{n_frames} frames]')
        if mean_odd > mean_even: # first dt of the file is a slow one. Image 0 is A == -1
            return result, None
        else: # first dt of the file is a fast one. Image 0 is B == 1
            return -result, None


    # Actual hamming deinterleaving.

    hamming = np.hamming(hamm_size)

    # L1 normalized, sign-alternating hamming window
    nyquist_hamming = hamming * ((np.arange(hamm_size) % 2)*2 - 1) / np.sum(hamming)

    # Clip crazy outliers
    if clip_vals is None:
        med: float = np.median(dt_array)
        clip_vals = (med - 4 * dt_jitter_us, med + 4 * dt_jitter_us)
    clipped = np.clip(dt_array, *clip_vals)

    convolved = np.convolve(nyquist_hamming, clipped, 'valid') / dt_jitter_us
    # len(convolved) == N_FRAMES + HAMM_N = len(dt_array) + 1 + HAMM_N
    
    # We then apply a minimum filter - if there's a wonky glitch we want to blacklist some neighboring frames.
    from scipy.ndimage import minimum_filter1d
    #convolved = minimum_filter1d(np.abs(convolved), hamm_size // 4) * np.sign(convolved)

    flc_state = np.zeros(n_frames, np.int32)

    # Write good states - mind the convolution reducing the array size
    flc_state[hamm_size // 2:-hamm_size//2][convolved > corr_trust_margin] = 1
    flc_state[hamm_size // 2:-hamm_size//2][convolved < -corr_trust_margin] = -1


    # Propagate trust to buffer edges - careful to handle signs if HAM_LENGTH // 2 is odd.
    flipper = (1, -1)[(hamm_size // 2) % 2] # (even = noflip, odd=flip)
    if np.all(flc_state[hamm_size // 2:hamm_size] != 0):
        flc_state[:hamm_size // 2] = flc_state[hamm_size // 2:hamm_size] * flipper
    if np.all(flc_state[-hamm_size:-hamm_size // 2] != 0):
        flc_state[-hamm_size // 2:] = flc_state[-hamm_size:-hamm_size // 2] * flipper

    # Return point without enforce_pairing
    if enforce_pairing and np.any(flc_state != 0):
        # Find the first certain frame.
        first = 0
        while flc_state[first] == 0:
            first += 1
        if (n_frames - first) % 2 == 1: # Odd number of frames
            flc_state[-1] = 0
        for ii in range(first, n_frames-1, 2):
            if flc_state[ii] + flc_state[ii+1] != 0: # So, only [0, 0] and [+-1, -+1] pass this.
                flc_state[ii] = 0
                flc_state[ii+1] = 0

    return flc_state, convolved


'''
from scxkw.daemons.gen2_archiving import archive_monitor_deinterleave_or_passthrough, PDIDeintJobManager
deinter = PDIDeintJobManager()
archive_monitor_deinterleave_or_passthrough(folder_root='/mnt/tier0/', job_manager=deinter)
'''

'''
from scxkw.tools import file_tools
from scxkw.tools import pdi_deinterleave as pdi
from scxkw.tools.framelist_file_obj import FrameListFitsFileObj
DATE = '20230711'
all_fobjs = file_tools.make_fileobjs_from_globs([f'/mnt/tier1/ARCHIVED_DATA/{DATE}/vsolo1/*.fitsframes',
                                                 f'/mnt/tier1/ARCHIVED_DATA/{DATE}/vsolo2/*.fitsframes',
                                                 f'/mnt/tier1/ARCHIVED_DATA/{DATE}/vsync/*.fitsframes'], [],
                                                 type_to_use=FrameListFitsFileObj)
deinterleaver = pdi.SyncPDIDeintManager()
for fobj in all_fobjs:
    ret = deinterleaver.run_pdi_deint_job(fobj, 'vgen2')
    if ret == pdi.PDIJobCodeEnum.NOTHING:
        fobj.move_file_to_streamname('vgen2')
'''
    
