from __future__ import annotations
import typing as typ

import logging

logg = logging.getLogger(__name__)

import numpy as np
import subprocess as sproc
from astropy.io import fits
from enum import IntEnum

from .logshim_txt_parser import LogshimTxtParser

from .file_obj import FitsFileObj as FFO
OpT_FFO = typ.Optional[FFO]

class PDIJobCodeEnum(IntEnum):
    ALREADY_RUNNING = -3
    NOFILE = -2
    TOOMANY = -1
    STARTED = 0


class PDIDeintJobManager:
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

    def run_pdi_deint_job(self, file_obj: FFO, new_stream_name: str) -> PDIJobCodeEnum:
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

def deinterleave_filechecker(file_list: typ.List[FFO]) -> typ.List[bool]:
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


def deinterleave_file(file_obj: FFO, *,
                      ir_true_vis_false: bool = True,
                      flc_jitter_us_hint: typ.Optional[int] = None,
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
    ret_files: typ.Dict[str, FFO] = {}
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

    

    


DataOpTxt = typ.Tuple[np.ndarray, typ.Optional[LogshimTxtParser]]
def deinterleave_data(data: np.ndarray, dt_jitter_us: int, txt_parser: LogshimTxtParser) -> typ.List[DataOpTxt]:
    '''
        Deinterleave a data cube based on a logshim txt parser object.
    '''
    
    n_frames = data.shape[0] + 1
    default_hamming_size = 30

    if n_frames <= 2: # Can't deinterleave with only one or two frames
        flc_state = flc_state = np.zeros(n_frames, np.int32)
    elif n_frames <= 2 * default_hamming_size + 1:
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
    


def deinterleave_compute_small(dt_array: np.ndarray,
                               dt_jitter_us: int) -> np.ndarray:
    mean_odd_frames = np.mean(dt_array[::2])
    mean_even_frames = np.mean(dt_array[1::2])
    std_odd_frames = np.std(dt_array[::2])
    std_even_frames = np.std(dt_array[1::2])

    split = abs(mean_even_frames - mean_odd_frames) # Should be equal to 2*dt_jitter_us


    n_frames = len(dt_array) + 1
    flc_state = np.zeros(n_frames, np.int32)




def deinterleave_compute(dt_array: np.ndarray,
                         dt_jitter_us: int,
                         enforce_pairing: bool, *,
                         hamm_size: int = 30,
                         clip_vals: typ.Optional[typ.Tuple[float,float]] = None,
                         corr_trust_margin: float = 0.75) -> typ.Tuple[np.ndarray, np.ndarray]:
    '''
        Validate the FLC state from an array of timing deltas
    '''

    n_frames = len(dt_array) + 1

    if len(dt_array) <= 2 * hamm_size: # Arbitrary but we need a little bit of length.
        return np.zeros(n_frames, np.int32), None # all frames dubious.


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
    
    # We then apply a minimum filter - if there's a wonky glitch we want to blacktyp.List all neighboring frames.
    from scipy.ndimage import minimum_filter1d
    convolved = minimum_filter1d(np.abs(convolved), hamm_size // 2) * np.sign(convolved)

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

    if enforce_pairing:
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


        
    
