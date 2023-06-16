from typing import List, Any, Tuple, Optional as Op

import os

import time
import subprocess as sproc

from astropy.io import fits

import numpy as np

from .logshim_txt_parser import LogshimTxtParser


def deinterleave_filechecker(file_list: List[str]) -> List[bool]:
    '''
        return False for files that need NOT to be deinterleaved
        return True for files that have to be deinterleaved
    '''

    statuses: List[bool] = [False] * len(file_list)

    for kk, file in enumerate(file_list):
        header = fits.getheader(file)

        detector = header['DETECTOR']

        valid_fpdi_file = (detector in ['CRED1 - APAPANE', 'CRED2 - PALILA']
                           and header['X_IFLCST'] == 'ON' and header['EXTTRIG']
                           and header['X_IFLCST'] == 'NA')
        valid_vpdi_file = (detector in ['VCAM1 - OrcaQ', 'VCAM2 - OrcaQ']
                           and header['U_FLCEN'] == 'ON'
                           and header['U_FLC'] == 'NA')

        if valid_fpdi_file or valid_vpdi_file:
            statuses[kk] = True

    return statuses


def deinterleave_start_job_async(file_name: str,
                                 source_tree: str, dest_tree: str,
                                 keep_original: bool = True,) -> sproc.Popen:
    '''
    source_tree and dest_tree are passed so that the subprocess
    knows where to move the resulting files.
    '''
    cmd = f'scxkw-pdideinterleave {file_name}' + ('', ' --keep')[keep_original] +\
            f'--source={source_tree} --dest={dest_tree}'

    return sproc.Popen(cmd.split(' '))


def deinterleave_file(file_name: str, *, ir_true_vis_false: bool = True, flc_jitter_us_hint: Op[int] = True,
                      source_tree: str = '/', dest_tree: str = '/'):
    '''
    This file will carry the header on to the split files.
    BUT it will not bother checking the header is sane.
    This should be done upstream and the relevant stuff is to be passed in the parameter.
    '''
    fullpath = os.path.abspath(file_name)

    header: fits.Header = fits.getheader(fullpath)
    data: np.ndarray = fits.getdata(fullpath) # type: ignore

    key = ('U_FLCJT', 'X_IFLCJT')[ir_true_vis_false]
    if flc_jitter_us_hint is None:
        flc_jitter_us: int = header[key] # type: ignore
    else:
        flc_jitter_us = flc_jitter_us_hint

    assert fullpath.endswith('.fits')
    txtparser = LogshimTxtParser(fullpath[:-5] + '.txt')


    subfiles = deinterleave_data(data, flc_jitter_us, txtparser)

    path_to_folder = '/'.join(fullpath.split('/')[:-1])

    flc_st_type = ['%-16.16s' % s for s in ('ACTIVE', 'RELAXED', 'DUBIOUS')]
    key_flc_state = ('U_FLC', 'X_IFLCAB')[ir_true_vis_false]
    for kk in range(3):
        subdata, subparser = subfiles[kk]
        kw_value = flc_st_type[kk]

        if subparser is None:
            continue
        subparser.name.replace(source_tree, dest_tree, 1)
        subparser.write_to_disk()
        
        name_disambiguated = path_to_folder + '/' + str(time.time()) + '.fits'
        
        name_fits_final = subparser.name[:-4] + '.fits'
        name_fits_final.replace(source_tree, dest_tree, 1)

        header[key_flc_state] = kw_value
        fits.writeto(name_disambiguated, subdata, header)
        import shutil
        shutil.move(name_disambiguated, name_fits_final)


    


DataOpTxt = Tuple[np.ndarray, Op[LogshimTxtParser]]
def deinterleave_data(data: np.ndarray, dt_jitter_us: int, txt_parser: LogshimTxtParser) -> List[DataOpTxt]:
    '''
        Deinterleave a data cube based on a logshim txt parser object.
    '''

    flc_state, _ = deinterleave_compute(txt_parser.fgrab_dt_us, dt_jitter_us, True)

    data_a = data[flc_state == -1]
    data_b = data[flc_state == 1]
    data_garbage = data[flc_state == 0]

    n_a = len(data_a)
    n_b = len(data_b)
    n_g = len(data_garbage)

    if n_a > 0:
        parser_a = txt_parser.sub_parser_by_selection('ACTIVE', flc_state == -1)
    else:
        parser_a = None
    if n_b > 0:
        parser_b = txt_parser.sub_parser_by_selection('RELAXED', flc_state == 1)
    else:
        parser_b = None
    if n_g > 0:
        parser_garbage = txt_parser.sub_parser_by_selection('DUBIOUS', flc_state == 0)
    else:
        parser_garbage = None

    return [(data_a, parser_a), (data_b, parser_b), (data_garbage, parser_garbage)]
    



def deinterleave_compute(dt_array: np.ndarray,
                         dt_jitter_us: int,
                         enforce_pairing: bool, *,
                         hamm_size: int = 30,
                         corr_trust_margin: float = 0.75) -> Tuple[np.ndarray, np.ndarray]:
    '''
        Validate the FLC state from an array of timing deltas
    '''

    n_frames = len(dt_array) + 1

    assert len(dt_array) > 2 * hamm_size # Arbitrary but we need a little bit of length.

    hamming = np.hamming(hamm_size)
    # L1 normalized, sign-alternating hamming window
    nyquist_hamming = hamming * ((np.arange(hamm_size) % 2)*2 - 1) / np.sum(hamming)

    convolved = np.convolve(nyquist_hamming, dt_array, 'valid') / dt_jitter_us
    # len(convolved) == N_FRAMES + HAMM_N = len(dt_array) + 1 + HAMM_N
    
    # We then apply a minimum filter - if there's a wonky glitch we want to blacklist all neighboring frames.
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

    # Return point without enfore_pairing
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




        
    
