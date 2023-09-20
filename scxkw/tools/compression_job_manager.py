import logging

logg = logging.getLogger(__name__)

import typing as typ

from pathlib import Path
import subprocess as sproc

from enum import IntEnum

from .fits_file_obj import FitsFileObj
from . import file_tools

class FPackJobCodeEnum(IntEnum):
    ALREADY_RUNNING = -3
    NOFILE = -2
    TOOMANY = -1
    STARTED = 0

class FpackJobManager:
    MAX_CONCURRENT_JOBS = 50 # FIXME
    FPACK_OPTIONS_SCX = '-h -s 0 -q 20' # Maintains the original .fits file!
    FPACK_OPTIONS_VMP = '-r -D -Y' # Removes the original .fits file

    def __init__(self) -> None:
        self.pending_jobs: typ.Dict[str, sproc.Popen] = {}

        # Now check for active fpack jobs that this manager didn't launch.
        running_fpacks_str = sproc.run(
            'ps -eo args | egrep ^fpack', shell=True,
            capture_output=True).stdout.decode('utf8').strip()
        if running_fpacks_str == '':
            running_fpacks = []
        else:
            running_fpacks = running_fpacks_str.split('\n')

        if len(running_fpacks) > 0:
            logg.error('FpackJobManager::__init__ - Running fpack jobs:')
            logg.error(str(running_fpacks))
            raise AssertionError(
                'There are running fpack jobs on the system. '
                'It is bad juju to instantiate a FPackJobManager now.')

    def run_fpack_compression_job(self,
                                  file_obj: FitsFileObj,
                                  forced_fpack_options: typ.Optional[str] = None) -> FPackJobCodeEnum:
        if not file_obj.check_existence_on_disk():
            logg.error(f'Fpack job manager: file {file_obj} does not exist.')
            return FPackJobCodeEnum.NOFILE
        if len(self.pending_jobs) == self.MAX_CONCURRENT_JOBS:
            logg.error(f'Fpack job manager: max allowed ({self.MAX_CONCURRENT_JOBS}) fpack jobs already running at the same time.')
            return FPackJobCodeEnum.TOOMANY
        if str(file_obj.full_filepath) in self.pending_jobs:
            return FPackJobCodeEnum.ALREADY_RUNNING

        if forced_fpack_options is not None:
            cmdline = f'fpack {forced_fpack_options} -v {str(file_obj.full_filepath)}'
        else:
            assert file_obj.archive_key is not None
            if file_obj.archive_key.startswith('VMPA'):
                cmdline = f'fpack {self.FPACK_OPTIONS_VMP} -v {str(file_obj.full_filepath)}'
            else:
                assert file_obj.archive_key.startswith('SCXB')
                cmdline = f'fpack {self.FPACK_OPTIONS_SCX} -v {str(file_obj.full_filepath)}'

        proc = sproc.Popen(cmdline.split(' '),
                           stdout=sproc.PIPE,
                           stderr=sproc.PIPE)

        self.pending_jobs[str(file_obj.full_filepath)] = proc

        return FPackJobCodeEnum.STARTED

    def refresh_running_jobs(self) -> None:
        # job.poll() is None if process is still running.
        self.pending_jobs = {
            filename: self.pending_jobs[filename]
            for filename in self.pending_jobs
            if self.pending_jobs[filename].poll() is None
        }


def compress_all_fits_in_given_folder_lossless(folder: typ.Union[str,Path], *,
                                               dry_run: bool = True):
    '''
    Find all fits files

    For all files, open them
    '''
    FPACK_OPTIONS = '-D -r' # Destroy original, use lossless Rice.
    import os
    import glob
    from astropy.io import fits

    path = Path(folder)

    assert path.is_dir()

    files = [os.path.abspath(f) for f in glob.glob(f'{folder}/**/*.fits') + glob.glob(f'{folder}/*.fits')]
    files_valid_and_integer: typ.List[str] = []

    for fname in files:
        try:
            if fits.getheader(fname)['BITPIX'] in (8, 16, 32, 64):
                files_valid_and_integer += [fname]
        except OSError:
            pass

    file_list = '/tmp/files_fpack_comp_batch.txt'
    with open(file_list, 'w') as f:
        for fname in files_valid_and_integer:
            f.write(fname + '\n')

    if dry_run:
        return
    
    sproc.run('cat %s | xargs -P20 -I {} fpack %s -v {}' % (file_list, FPACK_OPTIONS),
              shell = True)









