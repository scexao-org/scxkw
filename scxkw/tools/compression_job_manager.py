from typing import Dict

import os
import subprocess as sproc

from enum import IntEnum

import logging as logg

class FPackJobCodeEnum(IntEnum):
    ALREADY_RUNNING = -3
    NOFILE = -2
    TOOMANY = -1
    STARTED = 0


class FpackJobManager:
    MAX_CONCURRENT_JOBS = 15
    FPACK_OPTIONS_SCX = '-h -s 0 -q 20'
    FPACK_OPTIONS_VMP = '-r'

    def __init__(self) -> None:
        self.pending_jobs: Dict[str, sproc.Popen] = {}

        # Now check for active fpack jobs that this manager didn't launch.
        running_fpacks_str = sproc.run(
            'ps -eo args | egrep ^fpack', shell=True,
            capture_output=True).stdout.decode('utf8').strip()
        if running_fpacks_str == '':
            running_fpacks = []
        else:
            running_fpacks = running_fpacks_str.split('\n')

        if len(running_fpacks) > 0:
            logg.error('Running fpack jobs:')
            logg.error(str(running_fpacks))
            raise AssertionError(
                'There are running fpack jobs on the system. '
                'It is bad juju to instantiate a FPackJobManager now.')

    def run_fpack_compression_job(self,
                                  file_fullname: str) -> FPackJobCodeEnum:
        if not os.path.isfile(file_fullname):
            logg.error(f'Fpack job manager: file {file_fullname} does not exist.')
            return FPackJobCodeEnum.NOFILE
        if len(self.pending_jobs) == self.MAX_CONCURRENT_JOBS:
            logg.error(f'Fpack job manager: max allowed ({self.MAX_CONCURRENT_JOBS}) fpack jobs already running at the same time.')
            return FPackJobCodeEnum.TOOMANY
        if file_fullname in self.pending_jobs:
            return FPackJobCodeEnum.ALREADY_RUNNING

        is_vampires = file_fullname.split('/')[-1].startswith('VMP')
        if is_vampires:
            cmdline = f'fpack {self.FPACK_OPTIONS_VMP} -v {file_fullname}'
        else:
            assert file_fullname.split('/')[-1].startswith('SCX')
            cmdline = f'fpack {self.FPACK_OPTIONS_SCX} -v {file_fullname}'

        proc = sproc.Popen(cmdline.split(' '),
                           stdout=sproc.PIPE,
                           stderr=sproc.PIPE)

        self.pending_jobs[file_fullname] = proc

        return FPackJobCodeEnum.STARTED

    def refresh_running_jobs(self) -> None:
        # job.poll() is None if process is still running.
        self.pending_jobs = {
            filename: self.pending_jobs[filename]
            for filename in self.pending_jobs
            if self.pending_jobs[filename].poll() is None
        }
