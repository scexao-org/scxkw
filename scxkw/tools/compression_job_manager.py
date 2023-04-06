from typing import Dict

import os
import subprocess as sproc

from enum import IntEnum


class FPackJobCodeEnum(IntEnum):
    ALREADY_RUNNING = -3
    NOFILE = -2
    TOOMANY = -1
    STARTED = 0


class FpackJobManager:
    MAX_CONCURRENT_JOBS = 15
    FPACK_OPTIONS = '-h -s 0 -q 20'

    def __init__(self) -> None:
        self.pending_jobs: Dict[str, sproc.Popen] = {}

        # Now check for active fpack jobs that this manager didn't launch.
        running_fpacks = sproc.run(
            'ps -eo args | grep fpack', shell=True,
            capture_output=True).stdout.decode('utf8').rstrip().split('\n')
        if len(running_fpacks) > 0:
            raise AssertionError(
                'There are running fpack jobs on the system. '
                'It is bad juju to instantiate a FPackJobManager now.')

    def run_fpack_compression_job(self,
                                  file_fullname: str) -> FPackJobCodeEnum:
        if not os.path.isfile(file_fullname):
            print(f'Fpack job manager: file {file_fullname} does not exist.')
            return FPackJobCodeEnum.NOFILE
        if len(self.pending_jobs) == self.MAX_CONCURRENT_JOBS:
            print(
                f'Fpack job manager: max allowed fpack jobs already running.')
            return FPackJobCodeEnum.TOOMANY
        if file_fullname in self.pending_jobs:
            return FPackJobCodeEnum.ALREADY_RUNNING

        cmdline = f'fpack {self.FPACK_OPTIONS} -v {file_fullname}'

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
