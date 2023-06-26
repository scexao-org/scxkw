from __future__ import annotations

import logging
logg = logging.getLogger(__name__)

import typing as typ

from pathlib import Path

import numpy as np

class LogshimTxtParser:

    def __init__(self, filename_txt: typ.Union[str, Path]) -> None:
        '''
            Warning: check sub_parser_by_selection
                this class has an alternate constructor.
        '''
        path = Path(filename_txt)
        if not (path.is_file() and
            path.is_absolute()):
            message = f"LogshimTxtParser::__init__: does not exist / not an absolute path - {path}"
            logg.critical(message)
            raise AssertionError(message)

        self.name: str = str(path)

        with open(filename_txt, 'r') as f:
            raw_lines = f.readlines()
        
        self.header = [l.strip() for l in raw_lines if l[0] == '#']
        self.lines = [l.strip() for l in raw_lines if not l[0] == '#']

        self._init_arrays_from_lines()

    def clone_instance(self) -> LogshimTxtParser:
        '''
            This is really an alternative constructor.
            We create a bare instance without calling __init__()
        '''
        cls = type(self) # Important if this gets subclassed
        other = cls.__new__(cls)

        assert self.name.endswith('.txt')
        other.name = self.name

        other.header = self.header.copy()
        other.lines = self.lines.copy()

        other._init_arrays_from_lines()

        return other

    def sub_parser_by_selection(self, subname:str, selection: np.ndarray) -> LogshimTxtParser:
        '''
            This is really an alternative constructor.
            We create a bare instance without calling __init__()
        '''
        cls = type(self) # Important if this gets subclassed
        other = cls.__new__(cls)

        assert self.name.endswith('.txt')
        other.name = self.name[:-4] + '.' + subname + '.txt'

        other.header = self.header
        other.lines = [l for k, l in enumerate(self.lines) if selection[k]]

        other._init_arrays_from_lines()

        return other

    def _regenerate_lines_from_arrays(self):
        n_frames = len(self.fgrab_t_us)

        lines: typ.List[str] = []

        for ii in range(n_frames):
            lines += ['%10ld  %10lu  %15.9lf   %20.9lf  %17.6lf   %10ld   %10ld' %
                      (ii, self.cnt0[ii], self.logshim_t_us[ii] - self.logshim_t_us[0], self.logshim_t_us[ii],
                       self.fgrab_t_us[ii], self.cnt0[ii], self.cnt1[ii])]
            
        self.lines = lines


    def _init_arrays_from_lines(self):

        if len(self.lines) == 0: # Empty file case.
            values = np.zeros((0,6), np.float64)
        else:
            values = np.asarray([[float(v) for v in l.split()] for l in self.lines])

        self.logshim_t_us = values[:, 3] * 1e6
        self.fgrab_t_us = values[:, 4] * 1e6

        self.logshim_dt_us = self.logshim_t_us[1:] - self.logshim_t_us[:-1]
        self.fgrab_dt_us = self.fgrab_t_us[1:] - self.fgrab_t_us[:-1]

        self.cnt0 = values[:, 5]
        self.cnt1 = values[:, 6]


    def print_stats(self):
        print(f'Parsing {self.name}')

        logshim = self.logshim_dt_us
        fgrab = self.fgrab_dt_us

        print(f'Logshim jitter (sec):')
        print(f'{np.mean(logshim):.2f} - {np.std(logshim):.2f} ||'
            f' {logshim.min():.2f} - {np.percentile(logshim, 1):.2f} -'
            f' {np.percentile(logshim, 99):.2f} - {logshim.max():.2f}'
            )
        print(f'Framegrab jitt (sec):')
        print(f'{np.mean(fgrab):.2f} - {np.std(fgrab):.2f} ||'
            f' {fgrab.min():.2f} - {np.percentile(fgrab, 1):.2f} -'
            f' {np.percentile(fgrab, 99):.2f} - {fgrab.max():.2f}'
            )

    def write_to_disk(self):

        self._regenerate_lines_from_arrays()
        
        with open(self.name, 'w') as file:
            for line in self.header:
                file.write(line + '\n')

            for kk, line in enumerate(self.lines):
                file.write(line + '\n')

            




