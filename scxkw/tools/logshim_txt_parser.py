from __future__ import annotations

import numpy as np

class LogshimTxtParser:
    def __init__(self, filename_txt: str) -> None:
        '''
            Warning: check sub_parser_by_selection
                this class has an alternate constructor.
        '''

        self.name = filename_txt

        with open(filename_txt, 'r') as f:
            raw_lines = f.readlines()
        
        self.header = [l.strip() for l in raw_lines if l[0] == '#']
        self.lines = [l.strip() for l in raw_lines if not l[0] == '#']

        self._init_arrays_from_lines()

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


    def _init_arrays_from_lines(self):

        values = np.asarray([[float(v) for v in l.split()] for l in self.lines])

        self.logshim_t_us = values[:, 3] * 1e6
        self.fgrab_t_us = values[:, 4] * 1e6

        self.logshim_dt_us = self.logshim_t_us[1:] - self.logshim_t_us[:-1]
        self.fgrab_dt_us = self.fgrab_t_us[1:] - self.fgrab_t_us[:-1]


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
        with open(self.name, 'w') as file:
            for line in self.header:
                file.write(line + '\n')

            for kk, line in enumerate(self.lines):
                file.write(line + '\n')

            




