from collections import OrderedDict
from astropy.io import fits
from pathlib import Path
import multiprocessing as mp
import pandas as pd
import tqdm.auto as tqdm
from typing import Iterable

def load_fits_header(filename, ext=0, **kwargs) -> fits.Header:
    """Load FITS header automatically choosing extension for compressed files"""
    path = Path(filename)
    if ".fits.fz" in path.name:
        ext = 1
    return fits.getheader(path, ext=ext, **kwargs)    

def dict_from_header_file(filename, **kwargs) -> OrderedDict:
    """
    Parse a FITS header from a file and extract the keys and values as an ordered dictionary. 
    Multi-line keys like ``COMMENTS`` and ``HISTORY`` will be combined with commas. The resolved 
    path will be inserted with the ``path`` key.


    Parameters
    ----------
    filename : str
        FITS file to parse
    **kwargs
        All keyword arguments will be passed to ``load_fits_header``

    Returns
    -------
    OrderedDict
    """
    path = Path(filename)
    summary = OrderedDict()
    # add path to row before the FITS header keys- make sure it's a str
    summary["path"] = str(path.resolve().absolute())
    header = load_fits_header(filename, **kwargs)
    summary.update(dict_from_header(header))
    return summary

def dict_from_header(header: fits.Header, excluded=("COMMENT", "HISTORY")) -> OrderedDict:
    """
    Parse a FITS header and extract the keys and values as an ordered dictionary. Multi-line keys 
    like ``COMMENTS`` and ``HISTORY`` will be combined with commas. The resolved path will be 
    inserted with the ``path`` key.


    Parameters
    ----------
    header : Header
        FITS header to parse


    Returns
    -------
    OrderedDict
    """
    summary = OrderedDict()
    for k, v in header.items():
        # check if we have a good key and good value
        if k == "" or k in excluded or v is None or isinstance(v, fits.card.Undefined):
            continue
        
        summary[k] = v
    return summary

def header_table(
    filenames, num_proc: int = min(10, mp.cpu_count()), quiet: bool = False, **kwargs
) -> pd.DataFrame:
    """
    Generate a pandas dataframe from the FITS headers parsed from the given files.


    Parameters
    ----------
    filenames : list[pathlike]
    num_proc : int, optional
        Number of processes to use in multiprocessing, by default mp.cpu_count()
    quiet : bool, optional
        Silence the progress bar, by default False


    Returns
    -------
    pandas.DataFrame
    """
    with mp.Pool(num_proc) as pool:
        jobs = [pool.apply_async(dict_from_header_file, args=(f,), kwds=kwargs) for f in filenames]
        iter = jobs if quiet else tqdm.tqdm(jobs, desc="Parsing FITS headers")
        rows = [job.get() for job in iter]

    return pd.DataFrame(rows)

def create_db(
    subdirs: Iterable[Path], outdir, **kwargs
):
    base_outdir = Path(outdir)
    db_outpath = base_outdir / f"{base_outdir.name}_headers.db"
    db_uri = f"sqlite:///{db_outpath.absolute()}"
    # for each data subdir create a table, save it as its own CSV, and add to database
    for subdir in subdirs:
        df = header_table(subdir.glob("*.fits*"), **kwargs)
        csv_outpath = base_outdir / f"{base_outdir.name}_{subdir.name}.csv"
        df.to_csv(csv_outpath, index=False)
        df.to_sql(name=subdir.name, con=db_uri, if_exists="replace", index=False)
