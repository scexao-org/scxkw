from datetime import datetime, timezone
from dateutil.relativedelta import relativedelta
import pandas
import pathlib
import re
from astropy.io import fits
import tqdm
import subprocess
import paramiko
import logging
from logging.handlers import TimedRotatingFileHandler
import os

ARCHIVE_DIR = pathlib.Path("/mnt/fuuu/ARCHIVED_DATA")
ARCHIVE_DB_PATH = ARCHIVE_DIR / "ARCHIVE_LOG.csv"
DELETION_DB_PATH = ARCHIVE_DIR / "MARKED_FOR_DELETION.csv"
ARCHIVE_LOG_DIR = ARCHIVE_DIR / "LOGS"

PROCESS_DIR = pathlib.Path("/mnt/fuuu/")
PROCESS_DB_PATH = PROCESS_DIR / "PROCESS_LOG.csv"
PROCESS_DELETION_DB_PATH = PROCESS_DIR / "MARKED_FOR_DELETION.csv"
PROCESS_LOG_DIR = PROCESS_DIR / "LOGS"


def setup_logger(name: str, archive: bool=True):
    if archive:
        logfile = ARCHIVE_LOG_DIR / "archive-cronjobs.log"
    else:
        logfile = PROCESS_LOG_DIR / "process-cronjobs.log"

    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    logger.propagate = False  # prevents duplicate logs if root logger configured

    # Avoid adding multiple handlers if function is called twice
    if not logger.handlers:
        handler = TimedRotatingFileHandler(
            logfile,
            when="midnight",
            interval=1,
            utc=True,
        )

        # Format timestamps in UTC
        formatter = logging.Formatter(
            fmt="%(asctime)s %(name)s:%(lineno)d [%(levelname)s] %(message)s",
            datefmt="%Y-%m-%dT%H:%M:%SZ"
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)

        handler2 = logging.StreamHandler()
        handler2.setFormatter(formatter)
        logger.addHandler(handler2)
    return logger

class WrongComputerException(BaseException):
    """Use to safegaurd scripts to run on specific computers"""



def empty_entry(path: pathlib.Path):
    # sanitize inputs
    path_str = str(path.absolute())
    utc_date = _date_from_path(path).strftime("%Y-%m-%d")

    entry = {
        "scexao5_path" : path_str,
        "utc_date" :  utc_date,
        "processed" : False,
        "processed_timestamp" : None,
        "transferred_to_scexao6" : False,
        "transferred_timestamp" : None,
        "safe_on_scexao6" : False,
        "safe_timestamp" : None
    }
    return entry


def _date_from_path(path: pathlib.Path) -> datetime:
    # extract final name
    assert path.is_dir(), "Expected a directory!"
    date_str = path.name
    date_obj = datetime.strptime(date_str, "%Y%m%d").replace(tzinfo=timezone.utc)
    return date_obj

def load_table(archive: bool=True) -> pandas.DataFrame | None:
    path = ARCHIVE_DB_PATH if archive else PROCESS_DB_PATH
    logger = setup_logger(__file__, archive=archive)
    if path.exists():
        return pandas.read_csv(path)
    else:
        logger.warning("WARNING: No database CSV found!")
        return None


def load_deletion_table(archive: bool=True) -> pandas.DataFrame | None:
    path = DELETION_DB_PATH if archive else PROCESS_DELETION_DB_PATH
    logger = setup_logger(__file__, archive=archive)
    if path.exists():
        return pandas.read_csv(path)
    else:
        logger.warning("WARNING: No deletion CSV found!")
        return None


def create_deletion_entry(path: pathlib.Path):
    # sanitize inputs
    path_str = str(path.absolute())
    datetime_now = datetime.now(timezone.utc)
    datetime_tomorrow = datetime_now + relativedelta(days=1)

    entry = {
        "scexao5_path" : path_str,
        "delete_after" :  datetime_tomorrow.isoformat(),
    }
    return entry


def check_for_new_folders(directory: pathlib.Path=ARCHIVE_DIR):
    logger = setup_logger(__file__, archive=True)
    pattern = re.compile(r"\d{8}")
    table = load_table()
    if table is not None:
        existing_paths = list(map(pathlib.Path, table["scexao5_path"]))
    else:
        existing_paths = []

    new_telemetry = []
    for path in directory.iterdir():
        # filter 1: it's a directroy
        if not path.is_dir():
            continue
        # filter 2: it's 8 digits, like 20200506
        if not pattern.match(path.name):
            continue

        # filter 3: it's not in the table already
        if path.absolute() in existing_paths:
            continue
        telemetry = empty_entry(path)
        new_telemetry.append(telemetry)

    # step 2, cross match with database and determine which are new

    if len(new_telemetry) == 0:
        msg = "No new archive folders detected"
        logger.info(msg)
        return False

    dataframe = pandas.DataFrame(new_telemetry)

    if table is None:
        dataframe.to_csv(ARCHIVE_DB_PATH, mode="w", index=False)
    else:
        dataframe.to_csv(ARCHIVE_DB_PATH, mode="a", index=False, header=False)

    logger.info("New folder(s) detected and added to archive log database")
    logger.info(dataframe["scexao5_path"])
    return True

def get_checksums(filename):
    logger = setup_logger(__file__, archive=True)
    try:
        checksums = []
        with fits.open(filename) as hdul:
            for hdu in hdul:
                if "CHECKSUM" not in hdu.header:
                    logger.warning(f"Missing CHECKSUM for {filename}: data may be corrupted")
                    return None
                checksums.append(hdu.header["CHECKSUM"])
        return checksums
    except fits.VerifyError:
        return None

def crosscheck_scexao6_sdata(directory: pathlib.Path):
    # get fits files in that directory
    logger = setup_logger(__file__, archive=True)
    fits_files = sorted((directory / "vgen2").glob("[vV]*.fits.fz"))
    if len(fits_files) == 0:
        msg = f"Did not find any input FITS files in directory {directory}"
        logger.error(msg)
        raise ValueError(msg)
    pbar = tqdm.tqdm(fits_files, desc="Parsing local checksums")
    mapping = {filename.name: get_checksums(filename) for filename in pbar}
    ## now go find the same folder on scexao6 in sdata
    # Run the command and capture output
    sc6_folder = pathlib.Path(f"/mnt/sdata/{directory.name}/ARCHIVED/vgen2")
    # sc6_files = [str(sc6_folder / fname) for fname in mapping.keys()]

    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(
        hostname="scexao6",
        username="scexao",
    )
    cmd = f"/home/scexao/miniforge3/bin/python /home/scexao/src/scxkw/scripts/fitschecksums {sc6_folder}/V*.fits.fz"
    logger.info("Checking remote checksums (might take a while...)")
    stdin, stdout, stderr = client.exec_command(cmd)
    pbar = tqdm.tqdm(total=len(mapping), desc="Parsing remote checksums", leave=False)
    bad_files = []
    while line := stdout.readline().strip():
        pbar.update()
        tokens = line.split(",")
        filename = tokens[0]
        if filename not in mapping:
            msg = f"File found on scexao6 that isn't on scexao5: {filename}"
            pbar.write(msg)
            continue
        expected = mapping.pop(filename)
        if expected != tokens[1:]:
            msg = f"Checksums did not match for {filename} between scexao5 and scexao6\nscexao5: {', '.join(expected)}\nscexao6: {', '.join(tokens[1:])}"
            pbar.write(msg)
            bad_files.append(filename)
            continue
        
    if len(mapping) > 0:
        msg = "Files found on scexao5 that weren't found on scexao6"
        logger.warning(msg)
        logger.info("\n".join(mapping.keys()))
        bad_files.extend(mapping.keys())

    if len(bad_files) > 0:
        msg = f"{len(bad_files)}/{len(fits_files)} bad files"
        logger.warning(msg)
        return bad_files
    
    logger.info("All files verified")
    table = load_table()
    if table is None:
        return
    timestamp = datetime.now(timezone.utc).now().strftime("%Y-%m-%dT%H:%M:%S")
    row = table["scexao5_path"] == str(directory.absolute())
    table.loc[row, ["safe_on_scexao6", "safe_timestamp"]] = True, timestamp
    table.to_csv(ARCHIVE_DB_PATH, index=False)
    logger.info("Archive log updated!")

