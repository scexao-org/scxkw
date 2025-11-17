from datetime import datetime, timezone
from dateutil.relativedelta import relativedelta
import pandas
import pathlib
import re
from astropy.io import fits
import logging
from logging.handlers import TimedRotatingFileHandler
import os
import libtmux

ARCHIVE_DIR = pathlib.Path("/mnt/fuuu/ARCHIVED_DATA")
ARCHIVE_DB_PATH = ARCHIVE_DIR / "ARCHIVE_LOG.csv"
DELETION_DB_PATH = ARCHIVE_DIR / "MARKED_FOR_DELETION.csv"
ARCHIVE_LOG_DIR = ARCHIVE_DIR / "LOGS"

PROCESS_DIR = pathlib.Path("/mnt/fuuu/")
PROCESS_DB_PATH = PROCESS_DIR / "PROCESS_LOG.csv"
PROCESS_DELETION_DB_PATH = PROCESS_DIR / "MARKED_FOR_DELETION.csv"
PROCESS_LOG_DIR = PROCESS_DIR / "LOGS"


def _setup_logger(name: str, logfile):
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


def _get_checksums(filename, logger):
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


def month_has_passed(ymd_timestamp: str) -> bool:
    """Return True if at least one full month has passed since the given ISO timestamp."""
    past = datetime.strptime(ymd_timestamp, "%Y-%m-%d").astimezone(timezone.utc)
    now = datetime.now(past.tzinfo)  # preserve timezone if present
    one_month_later = past + relativedelta(months=1)
    return now >= one_month_later


def get_or_create_tmux_window(session_name: str):
    server = libtmux.Server()

    # Find or create the session
    session = server.find_where({"session_name": session_name})
    if session is None:
        session = server.new_session(session_name=session_name, attach=False, kill_session=True)

    # If no window name is given, return the attached (first) window
    return session.attached_window
