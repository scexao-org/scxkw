from datetime import datetime, timezone
import pandas
import pathlib
import re


ARCHIVE_DIR = pathlib.Path("/mnt/fuuu/ARCHIVED_DATA")
ARCHIVE_LOG_PATH = ARCHIVE_DIR / "ARCHIVE_LOG.csv"


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

def load_table() -> pandas.DataFrame | None:
    if ARCHIVE_LOG_PATH.exists():
        return pandas.read_csv(ARCHIVE_LOG_PATH)
    else:
        print("WARNING: No database CSV found!")
        return None

def check_for_new_folders(directory: pathlib.Path=ARCHIVE_DIR):
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
        print(msg)
        return False

    dataframe = pandas.DataFrame(new_telemetry)

    if table is None:
        dataframe.to_csv(ARCHIVE_LOG_PATH, mode="w", index=False)
    else:
        dataframe.to_csv(ARCHIVE_LOG_PATH, mode="a", index=False, header=False)

    print("New folder(s) detected and added to archive log database")
    print(dataframe["scexao5_path"])
    return True
