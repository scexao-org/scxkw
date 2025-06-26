import dataclasses
from datetime import datetime, timezone
import pandas
import pathlib
import re


ARCHIVE_DIR = pathlib.Path("/mnt/fuuu/ARCHIVED_DATA")
ARCHIVE_LOG_PATH = ARCHIVE_DIR / "ARCHIVE_LOG.csv"


class WrongComputerException(BaseException):
    """Use to safegaurd scripts to run on specific computers"""


@dataclasses.dataclass
class VAMPIRESArchiveTelemetry:
    scexao5_path: str
    utc_date: str

	# monitoring the syncdeint processing
    processed: bool
    processed_timestamp: str # date processed

	# monitoring the sc5 -> sc6 transfer
    transferred_to_scexao6: bool
    transferred_timestamp: str

    @classmethod
    def initialize(__cls__, path: str):

        proto = __cls__(
            scexao5_path = path,
            utc_date = _date_from_path(pathlib.Path(path)).strftime("%Y-%m-%d"),
            processed=False,
            processed_timestamp="",
            transferred_to_scexao6=False,
            transferred_timestamp=""
        )
        return proto


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

def check_for_new_folders(path: pathlib.Path=ARCHIVE_DIR):
    # step 1, get all candidate folders
    folders = filter(lambda p: p.is_dir(), path.iterdir())
    pattern = re.compile(r"\d{8}")
    folder_candidates = [str(folder.absolute()) for folder in folders if pattern.match(folder.name)]

    # step 2, cross match with database and determine which are new
    table = load_table()
    if table is not None:
        existing_paths = set(table["scexao5_path"])
    else:
        existing_paths = set()

    new_paths = set(folder_candidates) - existing_paths
    if len(new_paths) == 0:
        msg = "No new archive folders detected"
        print(msg)
        return False

    new_telemetry = list(map(VAMPIRESArchiveTelemetry.initialize, sorted(new_paths)))

    dataframe = pandas.DataFrame(map(dataclasses.asdict, new_telemetry))

    if table is None:
        dataframe.to_csv(ARCHIVE_LOG_PATH, mode="w", index=False)
    else:
        dataframe.to_csv(ARCHIVE_LOG_PATH, mode="a", index=False, header=False)

    print("New folder(s) detected and added to archive log database")
    print(dataframe["scexao5_path"])
    return True
