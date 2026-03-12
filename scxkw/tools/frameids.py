from pathlib import Path
import pandas as pd
from astropy.io import fits
import re


def get_fits_info(filenames):
    """
    Takes a list of Path filenames and returns a DataFrame with columns:
    'filename', 'ut', 'frameid'
    - Uses ext=0 if file ends with .fits
    - Uses ext=1 if file ends with .fits.fz
    """
    data = []
    for f in filenames:
        if f.suffix == ".fits":
            ext = 0
        elif f.suffix == ".fz":
            ext = 1
        else:
            print(f"Skipping {f}: unknown file type")
            continue

        with fits.open(f) as hdul:
            ut = hdul[ext].header.get("UT")
            frameid = hdul[ext].header.get("FRAMEID")
            if ut is None or frameid is None:
                print(f"Warning: {f} missing UT or FRAMEID header")
                continue
            data.append({"filename": f.resolve(), "ut": ut, "frameid": frameid})

    df = pd.DataFrame(data)
    return df


def extract_frame_number(frameid):
    """Extract numeric part from a FRAMEID string, e.g., VMPA00384759 -> 384759"""
    match = re.search(r"(\d+)", frameid)
    if match:
        return int(match.group(1))
    else:
        raise ValueError(f"Invalid FRAMEID format: {frameid}")


def assign_new_frameids(df):
    """
    Sorts DataFrame by numeric part of FRAMEID.
    Assigns new FRAMEIDs sequentially while preserving prefix and zero-padding.
    Returns a copy of the DataFrame with a new 'new_frameid' column.
    """
    df_copy = df.copy()
    df_copy["ut"] = pd.to_datetime(df_copy["ut"])
    df_copy["frame_num"] = df_copy["frameid"].apply(extract_frame_number)
    min_num = df_copy["frame_num"].min()

    # Sort by numeric part
    df_copy = df_copy.sort_values("frame_num").reset_index(drop=True)

    # Assign new numbers sequentially starting from min_num
    new_nums = range(min_num, min_num + len(df_copy))
    # Preserve prefix and zero-padding
    prefix = re.match(r"^\D+", df_copy["frameid"].iloc[0]).group(0)
    width = len(re.search(r"\d+", df_copy["frameid"].iloc[0]).group(0))
    df_copy["new_frameid"] = [f"{prefix}{num:0{width}d}" for num in new_nums]

    return df_copy


def rename_and_update_header(df):
    """
    For each row in the DataFrame:
    - Renames the file using the new FRAMEID
    - Updates the FITS header FRAMEID to the new value
    """
    for _, row in df.iterrows():
        path = Path(row["filename"])
        suf = ".fits.fz" if path.suffix == ".fz" else ".fits"
        new_name = path.with_name(f"{row['new_frameid']}{suf}")
        if path == new_name:
            continue
        ext = 0 if path.suffix == ".fits" else 1

        # Rename file
        path.rename(new_name)

        # Update header
        with fits.open(new_name, mode="update") as hdul:
            hdul[ext].header["FRAMEID"] = row["new_frameid"]
            hdul.flush()
