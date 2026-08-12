from pathlib import Path
import pandas as pd
from astropy.io import fits
import re
import tqdm.auto as tqdm


def get_fits_info(filenames):
    """
    Takes a list of Path filenames and returns a DataFrame with columns:
    'filename', 'ut', 'frameid'
    - Uses ext=0 if file ends with .fits
    - Uses ext=1 if file ends with .fits.fz
    """
    data = []
    pbar = tqdm.tqdm(filenames, desc="Getting FRAMEIDs")
    for f in pbar:
        if f.suffix == ".fits":
            ext = 0
        elif f.suffix == ".fz":
            ext = 1
        else:
            pbar.write(f"Skipping {f}: unknown file type")
            continue

        header = fits.getheader(f, ext=ext)
        date = header.get("DATE-OBS", None)
        ut = header.get("UT", None)
        frameid = header.get("FRAMEID", None)
        if date is None or ut is None or frameid is None:
            pbar.write(f"Warning: {f} missing UT or FRAMEID header")
            continue
        isot = f"{date}T{ut}"
        data.append({"filename": f.resolve(), "ut": isot, "frameid": frameid})

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
    frameids = df_copy["frameid"].values

    # Sort by date, and for synchronized data let the original frameid be the tiebreaker
    df_copy = df_copy.sort_values(["ut", "frameid"]).reset_index(drop=True)

    # Assign new numbers sequentially starting from min_num
    df_copy["new_frameid"] = [frameids[idx] for idx in range(len(df_copy))]

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
