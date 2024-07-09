from __future__ import annotations

import typing as typ

import numpy as np

NULL_DATA = np.array([0, 1, 2, 3], dtype=np.float32)


class FormattedFloat(float):
    # https://subarutelescope.org/Observing/fits/howto/floatformat/

    def __new__(cls, value, formatstr=None):
        return super().__new__(cls, value)

    def __init__(self, value, formatstr=None):
        if formatstr is not None:
            # remove the leading % if present to be compatible with the f-string format
            self.formatstr = formatstr[1:] if formatstr[0] == "%" else formatstr

    def __str__(self):
        return f"{self.__float__():{self.formatstr}}"

class FormattedInt(int):
    # https://subarutelescope.org/Observing/fits/howto/floatformat/

    def __new__(cls, value, formatstr=None):
        return super().__new__(cls, value)

    def __init__(self, value, formatstr=None):
        if formatstr is not None:
            # remove the leading % if present to be compatible with the f-string format
            self.formatstr = formatstr[1:] if formatstr[0] == "%" else formatstr

    def __str__(self):
        return f"{self.__int__():{self.formatstr}}"

class NeverEqualFormattedInt(FormattedInt):
    '''
    When amending a FITS card e.g. as
    header[key] = value
    The card is modified ONLY in case of non-equality.

    e.g. if
        val: float = header[key]
    then
        header[key] = FormattedFloat(val, format)
    the reformatting will NOT happen because we have equality.
    '''

    def __eq__(self, other: float) -> bool:
        return False

    def __ne__(self, other: float) -> bool:
        return True

class NeverEqualFormattedFloat(FormattedFloat):
    def __eq__(self, other: int) -> bool:
        return False

    def __ne__(self, other: int) -> bool:
        return True


T_kwValue_pre: typ.TypeAlias = bool | float | int | str
T_kwValue_post: typ.TypeAlias = bool | FormattedFloat | FormattedInt | str


def format_values(
        value: T_kwValue_pre | None,
        fmt: str,
        comment: str | None = None,
        non_equalizable_formattables: bool = False) -> tuple[T_kwValue_post, str | None]:
    """
    Formats values into a (val, comment) string tuple
    """
    if value is None:
        return "", comment
    # wrap this in a huge block because sometimes there's garbage in
    try:
        if fmt == 'BOOLEAN':
            ovalue = bool(value)
        elif fmt[-1] == 'd':
            kls = (FormattedInt, NeverEqualFormattedInt)[non_equalizable_formattables]
            ovalue = kls(value, fmt)
        elif fmt[-1] == 'f':
            kls = (FormattedFloat, NeverEqualFormattedFloat)[non_equalizable_formattables]
            ovalue = kls(value, fmt)
        elif fmt[-1] == 's':  # string
            ovalue = fmt % value
    except Exception:  # Sometimes garbage values cannot be formatted properly...
        ovalue = str(value)
        print(f"fits_headers: formatting error on {value}, {fmt}, {comment}")

    return ovalue, comment
