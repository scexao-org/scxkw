from __future__ import annotations

from scxkw.config import MAGIC_BOOL_STR

import typing as typ

ScxkwValueType = typ.Union[bool, int, float, complex, str]
ScxkwValueTypeNoBool = typ.Union[int, float, complex, str]


def b2s(byt: bytes) -> str:
    return byt.decode('utf-8')

def to_redis_scalar_cast(value: ScxkwValueType) -> ScxkwValueTypeNoBool:
    if type(value) is bool:
        return MAGIC_BOOL_STR.TUPLE[value]
    return value


def scalar_cast(value: str) -> ScxkwValueType:
    cast_types = [int, float, complex]
    for type in cast_types: # Try casting successively, return the first success
        try:
            return type(value)
        except:
            continue
    if value == MAGIC_BOOL_STR.TRUE:
        return True
    if value == MAGIC_BOOL_STR.FALSE:
        return False
    return value # It was probably a non-numeric string in the first place


def nested_cast(stuff: typ.Union[typ.List, typ.Set, typ.Tuple, typ.Dict]):
    if stuff is None:
        return None
    elif type(stuff) is tuple:
        return tuple((nested_cast(x) for x in stuff))
    elif type(stuff) is list:
        return [nested_cast(x) for x in stuff]
    elif type(stuff) is set:
        return {nested_cast(x) for x in stuff}
    elif type(stuff) is dict:
        return {scalar_cast(b2s(x)): nested_cast(stuff[x]) for x in stuff}
    elif type(stuff) is bytes:
        return scalar_cast(b2s(stuff))
    else:
        return stuff
