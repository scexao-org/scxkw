from typing import Union, List, Set, Tuple, Dict


def b2s(byt: bytes):
    return byt.decode('utf-8')


def scalar_cast(value: str):
    cast_types = [int, float, complex]
    for type in cast_types: # Try casting successively, return the first success
        try:
            return type(value)
        except:
            continue
    return value # It was probably a non-numeric string in the first place


def nested_cast(stuff: Union[List, Set, Tuple, Dict]):
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
