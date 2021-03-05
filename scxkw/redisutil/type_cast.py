import ast
from typing import Union, List, Set, Tuple, Dict


def b2s(byt: bytes):
    return byt.decode('utf-8')


def scalar_cast(value: str):
    try:  # Can we cast it to a scalar ?
        return ast.literal_eval(value)
    except ValueError:  # It was probably a string in the first place
        return value


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
