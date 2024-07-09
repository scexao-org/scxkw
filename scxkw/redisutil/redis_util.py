from __future__ import annotations

import typing as typ

from .typed_db import Redis
from ..config import REDIS_DB_HOST, REDIS_DB_PORT

def get_comments_for_keys(key_set: typ.Iterable[str]) -> dict[str, str]:
    return get_field_for_keys("Description", key_set)

def get_formats_for_keys(key_set: typ.Iterable[str]) -> dict[str, str]:
    return get_field_for_keys("Type", key_set)

def get_keys_from_redis(keyword_set: str) -> set[str]:
    with Redis(host=REDIS_DB_HOST, port=REDIS_DB_PORT) as rdb:
        s = rdb.smembers(keyword_set)
    return s

def get_all_uppercase_keys_from_redis() -> list[str]:
    with Redis(host=REDIS_DB_HOST, port=REDIS_DB_PORT) as rdb:
        s = rdb.keys('[A-Z]*')
    return s

def get_field_for_keys(field: str, key_set: typ.Iterable[str]) -> dict[str, typ.Any]:
    key_list = list(key_set)

    with Redis(host=REDIS_DB_HOST, port=REDIS_DB_PORT) as rdb:
        with rdb.pipeline() as pipe:
            for key in key_list:
                pipe.hget(key, field)
            all_comments = pipe.execute()

        key_comment_dict = {key: comment for (key, comment) in zip(key_list, all_comments)}
    
    return key_comment_dict