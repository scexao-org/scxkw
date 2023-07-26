from __future__ import annotations

import redis

import typing as typ


from .type_cast import nested_cast, to_redis_scalar_cast, ScxkwValueType
from ..config import MAGIC_BOOL_STR


def func_factory(method_name, superclass):
    def method(self, *args, **kwargs):
        superclass_method = getattr(superclass, method_name)
        try:
            #1/0
            ret = superclass_method(self, *args, **kwargs)
        except (redis.exceptions.ConnectionError, ZeroDivisionError):
            print("Running in Redis-less mode - not available")
            return None # Bad idea?
        return nested_cast(ret)

    return method

class Pipeline(redis.client.Pipeline):
    
    def hset(self, name: str,
             key: typ.Optional[str],
             value: typ.Optional[ScxkwValueType],
             mapping: typ.Optional[typ.Mapping[str, ScxkwValueType]]):
        '''
        Hack hset for the magic boolean
        '''
        new_value = None if value is None else to_redis_scalar_cast(value)
        new_mapping = None if mapping is None else {k: to_redis_scalar_cast(mapping[k]) for k in mapping}
        
        return redis.client.Pipeline.hset(self, name=name, key=key, value=new_value, mapping=new_mapping)


class Redis(redis.Redis):
    
    def __init__(self, *args, **kwargs):
        if not "socket_connect_timeout" in kwargs:
            kwargs["socket_connect_timeout"] = 0.1
        if not "socket_timeout" in kwargs:
            kwargs["socket_timeout"] = 0.1
        return redis.Redis.__init__(self, *args, **kwargs)

    def pipeline(self, transaction=True, shard_hint=None):
        return Pipeline(
            self.connection_pool,
            self.response_callbacks,
            transaction,
            shard_hint)
    
    def hset(self, name: str,
            key: typ.Optional[str],
            value: typ.Optional[ScxkwValueType],
            mapping: typ.Optional[typ.Mapping[str, ScxkwValueType]]):
        '''
        Hack hset for the magic boolean
        '''
        new_value = None if value is None else to_redis_scalar_cast(value)
        new_mapping = None if mapping is None else {k: to_redis_scalar_cast(mapping[k]) for k in mapping}
        
        return redis.Redis.hset(self, name=name, key=key, value=new_value, mapping=new_mapping)


METHODS_TO_CAST = [
    'get', 'hget', 'hmget', 'hgetall', 'scan', 'hscan', 'sadd', 'smembers',
    'sinter', 'sunion', 'exists', 'hexists', 'keys', 'execute', 'type',
]

for method_name in METHODS_TO_CAST:
    setattr(Redis, method_name, func_factory(method_name, redis.Redis))
    setattr(Pipeline, method_name, func_factory(method_name, redis.client.Pipeline))
