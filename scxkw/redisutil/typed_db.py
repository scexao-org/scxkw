from __future__ import annotations
from typing import Any

import redis

import typing as typ


from .type_cast import nested_cast, to_redis_scalar_cast, ScxkwValueType
from ..config import MAGIC_BOOL_STR


def func_factory(method_name, superclass):
    '''
        func_factory wraps a redis function that returns only bytes/strings
        and casts its output into primitive python types.

        Resolve the function to patch OUTSIDE of the nested call
        So we do it only once when this "decorator" is called,
        and not dynamically during execution
        (which could cause infinite recursion)
    '''
    method_to_patch = getattr(superclass, method_name)
    def method(self, *args, **kwargs):
        try:
            #1/0
            ret = method_to_patch(self, *args, **kwargs)
        except (redis.exceptions.ConnectionError, ZeroDivisionError):
            print("Running in Redis-less mode - not available")
            return None # Bad idea?
        return nested_cast(ret)

    return method

class Pipeline(redis.client.Pipeline):
    
    def __init__(self, connection_pool, response_callbacks, transaction, shard_hint, *, auto_execute: typ.Optional[int] = 50) -> None:
        super().__init__(connection_pool, response_callbacks, transaction, shard_hint)

        self.auto_execute = auto_execute
        self.return_cache = []

    def execute_command(self, *args, **options):
        ret =  super().execute_command(*args, **options)
        if self.auto_execute and len(self.command_stack) >= self.auto_execute:
            self.return_cache += super().execute()
        return ret
    
    def execute(self, raise_on_error: bool = True) -> list[Any]:
        ret = self.return_cache + super().execute(raise_on_error)
        self.return_cache = []
        return ret

    def hset(self, name: str,
             key: typ.Optional[str] = None,
             value: typ.Optional[ScxkwValueType] = None,
             mapping: typ.Optional[typ.Mapping[str, ScxkwValueType]] = None
             ):
        '''
        Hack hset for the magic boolean
        '''
        new_value = None if value is None else to_redis_scalar_cast(value)
        new_mapping = None if mapping is None else {k: to_redis_scalar_cast(mapping[k]) for k in mapping}
        
        return redis.client.Pipeline.hset(self, name=name, key=key, value=new_value, mapping=new_mapping)


class Redis(redis.Redis):
    
    def __init__(self, *args, **kwargs):
        if not "socket_connect_timeout" in kwargs:
            kwargs["socket_connect_timeout"] = 1.0
        if not "socket_timeout" in kwargs:
            kwargs["socket_timeout"] = 1.0
        return redis.Redis.__init__(self, *args, **kwargs)

    def pipeline(self, transaction=False, shard_hint=None, auto_execute: typ.Optional[int] = 50):
        return Pipeline(
            self.connection_pool,
            self.response_callbacks,
            transaction,
            shard_hint,
            auto_execute = auto_execute)
    
    def hset(self, name: str,
            key: typ.Optional[str] = None,
            value: typ.Optional[ScxkwValueType] = None,
            mapping: typ.Optional[typ.Mapping[str, ScxkwValueType]] = None
            ):
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
    if hasattr(Redis, method_name):
        setattr(Redis, method_name, func_factory(method_name, Redis))
    # In particular, we need the cast on the return of Pipeline.execute! But we've overloaded it!
    # So take Pipeline.execute, wrap and overset Pipeline.execute.
    if hasattr(Pipeline, method_name):
        setattr(Pipeline, method_name, func_factory(method_name, Pipeline))


