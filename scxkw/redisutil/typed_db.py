import redis

from .type_cast import nested_cast


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
    pass


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


METHODS_TO_CAST = [
    'get', 'hget', 'hmget', 'hgetall', 'scan', 'hscan', 'sadd', 'smembers',
    'sinter', 'sunion', 'exists', 'hexists', 'keys', 'execute', 'type',
]

for method_name in METHODS_TO_CAST:
    setattr(Redis, method_name, func_factory(method_name, redis.Redis))
    setattr(Pipeline, method_name, func_factory(method_name, redis.client.Pipeline))
