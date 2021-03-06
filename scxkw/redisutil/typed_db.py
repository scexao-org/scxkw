import redis

from .type_cast import nested_cast


def func_factory(method_name, superclass):
    def method(self, *args, **kwargs):
        superclass_method = getattr(superclass, method_name)
        ret = superclass_method(self, *args, **kwargs)
        return nested_cast(ret)

    return method


class Pipeline(redis.client.Pipeline):
    pass


class Redis(redis.Redis):
    
    def pipeline(self, transaction=True, shard_hint=None):
        return Pipeline(
            self.connection_pool,
            self.response_callbacks,
            transaction,
            shard_hint)


METHODS_TO_CAST = [
    'get', 'hget', 'hmget', 'hgetall', 'scan', 'hscan', 'sadd', 'smembers',
    'sinter', 'sunion', 'exists', 'hexists', 'keys', 'execute'
]

for method_name in METHODS_TO_CAST:
    setattr(Redis, method_name, func_factory(method_name, redis.Redis))
    setattr(Pipeline, method_name, func_factory(method_name, redis.client.Pipeline))