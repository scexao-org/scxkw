import redis

from .type_cast import nested_cast


def func_factory(method_name):

    def method(self, *args, **kwargs):
        superclass_method = getattr(redis.Redis, method_name)
        ret = superclass_method(self, *args, **kwargs)
        return nested_cast(ret)

    return method


class Redis(redis.Redis):
    pass


METHODS_TO_CAST = [
    'get', 'hget', 'hmget', 'hgetall', 'scan', 'hscan', 'sadd', 'smembers', 'sinter',
    'sunion', 'exists', 'hexists', 'keys'
]

for method_name in METHODS_TO_CAST:
    setattr(Redis, method_name, func_factory(method_name))