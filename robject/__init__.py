from redis import Redis

from robject.queryset import SortedQuerySet, ListQuerySet

_redis = None

def connect(*args, **kwargs):
    global _redis
    if args and isinstance(args[0], Redis):
        _redis = args[0]
    else:
        _redis = Redis(*args, **kwargs)
    return _redis

def connection():
    global _redis
    return _redis


class Field(object):
    def __init__(self, field_name, using=None):
        """
        Represents a key called field_name in Redis.
        Uses the global redis connection or the given one in using.
        """
        self.redis = using or connection()
        self.field_name = field_name

    def exists(self):
        return self.redis.exists(self.key())

    def key(self):
        """ Return the Redis key for this field. """
        return self.field_name

    def from_object(self, object):
        """
        Convert the desired object into the value that should be inserted.
        E.g. get the numerical ID of a model.
        """
        return object

    def to_objects(self, results):
        """
        Convert the result list into the desired object.
        E.g. load the appropriate model given a numerical ID.
        """
        return results

    def delete(self):
        return self.redis.delete(self.key())


class Sorted(Field):
    def add(self, obj, score=None):
        obj = self.from_object(obj)

        if score == None:
            score = obj

        return self.redis.zadd(self.key(), obj, score)

    def remove(self, obj):
        return self.redis.zrem(self.key(), self.from_object(obj))

    def incr(self, obj, amount=1):
        return self.redis.zincr(self.key(), self.from_object(obj), amount)

    def all(self, reverse=False):
        return SortedQuerySet(self.to_objects, reverse=reverse, field=self)

    # TODO: ZRANGEBYSCORE, ZREMRANGEBYSCORE

    def count(self):
        return self.redis.zcard(self.key())

    def contains(self, obj):
        return bool(self.score(obj))

    def score(self, obj):
        return self.redis.zscore(self.key(), self.from_object(obj))

    # TODO: SORT


class Set(Field):
    def add(self, obj):
        return self.redis.sadd(self.key(), self.from_object(obj))

    def remove(self, obj):
        return self.redis.srem(self.key(), self.from_object(obj))

    def move(self, obj, other):
        return self.redis.smove(self.key(), other.key(), self.from_object(obj))

    def count(self):
        return self.redis.scard(self.key())

    def contains(self, obj):
        return bool(self.redis.sismember(self.key(), self.from_object(obj)))

    def __contains__(self, obj):
        return self.contains(obj)

    # TODO: SINTER*, SUNION*, SDIFF*

    def all(self, reverse=False):
        return self.to_objects(self.redis.smembers(self.key())) or []
        #return ListQuerySet(self.to_objects, field=self)
