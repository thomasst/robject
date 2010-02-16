import copy

# Hacked QuerySet from Django. Maybe better to write from scratch.

REPR_OUTPUT_SIZE = 20
ITER_CHUNK_SIZE = 100 # copied from Django, not sure what exactly this is for

class QuerySet(object):
    def __init__(self, to_objects_func):
        self.to_objects_func = to_objects_func
        self._result_cache = None
        self._iter = None
        self._limits = [0, None]

    def set_limits(self, start, stop):
        # TODO: Stuff like [-10:][:20] is not working properly.

        self_start, self_stop = self._limits

        if stop is not None:
            if self_stop is not None:
                self_stop = min(self_stop, self_start + stop)
            else:
                self_stop = self_start + stop

        if start is not None:
            if self_stop is not None:
                self_start = min(self_stop, self_start + start)
            else:
                self_start = self_start + start

        self._limits = [self_start, self_stop]

    def _clone(self):
        return copy.copy(self)

    def all(self):
        return self._clone()

    def __getitem__(self, i):
        qs = self._clone()
        if isinstance(i, slice):
            qs.set_limits(i.start, i.stop)
            return qs
        elif isinstance(i, (int, long)):
            qs.set_limits(i, i+1)
            return list(qs)[0]
        else:
            raise

    def results_iter(self):
        raise NotImplemented

    def iterator(self):
        for obj in self.to_objects_func(self.results_iter()):
            yield obj

    def __iter__(self):
        if self._result_cache is None:
            self._iter = self.iterator()
            self._result_cache = []
        if self._iter:
            return self._result_iter()
        # Python's list iterator is better than our version when we're just
        # iterating over the cache.
        return iter(self._result_cache)

    def _result_iter(self):
        pos = 0
        while 1:
            upper = len(self._result_cache)
            while pos < upper:
                yield self._result_cache[pos]
                pos = pos + 1
            if not self._iter:
                raise StopIteration
            if len(self._result_cache) <= pos:
                self._fill_cache()

    def _fill_cache(self, num=None):
        """
        Fills the result cache with 'num' more entries (or until the results
        iterator is exhausted). 
        """     
        if self._iter:
            try:
                for i in range(num or ITER_CHUNK_SIZE):
                    self._result_cache.append(self._iter.next())
            except StopIteration:
                self._iter = None

    def __repr__(self):
        data = list(self[:REPR_OUTPUT_SIZE + 1])
        if len(data) > REPR_OUTPUT_SIZE:
            data[-1] = "...(remaining elements truncated)..."
        return repr(data)

    def __len__(self):
        # Since __len__ is called quite frequently (for example, as part of
        # list(qs), we make some effort here to be as efficient as possible
        # whilst not messing up any existing iterators against the QuerySet.
        if self._result_cache is None:
            if self._iter:
                self._result_cache = list(self._iter)
            else:
                self._result_cache = list(self.iterator())
        elif self._iter:
            self._result_cache.extend(list(self._iter))
        return len(self._result_cache)


class SortedQuerySet(QuerySet):
    def __init__(self, to_objects_func, reverse, field):
        super(SortedQuerySet, self).__init__(to_objects_func)
        self.reverse = reverse
        self.field = field

    def results_iter(self):
        start, stop = self._limits
        if start == None:
            start = 0
        if stop == None:
            stop = 0

        for id in self.field.redis.zrange(self.field.key(), start, stop-1, desc=self.reverse):
            yield id


class ListQuerySet(QuerySet):
    def __init__(self, to_objects_func, field):
        super(ListQuerySet, self).__init__(to_objects_func)
        self.field = field
        self.to_objects_func = to_objects_func

    def results_iter(self):
        start, stop = self._limits

        #for id in self.field.redis.smembers(self.field.key())[start:stop]:
        #    yield id
