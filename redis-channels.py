import redis
import random
import time

class Client(redis.Redis):
    KEY_PREFIX = 'channel'
    TIMEOUT = 1

    """
    Redis keys (prefixed with "<KEY_PREFIX>:"):
        list
        Channel list

        #<name>:clients
        Clients listening to this channel

        #<name>:<client>
        Channel messages for client
    """

    def __init__(self, redis):
        # Assign a random client ID.
        self.id = '%016x' % random.getrandbits(128)
        self.redis = redis
        self.channels = set()

    def _timestamp(self):
        return int(time.time())

    def open(self, channel):
        self.channels.add(channel)
        self.redis.zadd('%s:#%s:clients' % (self.KEY_PREFIX, channel), self.id, self._timestamp())
        self.redis.zadd('%s:list' % self.KEY_PREFIX, channel, self._timestamp())

    def push(self, channel, message):
        for client in self.redis.zrange('%s:#%s:clients' % (self.KEY_PREFIX, channel), 0, -1):
            self.redis.lpush('%s:#%s:%s' % (self.KEY_PREFIX, channel, client), message)

    def pop(self):
        while True:
            result = self.redis.blpop(['%s:#%s:%s' % (self.KEY_PREFIX, channel, self.id) for channel in self.channels], self.TIMEOUT)
            if result:
                channel, msg = result
                return channel[len(self.KEY_PREFIX)+1 : -len(self.id)-1], msg
            else:
                # Update client timestamp
                for channel in self.channels:
                    self.open(channel)

    def close(self, channel):
        self.channels.remove(channel)
        self.redis.zrem('%s:#%s:clients' % (self.KEY_PREFIX, channel), self.id)
        if self.redis.zcard('%s:#%s:clients' % (self.KEY_PREFIX, channel)) == 0:
            # XXX: Possible race-condition. Channel clean-up should be done by separate process.
            self.redis.zrem('%s:list' % self.KEY_PREFIX, channel)

    # TODO: Have a separate process that cleans up clients that timed out.


if __name__ == '__main__':
    import sys

    r = redis.Redis(port=62048)

    if len(sys.argv) < 2:
        print 'not enough args'
        sys.exit(1)

    if sys.argv[1] == 'pop':
        c = Client(r)
        c.open('chan1')
        c.open('chan2')

        while True:
            chan, msg = c.pop()
            print 'Got message on %s: %s' % (chan, msg)

        # Never reached, but should be called somewhere.
        c.close('chan1')
        c.close('chan2')

    if sys.argv[1] == 'push':
        # e.g. push chan1 msg
        c = Client(r)
        c.push(sys.argv[2], sys.argv[3])
