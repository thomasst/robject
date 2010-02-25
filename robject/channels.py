import redis
import random
import time

import robject


class Client(redis.Redis):
    KEY_PREFIX = 'channel'
    MAX_IDLE = 60 # How often a client has to update its timestamp when using BLPOP.
    TIMEOUT = 300 # When old clients will be removed by the cleanup method.

    """
    Redis keys (prefixed with "<KEY_PREFIX>:"):
        ZSET clients
        Client list (ordeed by last activity timestamp)

        # ZSET list
        # Channel list (ordered by last activity timestamp)

        SET <client>:list
        Channel list of the client

        SET #<name>:clients
        Clients listening to this channel

        LIST #<name>:<client>
        Channel messages for client
    """

    # XXX: SET #<name>:clients is not deleted when the last client closes the
    # channel. This should be fixed in a later Redis version.

    # TODO: Channel list not yet implemented.

    def __init__(self, redis, id=None):
        if not id:
            # Assign a random client ID.
            self.id = '%016x' % random.getrandbits(128)
        else:
            # Use existing ID (e.g. for cleanup)
            self.id = id

        self.redis = redis

        self.client_set = robject.Sorted(self.key('clients'), using=redis)
        self.channel_set = robject.Set(self.key(self.id, 'list'), using=redis)
        self.channel_clients = {} # lists with clients that are connected to this channel.
        if not id:
            self.update()
        else: # existing client may be listening to channels.
            for channel in self.channel_set.all():
                self.channel_clients[channel] = robject.Set(self.key('#%s' % channel, 'clients'), using=self.redis)

    @classmethod
    def key(cls, *args):
        return ':'.join([cls.KEY_PREFIX] + list(args))

    @staticmethod
    def timestamp():
        return int(time.time())

    def update(self):
        # Update client timestamp.
        self.client_set.add(self.id, self.timestamp())

    def open(self, channel):
        self.channel_set.add(channel)
        self.channel_clients[channel] = robject.Set(self.key('#%s' % channel, 'clients'), using=self.redis)
        self.channel_clients[channel].add(self.id)
        self.update()

    def push(self, channel, message):
        channel_clients = robject.Set(self.key('#%s' % channel, 'clients'), using=self.redis)
        for client in channel_clients.all():
            self.redis.lpush(self.key('#%s' % channel, client), message)
        self.update()

    def pop(self):
        while True:
            result = self.redis.blpop([self.key('#%s' % channel, self.id) for channel in self.channel_clients.keys()], self.MAX_IDLE)
            if result:
                channel, msg = result
                return channel[len(self.KEY_PREFIX)+1 : -len(self.id)-1], msg
            else:
                self.update()

    def close(self, channel):
        self.channel_set.remove(channel)
        self.channel_clients[channel].remove(self.id)
        del self.channel_clients[channel]
        # XXX: Race-condition, if something is written to the channel after deleting this key.
        self.redis.delete(self.key('#%s' % channel, self.id))

    def disconnect(self):
        for channel in self.channel_clients.keys():
            self.close(channel)
        self.client_set.remove(self.id)
        self.channel_set.delete()

    @classmethod
    def cleanup(cls, redis):
        clients = redis.zrangebyscore(self.key('clients'), 0, cls.timestamp() - cls.TIMEOUT)
        for client in clients:
            cls(redis, id=client).disconnect()


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

    if sys.argv[1] == 'clean':
        Client.cleanup(r)
