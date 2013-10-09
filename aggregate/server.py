#!/bin/env python
import zmq
import redis
import argparse

from threading import Thread
from zmq.eventloop import ioloop

class Aggregator(object):
    def __init__(self, db):
        self.data = {}

        self.__db = db

        host, port, db = db.split(':')

        self.client = redis.StrictRedis(host, int(port), db)

    def insert(self, tags, values):
        key = repr(frozenset(tags.items()))

        for i, v in values.iteritems():
            self.client.hincrbyfloat(key, i, float(v))

    def select(self, group_by=[], where={}):
        from django.conf import settings

        if not group_by and not where:
            return [dict(list(k)+v.items()) for k,v in self.data.iteritems()]

        a = Aggregator(self.__db)

        for k, v in self.data.iteritems():
            matched = 0
            for key_k, key_v in k:
                try:
                    if where[key_k] == key_v:
                        matched += 1
                    else:
                        break
                except KeyError:
                    pass
            if matched < len(where):
                continue
            a.insert(dict((kk, vv) for kk,vv in k if kk in group_by),
                     v)
        return a.select()

    def clear(self):
        self.data = {}

def ctl(aggregator):
    context = zmq.Context.instance()
    socket = context.socket(zmq.REP)
    socket.bind("tcp://*:5557")
    while True:
        cmd, args, kwargs = socket.recv_pyobj()
        ret = getattr(aggregator, cmd)(*args, **kwargs)
        socket.send_pyobj(ret)

def main():
    parser = argparse.ArgumentParser(description='Run aggregation daemon')

    parser.add_argument('--host', dest='host', action='store',
                        default='127.0.0.1',
                        help='The IP address/hostname to listen on')

    parser.add_argument('--port', dest='port', action='store', type=int,
                        default='5556',
                        help='The port to listen on')

    parser.add_argument('--db', dest='db', required=True,
        action='store', help='Redis configuration (ip_address:port:dbnum)')

    args = parser.parse_args()
    context = zmq.Context.instance()
    socket = context.socket(zmq.SUB)
    socket.bind("tcp://%s:%d"%(args.host, args.port))
    socket.setsockopt(zmq.SUBSCRIBE,'')
    a = Aggregator(args.db)
    statthread = Thread(target=ctl, args=(a,))
    statthread.daemon = True
    statthread.start()

    while True:
        q = socket.recv_pyobj()
        for l in q:
            a.insert(*l)

if __name__ == "__main__":
    main()
