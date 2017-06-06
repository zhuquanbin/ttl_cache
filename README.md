# ttl_cache
dependence
  pyredblack (0.1.1)  # 红黑树 dict set
example
```Python
import random
import pyredblack
import datetime
import uuid
import gevent
from gevent import  monkey
monkey.patch_all()

from cache import TTL_Cache
cache = TTL_Cache.getInstance()

import logging
logging.basicConfig(level=logging.INFO,
                format='%(asctime)s [%(levelname)s] %(message)s',
                datefmt='%Y-%m-%d %H:%M:%S',
                filename='test.log',
                filemode='w')


def callback(*args, **kwargs):
    _ct = datetime.datetime.now()
    logging.info("key: %s, value: %s, create_time:%s, expired_time:%s, live:%s" % (
    kwargs['key'], kwargs['value'], kwargs['time'], _ct, _ct - kwargs['time']))

def check_expired_cache():
    global  cache
    while True:
        cache.clear_expired(callback, msg="checked expired")
        gevent.sleep(0.2)

def generate_cache():
    global cache
    for x in xrange(1,200):
        for y in xrange(1,1000):
            id = str(uuid.uuid1())
            ttl = (x*y % 6 +1) *10*1000
            cache.setex(id, {"x":x ,"y":y, "ttl": ttl}, ttl)
        gevent.sleep(0.5)

def test_create_with_expired():
    gevent.joinall([
        gevent.spawn(generate_cache),
        gevent.spawn(check_expired_cache)
    ])

if __name__ == '__main__':
    test_create_with_expired()
```
