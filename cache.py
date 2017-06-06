# -*- encoding:utf8 -*-
"""
    author: quanbin_zhu
    time  : 2017/6/1 14:45
    
    ttl cache:
            cache  --->  {}  [key - value dict]        hash map
            /   \
          node  node         [time partition]          tree map - rb
          /       \
       {...}     {...}       [key's time - key  map]   tree map - rb
"""
import sys
import time
import pyredblack
from datetime import datetime, timedelta

__all__ = ["TTL_Cache"]

class DValue(object):
    DEFAULT_TTL_MILLISECONDS = 7*24*60*60*1000
    EXPIRED_TIME_PARTITION   = 10
    def __init__(self, v, ttl = DEFAULT_TTL_MILLISECONDS):
        '''
        :param v:       value 
        :param ttl:     time to live  milliseconds
        '''
        self.value = v
        self.create_time = datetime.now()
        # self.milliseconds = ttl
        self.expire_time = self.create_time + timedelta(milliseconds = ttl)

    @property
    def time(self):
        return  self.create_time

    # @property
    # def expire_time(self):
    #     return  self.create_time + timedelta(milliseconds = self.milliseconds)

    @property
    def is_expired(self):
        return datetime.now() >= self.expire_time

    def get_expire_partition(self, seconds = EXPIRED_TIME_PARTITION):
        '''
        :param seconds  
        how many seconds every partition,  every partition is rb-tree
        '''
        timestamp = int(time.mktime(self.expire_time.timetuple()))
        return timestamp / seconds * seconds


class Partition_Node(object):
    def __init__(self):
        self.partition = pyredblack.rbdict()
        self.size = 0

    def insert(self, key, value):
        '''
        :param key:     key is datetime 
        :param value:   value is key map with cache  
        :return: 
        '''
        if key not in self.partition:
            self.partition[key] = dict()
        self.partition[key][value] = None
        self.size += 1

    def delete(self, key, value):
        """ delete time with key map"""
        del self.partition[key][value]
        if not self.partition[key]:
            del self.partition[key]
        self.size -= 1

    def iteritems(self):
        return self.partition.iteritems()

    def iterkeys(self):
        return self.partition.iterkeys()

    def itervalues(self):
        return self.partition.itervalues()

    def __iter__(self):
        return self.partition.__iter__()


class ExpireDict(dict):
    def __init__(self, seq = None, **kwargs):
        super(ExpireDict, self).__init__()
        self.ttl_cache_map = pyredblack.rbdict()

    def get(self, key, default = None):
        try:
            return self.__getitem__(key)
        except KeyError:
            return default

    def pop(self, key, default=None):

        """ Get item from the dict and remove it.
        Return default if expired or does not exist. Never raise KeyError.
        
        """
        try:
            item = super(ExpireDict, self).pop(key)
            if item :
                self.del_ttl_map(item.get_expire_partition(), item.time, key)
                if not item.is_expired:
                    return item
        except KeyError:
            pass
        return default

    def set_ttl_map(self, partition, expire_time, key):
        '''
        set key with ttl map 
        '''
        if partition not in self.ttl_cache_map:
            self.ttl_cache_map[partition] = Partition_Node()
        node = self.ttl_cache_map.get(partition)
        node.insert(expire_time, key)

    def del_ttl_map(self, partition, expire_time, key):
        '''
        delete key from ttl map 
        '''
        try:
            node = self.ttl_cache_map.get(partition)
            node.delete(expire_time, key)
            if 0 == node.size:
                del self.ttl_cache_map[partition]
        except KeyError, e:
            pass

    def __setitem__(self, key, value):

        if self.__contains__(key):
            try:
                item = self.__getitem__(key)
                self.del_ttl_map(item.get_expire_partition(), item.expire_time, key)
            except KeyError:
                pass
        super(ExpireDict, self).__setitem__(key, value)
        self.set_ttl_map(value.get_expire_partition(), value.expire_time, key)

    def __delitem__(self, key):
        _ = self.pop(key)

    def __contains__(self, key):
        try:
            if self.__getitem__(key):
                return  True
        except KeyError:
            pass
        return False

    def __getitem__(self, key):
        item = super(ExpireDict, self).get(key, None)
        if not item:    raise  KeyError(key)
        if not item.is_expired:
            return item
        else:
            self.del_ttl_map(item.get_expire_partition(), item.expire_time, key)
            super(ExpireDict, self).__delitem__(key)
            raise KeyError(key)

    def clear(self):
        super(ExpireDict, self).clear()
        self.ttl_cache_map.clear()

    def clear_expired(self, callback = None ,*args, **kwargs):
        # expired_keys = []
        for node in self.ttl_cache_map:
            if node > int(time.time()):
                break
            partition = self.ttl_cache_map[node]
            for k, v in partition.iteritems():
                if k > datetime.now():
                    break
                # expired_keys += v.keys()
                for _k in v.keys():
                    item = super(ExpireDict, self).pop(_k)
                    if item:
                        self.del_ttl_map(item.get_expire_partition(), item.expire_time, _k)
                        if callback:
                            kwargs.update({"key": _k, "value": item.value, "time": item.time})
                            callback(*args, **kwargs)

        # for k in expired_keys:
        #     item = super(ExpireDict, self).pop(k)
        #     if item:
        #         self.del_ttl_map(item.get_expire_partition(), item.expire_time, k)
        #         if callback:
        #             kwargs.update({"key":k, "value" :item.value, "time":item.time})
        #             callback(*args, **kwargs)

    def popitem(self):
        raise NotImplementedError

    def setdefault(self, k, d=None):
        raise NotImplementedError

    def update(self, E=None, **F):
        raise NotImplementedError

    def copy(self):
        raise NotImplementedError


class TTL_Cache(object):
    __instance = None

    def __init__(self):
        self._cache = ExpireDict()

    def get(self, key):
        v = self._cache.get(key, None)
        if v:
            return v.value
        else:
            return None

    def set(self, key, value):
        self._cache[key] = DValue(value)

    def setex(self, key, value, milliseconds):
        self._cache[key] = DValue(value, ttl=milliseconds)

    def update(self, key, value, milliseconds):
        self._cache[key] = DValue(value, ttl=milliseconds)

    def delete(self, key):
        del self._cache[key]

    def clear(self):
        self._cache.clear()

    def clear_expired(self, callback = None, *args, **kwargs):
        self._cache.clear_expired(callback, *args, **kwargs)

    @classmethod
    def getInstance(cls):
        if not TTL_Cache.__instance:
            TTL_Cache.__instance = TTL_Cache()
        return  TTL_Cache.__instance

