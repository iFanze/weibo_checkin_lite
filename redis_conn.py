import redis


class RedisConn:
    def __init__(self, host="localhost", port=6379, db=0):
        self.redis_conn = redis.Redis(host=host, port=port, db=db,
                                      decode_responses=True)

    def redis_lfind(self, key, value):
        if not self.redis_conn.exists(key):
            return -2
        length = self.redis_conn.llen(key)
        for i in range(length):
            if value == self.redis_conn.lindex(key, i):
                return i
        return -1
