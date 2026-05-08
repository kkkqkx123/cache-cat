from time import sleep

import redis

r = redis.Redis(
    # db=0,
    host='localhost',
    port=6379,
    decode_responses=True
)

# print()
# r.lpush('test1', 'test')
# print(r.lrange('test1', 0, -1))
#
# r.hset('test2', 'test', 'test')
# print(r.hget('test2', 'test'))
#
# print(r.zadd("my_zset", {"a": 1, "b": 2, "c": 3}))
#
# print(r.hincrby("test5", "test", 1))
# print(r.hget("test5", "test"))
# print(r.exists("test5"))
#
# r.set('test6', 'test')
# print(r.rename('test6', 'test7'))
# print(r.get('test7'))
# 1秒过期
r.set('test10', 'test')
# print(r.save())
print(r.bgsave(True))
print(r.bgsave(True))
# sleep(0.99)
# print(r.get('test8'))
