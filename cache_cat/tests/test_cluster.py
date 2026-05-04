from time import sleep

import redis

r = redis.Redis(
    # db=0,
    host='localhost',
    port=6379,
    decode_responses=True
)

print()
r.lpush('test1', 'test')
print(r.lrange('test1', 0, -1))

r.hset('test2', 'test', 'test')
print(r.hget('test2', 'test'))

print(r.zadd("my_zset", {"a": 1, "b": 2, "c": 3}))

print(r.hincrby("test5", "test", 1))
print(r.hget("test5", "test"))
print(r.exists("test5"))

r.set("test6", "hello", ex=1)
r.persist("test6")
sleep(2)
print(r.get("test6"))
