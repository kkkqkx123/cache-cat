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