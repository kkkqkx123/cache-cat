import redis

r = redis.Redis(
    # db=0,
    host='localhost',
    port=6379,
    decode_responses=True
)

print()
res = r.zadd('222', {
    'zhangsan': 18,  # member: 'zhangsan', score: 18
    'lisi': 20,  # member: 'lisi', score: 20
    'wangwu': 19  # member: 'wangwu', score: 19
})
print(res)
print(r.zrange('222', 0, -1))
