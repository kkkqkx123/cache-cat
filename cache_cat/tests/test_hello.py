import redis

# 启用 RESP3 协议
r = redis.Redis(host='localhost', port=6379, protocol=3)

r.hset('user:1', mapping={
    'name': 'Alice',
    'age': 25,
    'city': 'Beijing'
})

print(r.hgetall('user:1'))