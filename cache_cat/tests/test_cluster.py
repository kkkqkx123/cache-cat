from time import sleep

import redis
import time

r = redis.Redis(
    # db=0,
    host='localhost',
    port=6379,
    decode_responses=True
)

# 设置 key，1 秒后过期
r.set('3333333', "test")
r.delete('3333333')
print(r.get('3333333'))
