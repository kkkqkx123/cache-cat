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
print(r.hset('321', 'zhangsan', '1'))
