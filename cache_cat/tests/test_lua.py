from time import sleep

import redis

r = redis.Redis(
    db=0,
    host='localhost',
    port=6379,
    decode_responses=True
)

r.set('key1', '2')

print()

lua_script = """
local value = redis.call('GET', 'key1')
return value
"""

# 执行脚本
result = r.eval(lua_script, 0)
print(f"GET 结果: {result}")