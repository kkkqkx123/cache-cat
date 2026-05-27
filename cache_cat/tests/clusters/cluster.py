# import redis
#
# direct_sentinel_conn = redis.Redis(host='127.0.0.1', port=6380)
# masters_info = direct_sentinel_conn.sentinel_masters()
#
# print(masters_info)
#
# slave_info = direct_sentinel_conn.sentinel_slaves("cat")
# print(slave_info)
from time import sleep

from redis.sentinel import Sentinel
print()
sentinel = Sentinel(
    [
        ('127.0.0.1', 6379),
        ('127.0.0.1', 6380),
        ('127.0.0.1', 6381),
    ],
    socket_timeout=0.5
)
master = sentinel.master_for(
    'cat',
    socket_timeout=0.5,
    db=0
)
master.set('foo', 'bar')
while True:
    sleep(1)
    print(master.get('foo'))
