import redis

print()

r = redis.Redis()
conn = r.connection_pool.get_connection("PING")

try:
    conn.send_command("SET", "k1", "v1")
    conn.send_command("SET", "k2", "v2")
    conn.send_command("GET", "k1")
    conn.send_command("GET", "k2")

    # 逐个读取响应（此时才开始收）
    print(conn.read_response())
    print(conn.read_response())
    print(conn.read_response())
    print(conn.read_response())
    print(conn.read_response())

finally:
    r.connection_pool.release(conn)
