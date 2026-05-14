import redis
import threading
import time

r = redis.Redis(
    db=0,
    host='localhost',
    port=6379,
    decode_responses=True
)

def dead_loop_script():
    """死循环Lua脚本"""
    script = """
    while true do
        redis.call('GET','key')
    end
    """
    r.eval(script, 0)

# 启动线程执行死循环脚本
t = threading.Thread(target=dead_loop_script, daemon=True)
t.start()

time.sleep(2)  # 等待脚本开始运行

print("尝试Kill脚本...")
try:
    # 设置超时，避免永久阻塞
    result = r.script_kill()
    print(f"结果: {result}")
except Exception as e:
    print(f"失败: {e}")

print("已执行script_kill")  # 这行可能很久之后才执行