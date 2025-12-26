import json
import time
from kafka import KafkaConsumer
import happybase

# 配置信息
KAFKA_BOOTSTRAP_SERVERS = ["kafka:9092"]
TOPIC = "orders"
HBASE_HOST = "hbase"  # 注意：这里用的是 docker-compose 里的服务名
HBASE_PORT = 9090
HBASE_TABLE = "orders"
HBASE_CF = "info"
BATCH_SIZE = 1000     # 【优化点】每 1000 条写一次

def wait_for_hbase():
    """循环等待直到连接上 HBase Thrift Server"""
    while True:
        try:
            print(f"Connecting to HBase Thrift at {HBASE_HOST}:{HBASE_PORT}...")
            conn = happybase.Connection(HBASE_HOST, port=HBASE_PORT, timeout=10000)
            conn.open()
            print("Connected to HBase successfully.")
            return conn
        except Exception as e:
            print(f"HBase not ready yet ({e}), retrying in 5s...")
            time.sleep(5)

def ensure_table(conn):
    """确保表存在"""
    try:
        tables = [t.decode() for t in conn.tables()]
        if HBASE_TABLE not in tables:
            print(f"Creating table '{HBASE_TABLE}'...")
            conn.create_table(HBASE_TABLE, {HBASE_CF: dict()})
        else:
            print(f"Table '{HBASE_TABLE}' already exists.")
    except Exception as e:
        print(f"Error checking table: {e}")

def main():
    # 1. 初始化 HBase 连接
    conn = wait_for_hbase()
    ensure_table(conn)
    table = conn.table(HBASE_TABLE)

    # 2. 初始化 Kafka 消费者
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="earliest",
        group_id="kafka-to-hbase-batch-v1" # 改个组名，避免处理旧的offset
    )

    print(f"Start consuming (Batch Size: {BATCH_SIZE})...")

    # 【核心优化代码】使用 batch 上下文管理器
    # batch_size=1000：表示每攒够 1000 条，HappyBase 会自动发送一次网络请求
    try:
        with table.batch(batch_size=BATCH_SIZE) as b:
            for msg in consumer:
                order = msg.value
                row_key = order["order_id"]

                # 构造数据
                data = {
                    b"info:user_id": str(order["user_id"]).encode(),
                    b"info:category": str(order["category"]).encode(),
                    b"info:price": str(order["price"]).encode(),
                    b"info:quantity": str(order["quantity"]).encode(),
                    b"info:order_time": str(order["order_time"]).encode(),
                    b"info:status": str(order["status"]).encode()
                }

                # 【注意】b.put 只是把数据放入本地内存缓冲，不会立即发网络请求
                # 只有当缓冲达到 batch_size 时，或者退出 with 语句时，才会发送
                b.put(row_key, data)

                # 打印日志稍微控制一下频率，否则日志刷屏太快
                if int(row_key[3:]) % 100 == 0:
                    print(f"Buffered order: {row_key} (Auto-flush at {BATCH_SIZE})")

    except Exception as e:
        print(f"Critical Error: {e}")
        # 在生产环境中，这里应该有重连逻辑或者报警
        # 简单起见，报错直接退出容器，由 Docker 重启
        conn.close()

if __name__ == "__main__":
    main()