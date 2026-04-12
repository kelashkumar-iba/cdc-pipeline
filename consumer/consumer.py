import json
import os
import time
import base64
import struct
from confluent_kafka import Consumer, KafkaError
import psycopg2

KAFKA_BROKER = os.environ.get("KAFKA_BROKER", "redpanda:29092")
TOPICS = ["cdc.public.customers", "cdc.public.orders"]

DB_CONFIG = {
    "host": os.environ.get("TARGET_DB_HOST", "target_db"),
    "port": os.environ.get("TARGET_DB_PORT", "5432"),
    "dbname": os.environ.get("TARGET_DB_NAME", "target"),
    "user": os.environ.get("TARGET_DB_USER", "postgres"),
    "password": os.environ.get("TARGET_DB_PASSWORD", "postgres"),
}

DEAD_LETTER_LOG = "/app/dead_letter_queue.log"


def log_dead_letter(event, error):
    with open(DEAD_LETTER_LOG, "a") as f:
        f.write(json.dumps({"error": str(error), "event": event}) + "\n")
    print(f"[DLQ] Event logged: {error}")


def decode_decimal(value, scale=2):
    if isinstance(value, str):
        try:
            raw = base64.b64decode(value)
            unscaled = int.from_bytes(raw, byteorder="big", signed=True)
            return unscaled / (10 ** scale)
        except Exception:
            return value
    return value


def get_db_connection():
    while True:
        try:
            conn = psycopg2.connect(**DB_CONFIG)
            conn.autocommit = True
            return conn
        except psycopg2.OperationalError as e:
            print(f"[DB] Waiting for target_db... {e}")
            time.sleep(3)


def ensure_target_tables(conn):
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS customers (
            customer_id INT PRIMARY KEY,
            name VARCHAR(100),
            email VARCHAR(150),
            city VARCHAR(100),
            created_at BIGINT,
            updated_at BIGINT
        );
        CREATE TABLE IF NOT EXISTS orders (
            order_id INT PRIMARY KEY,
            customer_id INT,
            product VARCHAR(200),
            amount DECIMAL(10,2),
            status VARCHAR(20),
            created_at BIGINT,
            updated_at BIGINT
        );
    """)
    cur.close()
    print("[DB] Target tables ready.")


def handle_event(conn, topic, event):
    table = topic.split(".")[-1]
    op = event["op"]
    cur = conn.cursor()
    try:
        if op in ("r", "c", "u"):
            row = event["after"]
            if table == "orders" and "amount" in row:
                row["amount"] = decode_decimal(row["amount"])
            columns = list(row.keys())
            values = list(row.values())
            placeholders = ", ".join(["%s"] * len(values))
            col_str = ", ".join(columns)
            pk = "customer_id" if table == "customers" else "order_id"
            update_str = ", ".join([f"{c} = EXCLUDED.{c}" for c in columns if c != pk])
            sql = f"""
                INSERT INTO {table} ({col_str}) VALUES ({placeholders})
                ON CONFLICT ({pk}) DO UPDATE SET {update_str}
            """
            cur.execute(sql, values)
            print(f"[{op.upper()}] {table}: PK={row.get(pk)}")
        elif op == "d":
            row = event["before"]
            pk = "customer_id" if table == "customers" else "order_id"
            cur.execute(f"DELETE FROM {table} WHERE {pk} = %s", (row[pk],))
            print(f"[D] {table}: PK={row[pk]} deleted")
    except Exception as e:
        log_dead_letter(event, e)
    finally:
        cur.close()


def main():
    print("[Consumer] Starting CDC consumer...")
    conn = get_db_connection()
    ensure_target_tables(conn)
    consumer = Consumer({
        "bootstrap.servers": KAFKA_BROKER,
        "group.id": "cdc-consumer-group",
        "auto.offset.reset": "earliest",
    })
    consumer.subscribe(TOPICS)
    print(f"[Consumer] Subscribed to {TOPICS}")
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                print(f"[Kafka Error] {msg.error()}")
                continue
            try:
                value = json.loads(msg.value().decode("utf-8"))
                event = value["payload"]
                handle_event(conn, msg.topic(), event)
            except Exception as e:
                log_dead_letter(msg.value().decode("utf-8"), e)
    except KeyboardInterrupt:
        print("[Consumer] Shutting down...")
    finally:
        consumer.close()
        conn.close()


if __name__ == "__main__":
    main()
