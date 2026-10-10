#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
向 RabbitMQ 的 das.solar (fanout) 交换机每 3 秒推送一次 battery_status 消息
"""

import json
import time
import pika
import sys
from datetime import datetime

# ----------------- 配置区 -----------------
AMQP_URL      = "amqp://guest:guest@172.16.61.130:5672/"
EXCHANGE_IN   = "das.solar"
EXCHANGE_TYPE = "fanout"
DURABLE       = True
INTERVAL      = 3          # 秒
# 测试数据（可自行修改）
PAYLOAD = {
    "battery_percentage": 45,
    "battery_status": 3
}
# -----------------------------------------


def connect():
    """建立连接并声明 fanout 交换机"""
    params = pika.URLParameters(AMQP_URL)
    params.heartbeat = 30
    params.blocked_connection_timeout = 30

    conn = pika.BlockingConnection(params)
    ch = conn.channel()
    ch.exchange_declare(
        exchange=EXCHANGE_IN,
        exchange_type=EXCHANGE_TYPE,
        durable=DURABLE
    )
    return conn, ch


def main():
    conn, ch = connect()
    print(f"[{datetime.now():%Y-%m-%d %H:%M:%S}] 已连接 {AMQP_URL}")
    print(f"开始每 {INTERVAL} 秒向 exchange='{EXCHANGE_IN}' 推送消息，Ctrl+C 停止\n")

    try:
        while True:
            body = json.dumps(PAYLOAD, ensure_ascii=False)
            ch.basic_publish(
                exchange=EXCHANGE_IN,
                routing_key="",          # fanout 会忽略该字段
                body=body,
                properties=pika.BasicProperties(
                    content_type="application/json",
                    delivery_mode=2      # 2=持久化
                )
            )
            ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            print(f"[{ts}] 已发送 -> {body}")
            time.sleep(INTERVAL)

    except KeyboardInterrupt:
        print("\n收到 Ctrl+C，正在退出...")
    except pika.exceptions.AMQPConnectionError as e:
        print(f"连接异常: {e}", file=sys.stderr)
    finally:
        try:
            if conn.is_open:
                conn.close()
        except Exception:
            pass
        print("连接已关闭")


if __name__ == "__main__":
    main()
