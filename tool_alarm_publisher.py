# -*- coding: utf-8 -*-
import json
import random
import time
import uuid
from datetime import datetime
import threading

import pika

# --- 配置区 ---
# RabbitMQ 连接信息
#盒子
# RABBIT_URL = "amqp://admin:hhkj1234@192.168.1.10:5672/"
RABBIT_URL = "amqp://guest:guest@localhost:5672/"
EXCHANGE = "das.alarm"
EXCHANGE_TYPE = "fanout"
ROUTING_KEY = ""

# RABBIT_URL = "amqp://hhkj:hhkj@172.16.60.217:5672/"
# EXCHANGE = "hhfh"
# EXCHANGE_TYPE = "direct"
# QUEUE_NAME = "fh_alarm"
# ROUTING_KEY = "hhfh"


# 模拟配置
NUM_ALARMS_TO_SEND = 1  # 1. 一次发送多少个开始报警
ALARM_DURATION_SECONDS = 20  # 2. 告警持续 n 秒
UPDATE_INTERVAL_SECONDS = 1  # 3. 在持续期间，每隔多少秒发送一次更新（设为1即每秒一次）
POINT_RANGE_START = 1  # 4. 点位范围开始
POINT_RANGE_END = 10  # 5. 点位范围结束
VFF_IMAGE_PATH = "/data/likai/fiberAlarm/fiberAlarmGateway/vff图2.png"  # 6. vff 图片固定路径，非空时结束告警会携带 vff_path 字段（例如 "/home/gxjc/fbg/vff/test.png"）

# --- VFF 图片发送时机配置 ---
# 可选值:
#   "before_end": 在结束告警之前发送（在告警持续期间发送，携带 vff_path 字段）
#   "after_end": 在结束告警之后发送（单独发送一条消息，携带 vff_path 字段）
#   "both": 在结束告警之前和之后都发送
#   "none": 不发送 vff 图片（即使 VFF_IMAGE_PATH 有值也不发送）
VFF_SEND_TIMING = "before_end"  # 默认在结束前发送

# 如果 VFF_SEND_TIMING 为 "before_end" 或 "both"，可以指定在持续的第几秒发送
# 如果设为 0，则在开始告警时立即发送
# 如果设为负数，则在结束告警前 n 秒发送（例如 -3 表示结束前3秒）
# 如果设为正数，则在开始告警后 n 秒发送
VFF_SEND_SECOND = -20  # 在结束前20秒发送

# 如果 VFF_SEND_TIMING 为 "after_end" 或 "both"，可以指定结束后延迟多少秒发送
VFF_DELAY_AFTER_END = 2  # 结束告警后延迟2秒发送


# --- 配置区结束 ---


def publish(alarm_message: dict, message_type: str = "未知"):
    """
    将一个构造好的告警消息字典发布到 RabbitMQ。
    这是一个通用的发布函数，只负责发送。

    :param alarm_message: 包含告警信息的字典。
    :param message_type: 消息类型描述（用于日志）。
    """
    try:
        params = pika.URLParameters(RABBIT_URL)
        connection = pika.BlockingConnection(params)
        channel = connection.channel()

        channel.exchange_declare(exchange=EXCHANGE, exchange_type=EXCHANGE_TYPE, durable=True)

        channel.basic_publish(
            exchange=EXCHANGE,
            routing_key=ROUTING_KEY,
            body=json.dumps(alarm_message, indent=4),
            properties=pika.BasicProperties(
                content_type='application/json',
                delivery_mode=2,  # 使消息持久化
            )
        )
        # 根据消息类型调整打印信息
        extra = ""
        if alarm_message.get("vff_path"):
            extra = f", vff_path={alarm_message['vff_path']}"
        print(f"✅ 已发送 [{message_type}] 消息: 告警ID {alarm_message['alarm_id']}, 点位 {alarm_message['point']}{extra}")
        connection.close()
    except pika.exceptions.AMQPConnectionError as e:
        print(f"❌ 连接 RabbitMQ 失败: {e}")
        print("请确保 RabbitMQ 服务器正在运行并且网络可达。")
    except Exception as e:
        print(f"❌ 发送消息时发生未知错误: {e}")


def generate_alarm(point_min: int, point_max: int) -> dict:
    """
    生成一个告警消息。
    
    :param point_min: 随机点位的最小值
    :param point_max: 随机点位的最大值
    :return: 告警消息字典
    """
    return {
        "alarm_id": str(uuid.uuid4()),
        "start_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3],
        "end_time": "",
        "point": random.randint(point_min, point_max),
        "alarm_type": "vibration"
    }


def create_vff_message(base_alarm: dict, is_end: bool = False) -> dict:
    """
    创建一个携带 vff_path 的消息。
    
    :param base_alarm: 基础告警消息（包含 alarm_id, point 等）
    :param is_end: 是否为结束告警（如果是，填充 end_time）
    :return: 携带 vff_path 的消息字典
    """
    message = base_alarm.copy()
    if is_end:
        message["end_time"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    if VFF_IMAGE_PATH:
        message["vff_path"] = VFF_IMAGE_PATH
    return message


def send_vff_update(alarms: list, message_type: str):
    """
    为所有告警发送携带 vff_path 的消息。
    
    :param alarms: 告警列表
    :param message_type: 消息类型（用于日志）
    """
    if not VFF_IMAGE_PATH:
        print(f"⚠️  VFF_IMAGE_PATH 未配置，跳过发送 [{message_type}] VFF 消息")
        return
    
    print(f"\n📷 准备发送 [{message_type}] VFF 图片消息 (共 {len(alarms)} 条)...")
    
    threads = []
    for alarm in alarms:
        vff_message = create_vff_message(alarm, is_end=False)
        thread = threading.Thread(target=publish, args=(vff_message, message_type))
        threads.append(thread)
        thread.start()
    
    for thread in threads:
        thread.join()
    
    print(f"✅ 所有 {len(alarms)} 条 [{message_type}] VFF 消息已发送完成")


def simulate_alarm_cycle(num_alarms: int, duration: int, point_min: int, point_max: int):
    """
    模拟一个完整的告警生命周期（同时下发多条告警）。

    1. 同时发送指定数量的"开始告警"（end_time 为空）。
    2. 在持续时间内，定期同时发送"持续中"的告警（内容与开始告警相同）。
    3. 持续时间结束后，同时发送与之前匹配的"结束告警"（填充 end_time）。
    4. 根据 VFF_SEND_TIMING 配置，在适当的时候发送 VFF 图片消息。

    :param num_alarms: 要生成的告警数量。
    :param duration: 告警的模拟持续时间（秒）。
    :param point_min: 随机点位的最小值。
    :param point_max: 随机点位的最大值。
    """
    active_alarms = []

    # --- 阶段 1: 同时发送开始告警 ---
    print(f"\n--- 阶段 1: 准备同时发送 {num_alarms} 条 [开始] 告警 (点位范围: {point_min}-{point_max}) ---")
    
    # 生成所有告警
    for _ in range(num_alarms):
        start_alarm = generate_alarm(point_min, point_max)
        active_alarms.append(start_alarm)
    
    # 使用线程同时发送所有开始告警
    threads = []
    for alarm in active_alarms:
        thread = threading.Thread(target=publish, args=(alarm, "开始"))
        threads.append(thread)
        thread.start()
    
    # 等待所有线程完成
    for thread in threads:
        thread.join()
    
    print(f"✅ 所有 {num_alarms} 条 [开始] 告警已同时发送完成")

    # --- 检查是否需要在开始时刻立即发送 VFF ---
    if VFF_SEND_TIMING in ["before_end", "both"] and VFF_SEND_SECOND == 0:
        send_vff_update(active_alarms, "VFF-开始时刻")

    # --- 阶段 2: 同时发送持续中的告警 ---
    print(f"\n--- 阶段 2: 模拟告警持续 {duration} 秒，每 {UPDATE_INTERVAL_SECONDS} 秒同时发送一次 [持续] 更新 ---")

    # 计算 VFF 发送的时机（秒数，从开始时刻计算）
    vff_send_time = None
    if VFF_SEND_TIMING in ["before_end", "both"]:
        if VFF_SEND_SECOND > 0:
            # 正数：从开始时刻后的第 n 秒
            vff_send_time = VFF_SEND_SECOND
        elif VFF_SEND_SECOND < 0:
            # 负数：结束前的 n 秒
            vff_send_time = duration + VFF_SEND_SECOND
        # VFF_SEND_SECOND == 0 已经在前面处理了

    # 记录 VFF 是否已经发送
    vff_sent_before_end = (VFF_SEND_SECOND == 0 and VFF_SEND_TIMING in ["before_end", "both"])

    if duration >= UPDATE_INTERVAL_SECONDS:
        num_updates = (duration // UPDATE_INTERVAL_SECONDS) - 1
        for i in range(num_updates):
            current_time = (i + 1) * UPDATE_INTERVAL_SECONDS
            time.sleep(UPDATE_INTERVAL_SECONDS)
            print(f"\n🔄 [更新] 第 {i + 1} 次. 当前时间: {current_time}s, 同时为 {len(active_alarms)} 条活动告警发送状态...")
            
            # 使用线程同时发送所有更新
            threads = []
            for alarm in active_alarms:
                thread = threading.Thread(target=publish, args=(alarm, "持续"))
                threads.append(thread)
                thread.start()
            
            # 等待所有线程完成
            for thread in threads:
                thread.join()
            
            print(f"✅ 所有 {len(active_alarms)} 条 [持续] 告警已同时发送完成")
            
            # 检查是否需要在此时发送 VFF
            if VFF_SEND_TIMING in ["before_end", "both"] and vff_send_time is not None:
                if not vff_sent_before_end and current_time >= vff_send_time:
                    send_vff_update(active_alarms, "VFF-持续中")
                    vff_sent_before_end = True

        # 等待最后一个时间间隔
        remaining_time = duration - (num_updates * UPDATE_INTERVAL_SECONDS)
        if remaining_time > 0:
            time.sleep(remaining_time)
            
            # 检查是否在最后的剩余时间内需要发送 VFF
            if VFF_SEND_TIMING in ["before_end", "both"] and vff_send_time is not None:
                if not vff_sent_before_end and duration >= vff_send_time:
                    send_vff_update(active_alarms, "VFF-持续中")
                    vff_sent_before_end = True
    else:  # 如果总持续时间小于更新间隔，直接等待即可
        time.sleep(duration)
        
        # 如果 VFF_SEND_TIMING 是 "before_end" 或 "both"，且 VFF_SEND_SECOND 在持续时间内
        if VFF_SEND_TIMING in ["before_end", "both"] and vff_send_time is not None:
            if not vff_sent_before_end and duration >= vff_send_time:
                send_vff_update(active_alarms, "VFF-持续中")
                vff_sent_before_end = True

    # 如果配置了 before_end 但还没发送（例如 VFF_SEND_SECOND 正好等于 duration），在结束前发送
    if VFF_SEND_TIMING in ["before_end", "both"] and not vff_sent_before_end and VFF_IMAGE_PATH:
        send_vff_update(active_alarms, "VFF-结束前")
        vff_sent_before_end = True

    # --- 阶段 3: 同时发送结束告警 ---
    print(f"\n--- 阶段 3: 持续时间结束，准备同时发送 {len(active_alarms)} 条 [结束] 告警 ---")
    
    # 准备所有结束告警
    end_alarms = []
    for start_alarm in active_alarms:
        end_alarm = start_alarm.copy()
        end_alarm["end_time"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        # 注意：这里不附加 vff_path，因为 VFF 消息是单独发送的
        end_alarms.append(end_alarm)
    
    # 使用线程同时发送所有结束告警
    threads = []
    for alarm in end_alarms:
        thread = threading.Thread(target=publish, args=(alarm, "结束"))
        threads.append(thread)
        thread.start()
    
    # 等待所有线程完成
    for thread in threads:
        thread.join()
    
    print(f"✅ 所有 {len(end_alarms)} 条 [结束] 告警已同时发送完成")
    
    # --- 阶段 4: 发送结束后的 VFF (如果配置了) ---
    if VFF_SEND_TIMING in ["after_end", "both"] and VFF_IMAGE_PATH:
        if VFF_DELAY_AFTER_END > 0:
            print(f"\n⏳ 等待 {VFF_DELAY_AFTER_END} 秒后发送结束后的 VFF 消息...")
            time.sleep(VFF_DELAY_AFTER_END)
        # 使用原始告警信息（不带 end_time）
        send_vff_update(active_alarms, "VFF-结束后")
    
    print("\n--- 告警模拟周期完成 ---")


def main():
    """
    脚本主入口。
    """
    print(f"\n=== VFF 发送配置 ===")
    print(f"  VFF_SEND_TIMING: {VFF_SEND_TIMING}")
    if VFF_SEND_TIMING in ["before_end", "both"]:
        print(f"  VFF_SEND_SECOND: {VFF_SEND_SECOND} (从开始时刻计算，负数表示结束前)")
    if VFF_SEND_TIMING in ["after_end", "both"]:
        print(f"  VFF_DELAY_AFTER_END: {VFF_DELAY_AFTER_END} 秒")
    print("====================\n")
    
    simulate_alarm_cycle(
        num_alarms=NUM_ALARMS_TO_SEND,
        duration=ALARM_DURATION_SECONDS,
        point_min=POINT_RANGE_START,
        point_max=POINT_RANGE_END
    )


if __name__ == "__main__":
    main()