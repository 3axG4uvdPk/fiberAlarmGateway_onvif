# -*- coding: utf-8 -*-
"""
状态信息线程。
包括：
- StatusReceiver: 应变光纤状态消息 (das.data)
- SolarReceiver: 电池电量消息 (das.solar)
"""
import json
import time
import threading
import logging
import pika

logger = logging.getLogger("alarm_app.messaging")


def _build_mq_connection_params(rabbit_url):
    """构建 RabbitMQ 连接参数。"""
    url_params = pika.URLParameters(rabbit_url)
    return pika.ConnectionParameters(
        host=url_params.host,
        port=url_params.port,
        virtual_host=url_params.virtual_host,
        credentials=url_params.credentials,
        heartbeat=30,
        blocked_connection_timeout=300
    )


class StatusReceiver(threading.Thread):
    """应变光纤状态信息接收线程。"""

    def __init__(self, config, system_status, heartbeat_worker):
        super().__init__(daemon=True, name="StatusReceiver")
        self.heartbeat_worker = heartbeat_worker
        self.system_status = system_status

        mq_config = config.get('rabbitmq_status_in', {})
        if not mq_config:
            logger.error("配置文件中缺少 'rabbitmq_status_in' 配置节！状态接收功能将禁用。")
            self._is_running = False
            return

        self.rabbit_url = mq_config['url']
        self.exchange_in = mq_config['exchange_in']
        self.exchange_type = mq_config['exchange_type']
        self.durable = mq_config['durable']
        self.routing_key = mq_config.get('routing_key', '')
        self.queue_name = mq_config.get('queue_name', '')

        self._is_running = True
        self.conn = None
        self.ch = None

    def stop(self):
        self._is_running = False
        if self.ch and self.ch.is_open:
            try:
                self.conn.add_callback_threadsafe(self.ch.stop_consuming)
            except Exception as e:
                logger.warning(f"请求停止状态MQ消费时出错: {e}")

    def run(self):
        if not self._is_running:
            return
        logger.info("状态信息接收线程已启动。")
        while self._is_running:
            try:
                self.conn = pika.BlockingConnection(_build_mq_connection_params(self.rabbit_url))
                self.ch = self.conn.channel()
                self.ch.exchange_declare(
                    exchange=self.exchange_in,
                    exchange_type=self.exchange_type,
                    durable=self.durable
                )
                result = self.ch.queue_declare(
                    queue=self.queue_name or '',
                    exclusive=not self.queue_name
                )
                queue_to_consume = result.method.queue
                self.ch.queue_bind(
                    queue=queue_to_consume,
                    exchange=self.exchange_in,
                    routing_key=self.routing_key
                )
                logger.info(f"成功连接，正在监听状态交换机 '{self.exchange_in}'。")
                self.ch.basic_consume(
                    queue=queue_to_consume,
                    on_message_callback=self._on_message,
                    auto_ack=True
                )
                self.ch.start_consuming()
            except pika.exceptions.AMQPConnectionError as e:
                logger.error(f"无法连接到状态信息MQ: {e}。将在10秒后重试...")
                time.sleep(10)
            except Exception as e:
                logger.critical(f"状态信息接收线程发生未知错误: {e}", exc_info=True)
                break
        logger.info("状态信息接收线程已停止。")

    def _on_message(self, ch, method, props, body):
        try:
            state_data = json.loads(body)
            state_str = state_data.get("state")
            if state_str:
                self.system_status.update_lidar_status(state_str)
                self.heartbeat_worker.send_heartbeat_now()
            else:
                logger.warning(f"收到的状态信息中缺少 'state' 字段")
        except json.JSONDecodeError:
            logger.error(f"收到的状态信息不是有效 JSON: {body.decode()}")
        except Exception as e:
            logger.error(f"处理状态信息时发生错误: {e}", exc_info=True)


class SolarReceiver(threading.Thread):
    """电池电量信息接收线程 (das.solar)。"""

    def __init__(self, config, system_status, heartbeat_worker):
        super().__init__(daemon=True, name="SolarReceiver")
        self.heartbeat_worker = heartbeat_worker
        self.system_status = system_status

        mq_config = config.get('rabbitmq_solar_in', {})
        if not mq_config:
            logger.error("配置文件中缺少 'rabbitmq_solar_in' 配置节！电池信息接收功能将禁用。")
            self._is_running = False
            return

        self.rabbit_url = mq_config['url']
        self.exchange_in = mq_config['exchange_in']
        self.exchange_type = mq_config['exchange_type']
        self.durable = mq_config['durable']
        self.routing_key = mq_config.get('routing_key', '')
        self.queue_name = mq_config.get('queue_name', '')

        self._is_running = True
        self.conn = None
        self.ch = None

    def stop(self):
        self._is_running = False
        if self.ch and self.ch.is_open:
            try:
                self.conn.add_callback_threadsafe(self.ch.stop_consuming)
            except Exception as e:
                logger.warning(f"请求停止Solar MQ消费时出错: {e}")

    def run(self):
        if not self._is_running:
            return
        logger.info("电池信息接收线程已启动。")
        while self._is_running:
            try:
                self.conn = pika.BlockingConnection(_build_mq_connection_params(self.rabbit_url))
                self.ch = self.conn.channel()
                self.ch.exchange_declare(
                    exchange=self.exchange_in,
                    exchange_type=self.exchange_type,
                    durable=self.durable
                )
                result = self.ch.queue_declare(
                    queue=self.queue_name or '',
                    exclusive=not self.queue_name
                )
                queue_to_consume = result.method.queue
                self.ch.queue_bind(
                    queue=queue_to_consume,
                    exchange=self.exchange_in,
                    routing_key=self.routing_key
                )
                logger.info(f"成功连接，正在监听电池交换机 '{self.exchange_in}'。")
                self.ch.basic_consume(
                    queue=queue_to_consume,
                    on_message_callback=self._on_message,
                    auto_ack=True
                )
                self.ch.start_consuming()
            except pika.exceptions.AMQPConnectionError as e:
                logger.error(f"无法连接到电池信息MQ: {e}。将在10秒后重试...")
                time.sleep(10)
            except Exception as e:
                logger.critical(f"电池信息接收线程发生未知错误: {e}", exc_info=True)
                break
        logger.info("电池信息接收线程已停止。")

    def _on_message(self, ch, method, props, body):
        try:
            data = json.loads(body)
            battery_val = data.get("battery_percentage")
            battery_status = data.get("battery_status")
            if battery_val is not None:
                self.system_status.update_battery_status(battery_val, battery_status)
                self.heartbeat_worker.send_heartbeat_now()
            else:
                logger.warning(f"收到的电池信息中缺少 'battery_percentage' 字段")
        except json.JSONDecodeError:
            logger.error(f"收到的电池信息不是有效 JSON")
        except Exception as e:
            logger.error(f"处理电池信息时发生错误: {e}", exc_info=True)
