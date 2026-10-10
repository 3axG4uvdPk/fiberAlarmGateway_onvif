# -*- coding: utf-8 -*-
"""
RabbitMQ 报警接收线程。
负责从 das.alarm 交换机接收报警消息并投递给处理队列。
"""
import json
import time
import threading
import logging
import queue
import pika

logger = logging.getLogger("alarm_app.messaging.alarm_receiver")


class AlarmReceiver(threading.Thread):
    """MQ 报警接收线程，负责消费报警并把任务放入处理队列。"""

    def __init__(self, config, processing_queue: queue.Queue, processing_worker=None):
        super().__init__(daemon=True, name="AlarmReceiver")
        self.config = config
        self.processing_queue = processing_queue
        self.processing_worker = processing_worker

        mq_cfg = config['rabbitmq_in']
        self.rabbit_url = mq_cfg['url']
        self.exchange_in = mq_cfg['exchange_in']
        self.exchange_type = mq_cfg['exchange_type']
        self.durable = mq_cfg['durable']
        self.routing_key = mq_cfg.get('routing_key', '')
        self.queue_name = mq_cfg.get('queue_name', '')

        self._is_running = True
        self.conn = None
        self.ch = None

    def stop(self):
        self._is_running = False
        if self.conn and self.ch and self.ch.is_open:
            try:
                self.conn.add_callback_threadsafe(self.ch.stop_consuming)
            except Exception as e:
                logger.warning(f"停止 MQ 异常: {e}")

    def _close_connection(self):
        """安全关闭通道与连接。"""
        if self.ch and self.ch.is_open:
            try:
                self.ch.close()
            except Exception:
                pass
        self.ch = None
        if self.conn and self.conn.is_open:
            try:
                self.conn.close()
            except Exception:
                pass
        self.conn = None

    def run(self):
        logger.info("MQ 接收线程已启动。")
        while self._is_running:
            self._close_connection()
            try:
                logger.info("正在连接 RabbitMQ...")
                url_params = pika.URLParameters(self.rabbit_url)
                params = pika.ConnectionParameters(
                    host=url_params.host,
                    port=url_params.port,
                    virtual_host=url_params.virtual_host,
                    credentials=url_params.credentials,
                    heartbeat=30,
                    blocked_connection_timeout=300
                )
                self.conn = pika.BlockingConnection(params)
                self.ch = self.conn.channel()
                self.ch.exchange_declare(
                    exchange=self.exchange_in,
                    exchange_type=self.exchange_type,
                    durable=self.durable
                )

                if self.queue_name:
                    self.ch.queue_declare(queue=self.queue_name, durable=self.durable)
                    queue_to_consume = self.queue_name
                else:
                    result = self.ch.queue_declare(queue='', exclusive=True)
                    queue_to_consume = result.method.queue

                self.ch.queue_bind(
                    queue=queue_to_consume,
                    exchange=self.exchange_in,
                    routing_key=self.routing_key
                )
                logger.info(f"已连接到 RabbitMQ 并监听交换机 '{self.exchange_in}'。")
                self.ch.basic_consume(
                    queue=queue_to_consume,
                    on_message_callback=self._on_message,
                    auto_ack=True
                )
                self.ch.start_consuming()

            except pika.exceptions.ConnectionClosedByBroker:
                logger.warning("RabbitMQ 连接被服务端关闭，准备重新连接...")
                continue
            except pika.exceptions.AMQPConnectionError as e:
                logger.error(f"连接 RabbitMQ 失败: {e}，等待 10 秒后重试...")
                time.sleep(10)
            except Exception as e:
                logger.error(f"MQ 接收线程发生异常，等待 5 秒后重试: {e}", exc_info=True)
                time.sleep(5)
        logger.info("MQ 接收线程已停止。")

    def _on_message(self, ch, method, props, body):
        try:
            alarm = json.loads(body)
            logger.debug(f"--- 从RabbitMQ接收到新消息 ---{alarm}")
            alarm_id = alarm.get('alarm_id')
            is_end_alarm = bool(alarm.get('end_time'))
            has_vff = bool(alarm.get('vff_path'))
            if (
                not is_end_alarm
                and not has_vff
                and alarm_id
                and self.processing_worker is not None
                and self.processing_worker.has_active_alarm_context(alarm_id)
            ):
                logger.debug(f"接收线程：忽略重复的开始报警消息:{alarm_id}")
                return
            # 提前投递 vff 路径到缓存，供开始报警登记等待时原子消费，
            # 避免 vff 排在慢速抓图任务之后、在等待超时前未被拼接而丢失。
            if has_vff and not is_end_alarm and alarm_id and self.processing_worker is not None:
                self.processing_worker.deliver_vff_early(alarm_id, alarm['vff_path'])
            # 记录 MQ 消息接收时间，用于计算 alarm_send_delay_seconds 延迟转发。
            alarm['_received_at'] = time.time()
            self.processing_queue.put(alarm)
        except json.JSONDecodeError:
            logger.error(f"收到无法解析的 JSON: {body}")