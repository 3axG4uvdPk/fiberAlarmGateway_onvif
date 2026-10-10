# -*- coding: utf-8 -*-
"""
看门狗子进程。
监控主应用心跳文件，超时后向 systemd 报告失败。
"""
import os
import sys
import time
import argparse
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - [Watchdog] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger("watchdog")


def main():
    parser = argparse.ArgumentParser(description="FiberAlarmGateway Watchdog")
    parser.add_argument("--heartbeat-file", type=str, required=True)
    parser.add_argument("--pid", type=int, required=True)
    parser.add_argument("--timeout", type=int, default=5)
    args = parser.parse_args()

    heartbeat_file = args.heartbeat_file
    parent_pid = args.pid
    timeout = args.timeout

    logger.info(f"看门狗启动，监控 PID={parent_pid}，超时={timeout}s，心跳文件={heartbeat_file}")

    # 通知 systemd 看门狗已就绪
    notify_socket = os.environ.get('NOTIFY_SOCKET')
    if notify_socket:
        try:
            import socket
            sock = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
            sock.connect(notify_socket)
            sock.sendall(b"READY=1\nWATCHDOG=1")
            sock.close()
            logger.info("已向 systemd 发送 READY 信号。")
        except Exception as e:
            logger.warning(f"向 systemd 发送 READY 信号失败: {e}")

    while True:
        try:
            # 检查主进程是否存在
            try:
                os.kill(parent_pid, 0)
            except OSError:
                logger.critical(f"主进程 PID={parent_pid} 已不存在，看门狗退出。")
                sys.exit(1)

            # 检查心跳文件时间戳
            try:
                last_mod = os.path.getmtime(heartbeat_file)
                elapsed = time.time() - last_mod
                if elapsed > timeout:
                    logger.critical(
                        f"主应用心跳超时！最后更新 {elapsed:.1f} 秒前（超时阈值 {timeout}s）。"
                    )
                    sys.exit(1)
                else:
                    logger.debug(f"心跳正常，距上次更新 {elapsed:.1f}s")
            except FileNotFoundError:
                logger.critical(f"心跳文件 {heartbeat_file} 不存在！")
                sys.exit(1)

            time.sleep(2)

        except KeyboardInterrupt:
            logger.info("看门狗收到退出信号。")
            break
        except Exception as e:
            logger.error(f"看门狗发生未知错误: {e}", exc_info=True)
            time.sleep(2)


if __name__ == "__main__":
    main()
