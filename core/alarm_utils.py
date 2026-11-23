# -*- coding: utf-8 -*-
"""
报警消息处理公共工具。

``device_id`` / ``channel`` 在配置中作为字符串键使用，上游可能以整数或浮点数上报，
需要统一规范化后才能与配置比对。
"""


def normalize_alarm_key(value):
    """把设备ID / 通道号规范化为配置键使用的字符串形式。

    - ``None`` -> ``''``；
    - 整型按十进制字符串处理（``1`` -> ``'1'``）；
    - 兼容上游以浮点数上报的整数（``1.0`` -> ``'1'``）；
    - 其余值去除首尾空白后原样返回。
    """
    if value is None:
        return ''
    if isinstance(value, bool):
        return str(int(value))
    if isinstance(value, int):
        return str(value)
    text = str(value).strip()
    # 容忍 1.0 / 2.0 这类浮点写法，避免与配置键 "1" 匹配不上
    if text.endswith('.0'):
        head = text[:-2]
        if head.isdigit() or (head.startswith('-') and head[1:].isdigit()):
            return head
    return text
