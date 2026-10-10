# 三家厂商私有 RTSP 回放协议对照说明

> 适用场景：通过 RTSP 拉取历史录像（Playback），时间语义各家不同，**最容易踩坑的是「UTC vs 本地时间」**。

***

## 一、总览对照表

| 项目       | 宇视 Uniview              | 大华 Dahua                                         | 海康 Hikvision             |
| -------- | ----------------------- | ------------------------------------------------ | ------------------------ |
| URL 路径   | `/media/record<N>`      | `/cam/playback`                                  | `/Streaming/tracks/<ID>` |
| 通道/码流表达  | `record<N>` 路径段         | `channel` + `subtype` 查询参数                       | `tracksID`（通道号+码流号拼接）    |
| 时间参数名    | `begintime` / `endtime` | `starttime` / `endtime`                          | `starttime` / `endtime`  |
| 时间格式     | `YYYYMMDDTHHMMSSZ`      | `YYYY_MM_DD_HH_MM_SS`                            | `YYYYMMDDThhmmssZ`       |
| **时间语义** | **UTC**                 | **本地时间（非 UTC）**                                  | **UTC**                  |
| 参数顺序要求   | 无严格限制                   | **必须 channel→subtype→starttime→endtime**         | 无严格限制                    |
| 额外参数     | 无                       | **禁止携带任何多余参数**（`type=`/`playback=`/`em=` 都会 404） | 无                        |
| 分隔符转义    | `?` 后建议整体加引号            | `&` 必须加引号，否则被 shell 截断                           | 同左                       |

***

## 二、宇视 Uniview

### 2.1 URL 模板

```
rtsp://<user>:<pass>@<ip>:554/media/record<N>?begintime=YYYYMMDDTHHMMSSZ&endtime=YYYYMMDDTHHMMSSZ
```

### 2.2 参数说明

| 参数          | 含义            | 取值                       |
| ----------- | ------------- | ------------------------ |
| `record<N>` | 码流编号          | `1`=主码流，`2`=子码流，`3`=第三码流 |
| `begintime` | 起始时间（**UTC**） | ISO8601 紧凑格式，末尾 `Z`      |
| `endtime`   | 结束时间（**UTC**） | 同上                       |

### 2.3 关键坑点：时间是 UTC

- 实测：写入 `20260922T165950Z`，回放画面 OSD 显示 `2026-09-23 00:59:51`（北京时间）
- 说明：**URL 参数按 UTC 解析，OSD 显示本地时间**
- **转换规则**：想取本地 `16:59:50` 的录像 → 参数必须写 `08:59:50`
  ```
  本地时间 - 8 小时 = UTC 参数
  ```
- 中国时区固定 UTC+8，无需处理夏令时

### 2.4 示例

取**北京时间 2026-09-29 11:19:50 \~ 11:20:10** 的主码流录像：

```
本地 11:19:50  →  UTC 03:19:50
本地 11:20:10  →  UTC 03:20:10
```

```bash
ffmpeg -nostdin -rtsp_transport tcp -timeout 5000000 \
  -i "rtsp://admin:admin1234.@172.16.61.13:554/media/record1?begintime=20260929T031950Z&endtime=20260929T032010Z" \
  -t 20 -c copy -an -y \
  /data/likai/onvif/downloads/cam1_uniview_172_16_61_13_20260929_112000.mp4
```

***

## 三、大华 Dahua

### 3.1 URL 模板

```
rtsp://<user>:<pass>@<ip>:554/cam/playback?channel=<C>&subtype=<S>&starttime=YYYY_MM_DD_HH_MM_SS&endtime=YYYY_MM_DD_HH_MM_SS
```

### 3.2 参数说明

| 参数          | 含义             | 取值                    |
| ----------- | -------------- | --------------------- |
| `channel`   | 通道号            | 从 `1` 开始              |
| `subtype`   | 码流类型           | `0`=主码流，`1`=子码流       |
| `starttime` | 起始时间（**本地时间**） | `YYYY_MM_DD_HH_MM_SS` |
| `endtime`   | 结束时间（**本地时间**） | 同上                    |

### 3.3 关键坑点

1. **时间是本地时间，不是 UTC**
   - 实测机型：`DH-SD-6C3220T-HN`，固件 `2.400.0000.9.R`（2016 年固件）
   - 请求 `2026_09_23_08_00_00`（本地 08:00，已经过去）能成功
   - 若按 UTC 解析，该时刻相当于本地 16:00（未来），设备对未来的时间必返 404
   - **结论**：直接填本地时间，**不要减 8 小时**
2. **参数顺序和数量必须严格**
   - 顺序：`channel` → `subtype` → `starttime` → `endtime`
   - **不能多带任何参数**（`type=`、`playback=`、`em=` 等一律 404）
3. **等价写法**
   - `2026-09-23T08:00:00Z` 也能识别，但 **`Z`** **会被忽略**（仍按本地时间解析）
   - 也就是说这条 URL 里的 `Z` **不代表 UTC**，跟海康/宇视完全不同
4. **shell 转义**
   - `&` 必须放在引号里，否则被 bash 当作后台执行符号截断

### 3.4 示例

取**北京时间 2026-09-29 11:19:50 \~ 11:20:10** 的主码流录像：

```
本地 11:19:50  →  参数 2026_09_29_11_19_50
本地 11:20:10  →  参数 2026_09_29_11_20_10
```

```bash
ffmpeg -nostdin -rtsp_transport tcp -timeout 5000000 \
  -i "rtsp://admin:HHkj1994@172.16.61.109:554/cam/playback?channel=1&subtype=0&starttime=2026_09_29_11_19_50&endtime=2026_09_29_11_20_10" \
  -t 20 -c copy -an -y \
  /data/likai/onvif/downloads/cam2_dahua_172_16_61_109_20260929_112000.mp4
```

***

## 四、海康 Hikvision

### 4.1 URL 模板

```
rtsp://<user>:<pass>@<ip>:554/Streaming/tracks/<tracksID>?starttime=YYYYMMDDThhmmssZ&endtime=YYYYMMDDThhmmssZ
```

### 4.2 参数说明

| 参数           | 含义            | 取值             |
| ------------ | ------------- | -------------- |
| `<tracksID>` | 通道号 + 码流号拼接   | 见下表            |
| `starttime`  | 起始时间（**UTC**） | ISO8601，末尾 `Z` |
| `endtime`    | 结束时间（**UTC**） | 同上             |

### 4.3 tracksID 编号规则

| 设备类型         | 通道/码流         | tracksID              |
| ------------ | ------------- | --------------------- |
| IPC（单通道）     | 通道 1 主/子/第三码流 | `101` / `102` / `103` |
| 模拟+IP 混合 DVR | IP 通道         | 从 `17xx` 起跳号          |
| NVR（老 URL）   | 通道            | 从 `33` 起              |

**IPC 单通道**：固定用 `101`（主）/ `102`（子）/ `103`（第三）。

### 4.4 关键坑点

1. **时间是 UTC**（官方文档明确：带 `Z` 即 GMT 时间）
   - 跟宇视一样，**本地时间 - 8 小时**
2. **格式为 ISO 8601**：`YYYYMMDD"T"hhmmss"Z"`
   - 官方文档大小写都出现，`T/Z` 与 `t/z` 都能识别
3. **shell 转义**：`&` 必须加引号

### 4.5 示例

取**北京时间 2026-09-29 11:19:50 \~ 11:20:10** 的通道 1 主码流：

```
本地 11:19:50  →  UTC 03:19:50
本地 11:20:10  →  UTC 03:20:10
```

```bash
ffmpeg -nostdin -rtsp_transport tcp -timeout 5000000 \
  -i "rtsp://admin:Hhkj1994@172.16.61.64:554/Streaming/tracks/101?starttime=20260929T031950Z&endtime=20260929T032010Z" \
  -t 20 -c copy -an -y \
  /data/likai/onvif/downloads/cam3_hikvision_172_16_61_64_20260929_112000.mp4
```

***

## 五、时间语义速查（最容易出错的一栏）

| 厂商 | 参数里的时间   | 举例：要取本地 `2026-09-29 11:19:50`   |
| -- | -------- | ------------------------------- |
| 宇视 | **UTC**  | 写 `20260929T031950Z`（减 8h）      |
| 大华 | **本地时间** | 写 `2026_09_29_11_19_50`（**不减**） |
| 海康 | **UTC**  | 写 `20260929T031950Z`（减 8h）      |

> 记忆口诀：**「宇海减八，大华照搬」**

***

## 六、通用 ffmpeg 使用建议

```bash
ffmpeg -nostdin \
  -rtsp_transport tcp \
  -timeout 5000000 \
  -i "<RTSP_URL>" \
  -t <时长秒> \
  -c copy \
  -an \
  -y \
  "<输出.mp4>"
```

| 参数                    | 作用             | 备注                            |
| --------------------- | -------------- | ----------------------------- |
| `-nostdin`            | 禁用标准输入交互       | 后台脚本必加                        |
| `-rtsp_transport tcp` | 强制 TCP 传流      | 防丢包                           |
| `-timeout 5000000`    | RTSP 超时（微秒，5s） | **新版 ffmpeg**；旧版用 `-stimeout` |
| `-t <秒>`              | 拉流时长           | 精确度受关键帧限制，非精确切                |
| `-c copy`             | 流复制，不重编码       | 快，但切割点在关键帧                    |
| `-an`                 | 丢弃音频           | 回放流一般无音频                      |
| `-y`                  | 覆盖输出           | —                             |

### 补充建议

1. **URL 一定整体加双引号**：`"rtsp://...&...&..."`，否则 `&` 被 shell 截断。
2. **`-c copy`** **切不准**：只能在关键帧切，实际时长可能 ±2\~3s。需要精确到帧就删 `-c copy` 重编码。
3. **时间戳异常**：回放流时间戳往往不从 0 开始，可加 `-avoid_negative_ts make_zero` 或 `-fflags +genpts`。
4. **输出目录需预先存在**：ffmpeg 不会自动 `mkdir`。
5. **`-timeout`** **vs** **`-stimeout`**：报 `Option timeout not found` 时换 `-stimeout`。

***

## 七、三家 URL 拼接函数（伪代码参考）

```python
from datetime import datetime, timedelta

def uniview_url(user, pwd, ip, stream, t_start, t_end):
    # stream: 1=主, 2=子, 3=第三
    # 本地 -> UTC -8h
    b = (t_start - timedelta(hours=8)).strftime("%Y%m%dT%H%M%SZ")
    e = (t_end   - timedelta(hours=8)).strftime("%Y%m%dT%H%M%SZ")
    return f"rtsp://{user}:{pwd}@{ip}:554/media/record{stream}?begintime={b}&endtime={e}"

def dahua_url(user, pwd, ip, channel, subtype, t_start, t_end):
    # 本地时间，不减 8h
    b = t_start.strftime("%Y_%m_%d_%H_%M_%S")
    e = t_end.strftime("%Y_%m_%d_%H_%M_%S")
    return (f"rtsp://{user}:{pwd}@{ip}:554/cam/playback"
            f"?channel={channel}&subtype={subtype}&starttime={b}&endtime={e}")

def hikvision_url(user, pwd, ip, tracks_id, t_start, t_end):
    # 本地 -> UTC -8h
    b = (t_start - timedelta(hours=8)).strftime("%Y%m%dT%H%M%SZ")
    e = (t_end   - timedelta(hours=8)).strftime("%Y%m%dT%H%M%SZ")
    return (f"rtsp://{user}:{pwd}@{ip}:554/Streaming/tracks/{tracks_id}"
            f"?starttime={b}&endtime={e}")
```

***

