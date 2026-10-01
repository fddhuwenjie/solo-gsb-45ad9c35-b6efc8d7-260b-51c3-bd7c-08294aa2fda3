"""现场设备观测批次：规范化载荷与链式摘要计算。

每个设备维护一条批次链：``sequence`` 从 1 递增，``previous_digest`` 引用
上一批摘要（首批为空串）。服务端对**排除摘要字段**的批次载荷计算 SHA-256
作为批次摘要——摘要是载荷的唯一指纹，重放/变更判定即比较摘要。

规范化规则（与客户端约定一致，可独立复算）：

- 载荷字段：``batch_id``、``device_id``、``sequence``、``observations``
  （``captured_at``/``local_id``）与 ``/judge`` 四项阈值
  （``tolerance``/``max_drift``/``max_missed``/``max_false``）；
  ``previous_digest`` 等摘要字段不参与；
- JSON 序列化：键按 UTF-8 序排序（``sort_keys``）、紧凑分隔符
  （``,``/``:``，无空白）、UTF-8 编码（``ensure_ascii=False``）；
- 摘要：对上述字节串取 SHA-256，十六进制小写。
"""
from __future__ import annotations

import hashlib
import json


def canonical_payload(device_id: str, req) -> dict:
    """批次的规范化载荷（摘要输入）：排除 previous_digest 等摘要字段。"""
    return {
        "batch_id": req.batch_id,
        "device_id": device_id,
        "max_drift": req.max_drift,
        "max_false": req.max_false,
        "max_missed": req.max_missed,
        "observations": [
            {"captured_at": o.captured_at, "local_id": o.local_id}
            for o in req.observations
        ],
        "sequence": req.sequence,
        "tolerance": req.tolerance,
    }


def compute_digest(payload: dict) -> str:
    """UTF-8 键序 + 紧凑分隔符的 JSON 上计算 SHA-256（十六进制小写）。"""
    blob = json.dumps(
        payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")
    return hashlib.sha256(blob).hexdigest()


def merge_batch_observations(batch_rows: list[dict]) -> list[dict]:
    """把连续批次的明细观测合并为一次判定的观测序列。

    入参 ``batch_rows`` 每项为 ``{sequence, observations}``，其中
    ``observations`` 为 ``[{captured_at, local_id}, …]``（批内已按
    ``(captured_at, local_id)`` 严格递增）。合并以
    ``(captured_at, sequence, local_id)`` 为全序键跨批排序：不同批次同一时刻
    的观测以序号区分先后，同一批内保持采集顺序。完全相同的三元组只保留一条
    （防御性去重：链式追加的批次间 local_id 不应重复）。

    返回 ``[{captured_at, local_id, sequence}, …]``：判定只取时间戳，
    来源三元组随判定来源一起保存，归档后仍可复核每条观测来自哪一批。
    """
    triples: list[tuple[float, int, str]] = []
    for row in batch_rows:
        for o in row["observations"]:
            triples.append((o["captured_at"], row["sequence"], o["local_id"]))
    triples = sorted(set(triples))
    return [
        {"captured_at": t, "sequence": seq, "local_id": local_id}
        for t, seq, local_id in triples
    ]
