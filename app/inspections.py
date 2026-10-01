"""连续批次巡检判定：跨批合并观测，并复用与 ``/judge`` 完全相同的判定规则。

归档前对现场设备上传的一段**连续批次**做一次可复核的整体判定：每个批次保存的
四项阈值（``tolerance`` / ``max_drift`` / ``max_missed`` / ``max_false``）必须
完全一致，观测按 ``(captured_at, sequence, local_id)`` 全局有序合并为一条观测
时间戳序列，特征库使用请求开始时固定的当前激活版本快照，判定算法直接复用
:func:`app.align.judge`，不引入任何旁路逻辑。

本模块只做纯计算与合并：序号连续性、墓碑、阈值一致性等校验以及结果落库都在
``app.db`` 的单个写事务内完成。
"""
from __future__ import annotations

from .align import AidPattern, judge


def merge_observations(batches: list[dict]) -> list[tuple[float, int, str]]:
    """把多批观测按 ``(captured_at, sequence, local_id)`` 升序稳定合并。

    ``batches`` 按 sequence 升序给出，每批含 ``sequence`` 与
    ``observations``（``captured_at`` / ``local_id``，批内已按
    ``(captured_at, local_id)`` 严格递增）。不同批次间允许出现相同的
    ``captured_at``（以 sequence、local_id 次序排列），不去重——同一观测若
    真被两批携带，会作为两个时间戳进入判定，与显式提交 ``/judge`` 的行为一致。
    """
    merged: list[tuple[float, int, str]] = []
    for batch in batches:
        seq = batch["sequence"]
        for observation in batch["observations"]:
            merged.append(
                (observation["captured_at"], seq, observation["local_id"])
            )
    merged.sort(key=lambda item: (item[0], item[1], item[2]))
    return merged


def run_inspection(
    stored_aids: list[dict],
    observations: list[float],
    thresholds: dict,
    *,
    grid_max_steps: int,
    seed_top_k: int,
) -> dict:
    """对合并后的观测序列执行与 ``/judge`` 相同的判定。

    ``stored_aids`` 为请求开始时固定的激活特征库版本完整快照，``thresholds``
    取各批一致的四项阈值。返回 ``{status, reason, best, candidates}``，其中
    ``best`` 为最佳候选证据（无合格候选时为 ``None``）。
    """
    patterns = [
        AidPattern(a["aid_id"], a["period"], tuple(a["flashes"]))
        for a in stored_aids
    ]
    best, candidates, reason = judge(
        patterns,
        observations,
        tolerance=thresholds["tolerance"],
        max_drift=thresholds["max_drift"],
        max_missed=thresholds["max_missed"],
        max_false=thresholds["max_false"],
        grid_max_steps=grid_max_steps,
        seed_top_k=seed_top_k,
    )
    return {
        "status": "matched" if best is not None else "no_candidate",
        "reason": reason,
        "best": best.to_detail() if best is not None else None,
        "candidates": [c.to_summary() for c in candidates],
    }
