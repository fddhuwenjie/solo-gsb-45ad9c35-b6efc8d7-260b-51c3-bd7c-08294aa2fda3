"""草稿特征库回放：用草稿快照重放历史判定并分类变更。

回放只针对**使用服务端特征库**的历史判定（请求未携带内联 ``library``）：
沿用原观测与四项阈值（``tolerance`` / ``max_drift`` / ``max_missed`` /
``max_false``），把判定特征库整体替换为草稿快照，复用与 ``/judge`` 完全相同
的现有判定算法（:func:`app.align.judge`），不引入任何旁路逻辑。

新旧最佳候选按以下四类分类（穷尽）：

- ``unchanged``：裁决结果未变——新旧均无合格候选，或最佳候选仍为同一航标；
- ``match_gained``：旧判定无合格候选（``no_candidate``），草稿下得到匹配；
- ``match_lost``：旧判定有匹配，草稿下变为无合格候选；
- ``winner_changed``：新旧均有匹配，但最佳航标编号不同。

回放是纯计算：不写入 ``judgments``，也不读取或修改任何既有判定记录。
"""
from __future__ import annotations

from .align import AidPattern, judge

# 四类变更标签（同时作为 HTTP 查询参数的合法取值）
CLASSIFICATIONS = ("unchanged", "match_gained", "match_lost", "winner_changed")

# 内联库记录的跳过原因
SKIP_INLINE_LIBRARY = "inline_library"
# 历史载荷缺少回放必需字段（观测/阈值）时的防御性跳过原因
SKIP_MISSING_INPUT = "missing_replay_input"

REPLAY_THRESHOLD_FIELDS = ("tolerance", "max_drift", "max_missed", "max_false")


def is_inline_judgment(request: dict) -> bool:
    """判定记录是否使用了内联特征库（请求显式携带非空 library）。"""
    library = request.get("library")
    return library is not None


def has_replay_input(request: dict) -> bool:
    """历史请求载荷是否包含回放所需的观测与四项阈值。"""
    if "observations" not in request:
        return False
    return all(field in request for field in REPLAY_THRESHOLD_FIELDS)


def classify_change(
    old_status: str | None,
    old_best: dict | None,
    new_status: str | None,
    new_best: dict | None,
) -> str:
    """按新旧裁决结果给出四分类。

    两侧均无合格候选、或两侧最佳航标编号相同 → ``unchanged``；
    无 → 有 为 ``match_gained``；有 → 无 为 ``match_lost``；
    两侧均有但航标不同为 ``winner_changed``。
    """
    old_matched = old_status == "matched" and old_best is not None
    new_matched = new_status == "matched" and new_best is not None
    if old_matched and new_matched:
        if old_best.get("aid_id") == new_best.get("aid_id"):
            return "unchanged"
        return "winner_changed"
    if not old_matched and new_matched:
        return "match_gained"
    if old_matched and not new_matched:
        return "match_lost"
    return "unchanged"


def replay_records(
    draft_aids: list[dict],
    records: list[dict],
    *,
    grid_max_steps: int,
    seed_top_k: int,
) -> list[dict]:
    """对一批历史判定记录执行草稿快照回放。

    ``draft_aids`` 为草稿版本的完整快照（``aid_id`` / ``period`` /
    ``flashes``），``records`` 每项含 ``judgment_id`` 与原始 ``request``。
    返回顺序与入参一致，每项为 ``{judgment_id, status, reason, best}``，
    其中 ``best`` 为新裁决最佳候选证据（无合格候选时为 ``None``）。
    """
    patterns = [
        AidPattern(a["aid_id"], a["period"], tuple(a["flashes"])) for a in draft_aids
    ]
    results: list[dict] = []
    for rec in records:
        req = rec["request"]
        best, _candidates, reason = judge(
            patterns,
            req["observations"],
            tolerance=req["tolerance"],
            max_drift=req["max_drift"],
            max_missed=req["max_missed"],
            max_false=req["max_false"],
            grid_max_steps=grid_max_steps,
            seed_top_k=seed_top_k,
        )
        results.append(
            {
                "judgment_id": rec["judgment_id"],
                "status": "matched" if best is not None else "no_candidate",
                "reason": reason,
                "best": best.to_detail() if best is not None else None,
            }
        )
    return results
