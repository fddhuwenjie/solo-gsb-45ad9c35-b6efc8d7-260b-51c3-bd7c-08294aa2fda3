"""草稿回放报告测试：水位冻结、四分类、幂等、409、并发与游标分页。"""
import os
import sys
import threading

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pytest
from fastapi.testclient import TestClient

from app.main import create_app

# v1：A/B 为同周期同闪光的并列航标（裁决按编号取 A）；
# C 为周期 6 的单闪光航标。
LIBRARY = {
    "aids": [
        {"aid_id": "A", "period": 10.0, "flashes": [0.0, 1.5]},
        {"aid_id": "B", "period": 10.0, "flashes": [0.0, 1.5]},
        {"aid_id": "C", "period": 6.0, "flashes": [0.0]},
    ],
    "mode": "merge",
}

PARAMS = {"tolerance": 0.5, "max_drift": 1e-4, "max_missed": 2, "max_false": 2}


@pytest.fixture()
def client(tmp_path):
    app = create_app(db_path=str(tmp_path / "test.db"))
    with TestClient(app) as c:
        yield c


def judge(client, obs, *, library=None, params=PARAMS):
    body = {"observations": obs, **params}
    if library is not None:
        body["library"] = library
    r = client.post("/judge", json=body)
    assert r.status_code == 200, r.text
    return r.json()


def make_obs(aid="A", psi=1_700_000_000.0, drift=0.0, n_cycles=20, noise=0.01, seed=3):
    import random

    rng = random.Random(seed)
    patterns = {
        "A": (10.0, [0.0, 1.5]),
        "B": (10.0, [0.0, 1.5]),
        "C": (6.0, [0.0]),
        "D": (10.0, [0.0, 3.0]),
    }
    period, flashes = patterns[aid]
    s = 1.0 + drift
    obs = [
        s * (psi + n * period + f) + rng.uniform(-noise, noise)
        for n in range(n_cycles)
        for f in flashes
    ]
    obs.sort()
    return obs


def create_report(client, version_id, key="k"):
    return client.post(
        f"/library/versions/{version_id}/replay-reports",
        json={"idempotency_key": key},
    )


# ---------------- 四分类与证据 ----------------

def test_replay_classifies_all_four_categories(client):
    client.post("/library/import", json=LIBRARY)
    v1 = client.get("/library").json()["library_version"]

    obs_a = make_obs("A")          # 与 A/B 精确匹配
    obs_c = make_obs("C")          # 仅与 C 匹配（B 模式完全不符）
    # 草稿：删除 A、C，仅保留 B（内容不变）→ A 的观测改判 B（winner_changed），
    # C 的观测彻底失去合格候选（match_lost）
    draft_aids = [
        {"aid_id": "B", "period": 10.0, "flashes": [0.0, 1.5]},
    ]

    j_a = judge(client, obs_a)
    assert j_a["best"]["aid_id"] == "A"  # 并列时编号小者胜
    j_c = judge(client, obs_c)
    assert j_c["best"]["aid_id"] == "C"
    # 一条内联库判定（草稿回放时应跳过）
    j_inline = judge(
        client,
        obs_c,
        library=[{"aid_id": "X", "period": 6.0, "flashes": [0.0]}],
    )
    assert j_inline["library_source"] == "inline"

    d = client.post("/library/versions", json={"aids": draft_aids, "base_version": v1}).json()
    assert d["status"] == "draft"

    r = create_report(client, d["version_id"], key="report-1")
    assert r.status_code == 201, r.text
    summary = r.json()
    assert summary["watermark_judgment_id"] == j_inline["judgment_id"]
    assert summary["total_judgments"] == 3
    assert summary["replayed_count"] == 2
    assert summary["skipped_count"] == 1
    assert summary["changes"] == {
        "unchanged": 0,
        "match_gained": 0,
        "match_lost": 1,
        "winner_changed": 1,
    }

    # 被跳过记录：内联库原因
    page = client.get(
        f"/library/versions/{d['version_id']}/replay-reports/"
        f"{summary['report_id']}/changes?include_skipped=true"
    ).json()
    assert page["count"] == 1
    skipped = page["changes"][0]
    assert skipped["judgment_id"] == j_inline["judgment_id"]
    assert skipped["item_status"] == "skipped"
    assert skipped["skip_reason"] == "inline_library"
    assert skipped["old_best_aid_id"] == "X"

    # 变更明细（默认仅 replayed，按原 judgment_id 升序）
    page = client.get(
        f"/library/versions/{d['version_id']}/replay-reports/"
        f"{summary['report_id']}/changes"
    ).json()
    assert page["count"] == 2 and page["next_cursor"] is None
    by_judgment = {item["judgment_id"]: item for item in page["changes"]}

    winner = by_judgment[j_a["judgment_id"]]
    assert winner["classification"] == "winner_changed"
    assert winner["old"]["best_aid_id"] == "A"
    assert winner["new"]["best_aid_id"] == "B"
    assert winner["old"]["best"]["aid_id"] == "A"          # 旧最佳候选证据
    assert winner["new"]["best"]["aid_id"] == "B"          # 新最佳候选证据
    assert winner["old"]["library_version"] == v1

    lost = by_judgment[j_c["judgment_id"]]
    assert lost["classification"] == "match_lost"
    assert lost["old"]["status"] == "matched" and lost["old"]["best_aid_id"] == "C"
    assert lost["new"]["status"] == "no_candidate"
    assert lost["new"]["best"] is None and lost["new"]["reason"]
    assert lost["new"]["best_aid_id"] is None

    # 分类过滤
    page = client.get(
        f"/library/versions/{d['version_id']}/replay-reports/"
        f"{summary['report_id']}/changes?classification=winner_changed"
    ).json()
    assert [x["judgment_id"] for x in page["changes"]] == [j_a["judgment_id"]]


def test_replay_unchanged_and_match_gained(client):
    # v1 只有 A；草稿加入 D（与 C 系列观测匹配的新航标）
    client.post("/library/import", json={
        "aids": [{"aid_id": "A", "period": 10.0, "flashes": [0.0, 1.5]}],
    })
    v1 = client.get("/library").json()["library_version"]

    j_same = judge(client, make_obs("A"))                       # 草稿下仍判 A
    j_gain = judge(client, make_obs("C"), params={
        **PARAMS, "max_missed": 2, "max_false": 2})             # v1 无候选，草稿 D 命中
    assert j_same["best"]["aid_id"] == "A"
    assert j_gain["status"] == "no_candidate"

    draft_aids = [
        {"aid_id": "A", "period": 10.0, "flashes": [0.0, 1.5]},
        {"aid_id": "D", "period": 6.0, "flashes": [0.0]},
    ]
    d = client.post("/library/versions", json={"aids": draft_aids, "base_version": v1}).json()
    r = create_report(client, d["version_id"], key="g1")
    summary = r.json()
    assert summary["changes"] == {
        "unchanged": 1,
        "match_gained": 1,
        "match_lost": 0,
        "winner_changed": 0,
    }

    page = client.get(
        f"/library/versions/{d['version_id']}/replay-reports/"
        f"{summary['report_id']}/changes"
    ).json()
    by_judgment = {item["judgment_id"]: item for item in page["changes"]}
    assert by_judgment[j_same["judgment_id"]]["classification"] == "unchanged"
    gained = by_judgment[j_gain["judgment_id"]]
    assert gained["classification"] == "match_gained"
    assert gained["old"]["status"] == "no_candidate"
    assert gained["old"]["best"] is None
    assert gained["new"]["status"] == "matched"
    assert gained["new"]["best_aid_id"] == "D"


def test_replay_reuses_original_thresholds_and_observations(client):
    # 阈值沿用原记录：原判定用宽松阈值，草稿回放也必须按宽松阈值裁决
    client.post("/library/import", json={
        "aids": [{"aid_id": "A", "period": 10.0, "flashes": [0.0, 1.5]}],
    })
    v1 = client.get("/library").json()["library_version"]
    loose = {"tolerance": 0.6, "max_drift": 1e-4, "max_missed": 4, "max_false": 4}

    # 草稿把 A 的第二个闪光移到 3.0：与原 1.5 偏移 1.5s，按原宽松阈值会漏记大量闪光
    obs = make_obs("A", noise=0.0)
    j = judge(client, obs, params=loose)
    assert j["best"]["aid_id"] == "A" and j["parameters"]["tolerance"] == 0.6

    draft_aids = [{"aid_id": "A", "period": 10.0, "flashes": [0.0, 3.0]}]
    d = client.post("/library/versions", json={"aids": draft_aids, "base_version": v1}).json()
    summary = create_report(client, d["version_id"], key="thr").json()
    # 宽松阈值下：0 偏移的闪光照常匹配，1.5 的全部漏记（20 个），超过 max_missed=4
    item = client.get(
        f"/library/versions/{d['version_id']}/replay-reports/"
        f"{summary['report_id']}/changes"
    ).json()["changes"][0]
    assert item["classification"] == "match_lost"
    assert "missed" in item["new"]["reason"]


# ---------------- 水位：新判定不得混入 ----------------

def test_replay_freezes_watermark_and_never_writes_judgments(client):
    client.post("/library/import", json={
        "aids": [{"aid_id": "A", "period": 10.0, "flashes": [0.0, 1.5]}],
    })
    v1 = client.get("/library").json()["library_version"]
    j1 = judge(client, make_obs("A"))

    draft_aids = [{"aid_id": "D", "period": 6.0, "flashes": [0.0]}]
    d = client.post("/library/versions", json={"aids": draft_aids, "base_version": v1}).json()
    summary = create_report(client, d["version_id"], key="wm").json()
    assert summary["watermark_judgment_id"] == j1["judgment_id"]

    # 回放之后再产生新判定：既有报告的水位与内容不变，新判定不出现在明细中
    j2 = judge(client, make_obs("A"))
    assert j2["judgment_id"] > summary["watermark_judgment_id"]
    page = client.get(
        f"/library/versions/{d['version_id']}/replay-reports/"
        f"{summary['report_id']}/changes"
    ).json()
    ids = [x["judgment_id"] for x in page["changes"]]
    assert ids == [j1["judgment_id"]]

    # 回放没有写入 judgments：最大 id 仍为 j2，列表不含任何回放痕迹
    latest = client.get("/judgments?limit=10").json()["judgments"]
    assert {row["id"] for row in latest} == {j1["judgment_id"], j2["judgment_id"]}


# ---------------- 幂等 / 409 / 并发 ----------------

def test_replay_idempotent_retry_returns_original_report(client):
    client.post("/library/import", json={
        "aids": [{"aid_id": "A", "period": 10.0, "flashes": [0.0, 1.5]}],
    })
    v1 = client.get("/library").json()["library_version"]
    j = judge(client, make_obs("A"))
    draft_aids = [{"aid_id": "D", "period": 6.0, "flashes": [0.0]}]
    d = client.post("/library/versions", json={"aids": draft_aids, "base_version": v1}).json()
    url = f"/library/versions/{d['version_id']}/replay-reports"

    r1 = client.post(url, json={"idempotency_key": "same-key"})
    assert r1.status_code == 201
    first = r1.json()

    r2 = client.post(url, json={"idempotency_key": "same-key"})
    assert r2.status_code == 200
    again = r2.json()
    assert again == first  # 返回原报告（编号、水位、计数、创建时刻全部不变）

    # 多次重试仍然只有一份报告与一份明细
    r3 = client.post(url, json={"idempotency_key": "same-key"})
    assert r3.status_code == 200 and r3.json()["report_id"] == first["report_id"]
    page = client.get(
        f"/library/versions/{d['version_id']}/replay-reports/"
        f"{first['report_id']}/changes"
    ).json()
    assert [x["judgment_id"] for x in page["changes"]] == [j["judgment_id"]]


def test_replay_same_key_other_version_conflicts(client):
    client.post("/library/import", json={
        "aids": [{"aid_id": "A", "period": 10.0, "flashes": [0.0, 1.5]}],
    })
    v1 = client.get("/library").json()["library_version"]
    judge(client, make_obs("A"))

    d1 = client.post("/library/versions", json={
        "aids": [{"aid_id": "D", "period": 6.0, "flashes": [0.0]}],
        "base_version": v1,
    }).json()
    d2 = client.post("/library/versions", json={
        "aids": [{"aid_id": "E", "period": 8.0, "flashes": [0.0]}],
        "base_version": v1,
    }).json()

    r1 = create_report(client, d1["version_id"], key="shared")
    assert r1.status_code == 201
    report_id = r1.json()["report_id"]

    # 同一键用于另一版本 → 409，detail 指明占用方
    r2 = create_report(client, d2["version_id"], key="shared")
    assert r2.status_code == 409
    detail = r2.json()["detail"]
    assert detail["error"] == "replay_conflict"
    assert detail["reason"] == "idempotency_key_in_use"
    assert detail["occupied_by_version"] == d1["version_id"]
    assert detail["occupied_by_report"] == report_id

    # 被拒版本没有生成任何报告
    assert client.get(
        f"/library/versions/{d2['version_id']}/replay-reports/{report_id}"
    ).status_code == 404


def test_replay_concurrent_same_key_creates_single_report(client):
    client.post("/library/import", json={
        "aids": [{"aid_id": "A", "period": 10.0, "flashes": [0.0, 1.5]}],
    })
    v1 = client.get("/library").json()["library_version"]
    judge(client, make_obs("A"))
    d = client.post("/library/versions", json={
        "aids": [{"aid_id": "D", "period": 6.0, "flashes": [0.0]}],
        "base_version": v1,
    }).json()

    results: list[int] = []
    barrier = threading.Barrier(6)

    def worker():
        c = TestClient(create_app_with_same_db(client))
        barrier.wait()
        r = c.post(
            f"/library/versions/{d['version_id']}/replay-reports",
            json={"idempotency_key": "race-key"},
        )
        results.append(r.status_code)

    def create_app_with_same_db(existing_client):
        return existing_client.app  # 同一 app/数据库；TestClient 各开连接

    threads = [threading.Thread(target=worker) for _ in range(6)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    # 恰好一个 201，其余 200；无 4xx/5xx
    assert sorted(results).count(201) == 1
    assert sorted(results).count(200) == 5

    import sqlite3

    db_path = client.app.state.db._path
    with sqlite3.connect(db_path) as conn:
        n = conn.execute(
            "SELECT COUNT(*) FROM replay_reports WHERE idempotency_key = 'race-key'"
        ).fetchone()[0]
        items = conn.execute(
            "SELECT COUNT(*) FROM replay_report_items"
        ).fetchone()[0]
    assert n == 1 and items == 1


def test_replay_concurrent_across_separate_db_handles_unique_constraint(tmp_path):
    """多连接场景（各自的进程锁不互斥）：UNIQUE 约束兜底仍只生成一份报告。"""
    from app.db import Database
    from app.replay import replay_records
    from fastapi.testclient import TestClient

    db_path = str(tmp_path / "test.db")
    app = create_app(db_path=db_path)
    with TestClient(app) as c:
        c.post("/library/import", json={
            "aids": [{"aid_id": "A", "period": 10.0, "flashes": [0.0, 1.5]}],
        })
        v1 = c.get("/library").json()["library_version"]
        judge(c, make_obs("A"))
        d = c.post("/library/versions", json={
            "aids": [{"aid_id": "D", "period": 6.0, "flashes": [0.0]}],
            "base_version": v1,
        }).json()
        vid = d["version_id"]

    # 两个互不共享线程锁的 Database 实例，模拟多进程各自抢写锁
    db_a = Database(db_path)
    db_b = Database(db_path)

    def runner(aids, records):
        return replay_records(aids, records, grid_max_steps=201, seed_top_k=5)

    outcomes: list = []
    barrier = threading.Barrier(2)

    def create(db):
        barrier.wait()
        try:
            outcomes.append(db.create_replay_report(vid, "cross-key", runner))
        except Exception as exc:  # noqa: BLE001
            outcomes.append(("error", exc))

    t1 = threading.Thread(target=create, args=(db_a,))
    t2 = threading.Thread(target=create, args=(db_b,))
    t1.start(); t2.start(); t1.join(); t2.join()

    created = [o for o in outcomes if o not in ("error",) and o[1]]
    retried = [o for o in outcomes if o not in ("error",) and not o[1]]
    assert len(created) == 1 and len(retried) == 1
    assert created[0][0]["report_id"] == retried[0][0]["report_id"]

    import sqlite3

    with sqlite3.connect(db_path) as conn:
        n = conn.execute(
            "SELECT COUNT(*) FROM replay_reports WHERE idempotency_key = 'cross-key'"
        ).fetchone()[0]
    assert n == 1


# ---------------- 游标分页 ----------------

def test_replay_changes_cursor_pagination(client):
    client.post("/library/import", json={
        "aids": [
            {"aid_id": "A", "period": 10.0, "flashes": [0.0, 1.5]},
            {"aid_id": "C", "period": 6.0, "flashes": [0.0]},
        ],
    })
    v1 = client.get("/library").json()["library_version"]
    j1 = judge(client, make_obs("A"))
    j2 = judge(client, make_obs("C"))
    j3 = judge(client, make_obs("A"))  # 草稿删除 A → match_lost；C 保持 → unchanged

    draft_aids = [{"aid_id": "C", "period": 6.0, "flashes": [0.0]}]
    d = client.post("/library/versions", json={"aids": draft_aids, "base_version": v1}).json()
    summary = create_report(client, d["version_id"], key="page").json()
    base = f"/library/versions/{d['version_id']}/replay-reports/{summary['report_id']}/changes"

    page1 = client.get(base + "?limit=1").json()
    assert page1["count"] == 1 and page1["next_cursor"] == j1["judgment_id"]
    page2 = client.get(
        base + f"?limit=1&after_judgment_id={page1['next_cursor']}"
    ).json()
    assert [x["judgment_id"] for x in page2["changes"]] == [j2["judgment_id"]]
    page3 = client.get(
        base + f"?limit=1&after_judgment_id={page2['next_cursor']}"
    ).json()
    assert [x["judgment_id"] for x in page3["changes"]] == [j3["judgment_id"]]
    assert page3["next_cursor"] is None

    # 过滤 + 分页组合
    lost = client.get(base + "?classification=match_lost").json()
    assert [x["judgment_id"] for x in lost["changes"]] == [
        j1["judgment_id"],
        j3["judgment_id"],
    ]

    # 摘要直查
    got = client.get(
        f"/library/versions/{d['version_id']}/replay-reports/{summary['report_id']}"
    )
    assert got.status_code == 200 and got.json()["report_id"] == summary["report_id"]


# ---------------- 错误处理 ----------------

def test_replay_error_cases(client):
    client.post("/library/import", json={
        "aids": [{"aid_id": "A", "period": 10.0, "flashes": [0.0, 1.5]}],
    })
    v1 = client.get("/library").json()["library_version"]
    judge(client, make_obs("A"))

    # 版本不存在 → 404
    r = client.post("/library/versions/999/replay-reports", json={"idempotency_key": "x"})
    assert r.status_code == 404

    # 非草稿（激活版本）→ 400
    r = client.post(f"/library/versions/{v1}/replay-reports", json={"idempotency_key": "x"})
    assert r.status_code == 400

    # idempotency_key 缺失或纯空白 → 422
    r = client.post(f"/library/versions/{v1}/replay-reports", json={})
    assert r.status_code == 422
    r = client.post(f"/library/versions/{v1}/replay-reports", json={"idempotency_key": "   "})
    assert r.status_code == 422

    # 草稿上正常创建一份，再查不存在的报告 → 404；非法分类 → 422；
    # classification 与 include_skipped 同用 → 400
    d = client.post("/library/versions", json={
        "aids": [{"aid_id": "D", "period": 6.0, "flashes": [0.0]}],
        "base_version": v1,
    }).json()
    created = create_report(client, d["version_id"], key="ok").json()
    base = f"/library/versions/{d['version_id']}/replay-reports/{created['report_id']}"
    assert client.get(base + "/9999").status_code == 404
    assert client.get(base + "/changes?classification=bogus").status_code == 422
    assert client.get(
        base + "/changes?classification=unchanged&include_skipped=true"
    ).status_code == 400

    # 报告不属于该版本时 → 404（换一个新版本查询别人的报告）
    d2 = client.post("/library/versions", json={
        "aids": [{"aid_id": "E", "period": 8.0, "flashes": [0.0]}],
        "base_version": v1,
    }).json()
    assert client.get(
        f"/library/versions/{d2['version_id']}/replay-reports/{created['report_id']}"
    ).status_code == 404


def test_replay_on_empty_judgment_history(client):
    # 没有任何判定记录也能出报告：水位 0，各项计数为 0，明细为空
    client.post("/library/import", json={
        "aids": [{"aid_id": "A", "period": 10.0, "flashes": [0.0, 1.5]}],
    })
    v1 = client.get("/library").json()["library_version"]
    d = client.post("/library/versions", json={
        "aids": [{"aid_id": "D", "period": 6.0, "flashes": [0.0]}],
        "base_version": v1,
    }).json()
    r = create_report(client, d["version_id"], key="empty")
    assert r.status_code == 201
    summary = r.json()
    assert summary["watermark_judgment_id"] == 0
    assert summary["total_judgments"] == 0
    assert summary["replayed_count"] == 0 and summary["skipped_count"] == 0
    base = f"/library/versions/{d['version_id']}/replay-reports/{summary['report_id']}"
    assert client.get(base + "/changes").json()["changes"] == []
    assert client.get(base + "/changes?include_skipped=true").json()["changes"] == []
