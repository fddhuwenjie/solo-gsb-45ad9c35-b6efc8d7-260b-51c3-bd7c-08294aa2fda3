"""连续批次归档前巡检判定测试。

覆盖：区间校验（序号连续/缺批次/墓碑/四项阈值不一致均整体拒绝且不落判定）、
按 (captured_at, sequence, local_id) 合并观测、复用 /judge 规则并保存
judgment_id 与各批摘要/library_version 等不可变来源、请求开始固定激活版本、
幂等键重试 200 / 改范围或换设备 409、与归档及上传并发不引用半个范围、
归档后已保存来源仍可查询，以及结果列表/详情查询与底层 judgments 记录。
"""
import os
import sys
import threading

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pytest
from fastapi.testclient import TestClient

from app.main import create_app

DEVICE = "dev-01"
THRESHOLDS = {"tolerance": 0.5, "max_drift": 5e-4, "max_missed": 3, "max_false": 3}
# A：周期 10、双闪；C：周期 6、单闪（与 test_replay 一致的合成约定）
LIBRARY = {
    "aids": [
        {"aid_id": "A", "period": 10.0, "flashes": [0.0, 1.5]},
        {"aid_id": "C", "period": 6.0, "flashes": [0.0]},
    ],
    "mode": "merge",
}
PSI = 1_700_000_000.0


@pytest.fixture()
def client(tmp_path):
    app = create_app(db_path=str(tmp_path / "test.db"))
    with TestClient(app) as c:
        yield c


def make_flashes(seq_per_batch, *, aid="A", psi=PSI, noise=0.01, seed=7):
    """按 aid 灯质生成闪光观测（无漂移、无漏误记），再切成若干连续批次。

    返回每个批次的 observations 列表（local_id 全局唯一，
    captured_at 严格递增）。
    """
    import random

    rng = random.Random(seed)
    period, flashes = {"A": (10.0, [0.0, 1.5]), "C": (6.0, [0.0])}[aid]
    timestamps = [
        psi + n * period + f + rng.uniform(-noise, noise)
        for n in range(sum(seq_per_batch))
        for f in flashes
    ]
    batches, idx, lid = [], 0, 0
    for s, n_cycles in enumerate(seq_per_batch, start=1):
        obs = []
        for _ in range(n_cycles * len(flashes)):
            obs.append({"local_id": f"o{lid:04d}", "captured_at": timestamps[idx]})
            idx += 1
            lid += 1
        batches.append(obs)
    return batches


def upload_chain(client, device, batches, *, thresholds_by_seq=None, base=THRESHOLDS):
    """上传给定观测批次序列，返回 [(sequence, batch_id, digest, count), …]。"""
    out, prev = [], ""
    for i, observations in enumerate(batches, start=1):
        th = (thresholds_by_seq or {}).get(i, base)
        r = client.post(
            f"/field-devices/{device}/batches",
            json={
                "batch_id": f"b-{i}",
                "sequence": i,
                "previous_digest": prev,
                "observations": observations,
                **th,
            },
        )
        assert r.status_code == 201, r.text
        d = r.json()
        out.append((i, f"b-{i}", d["digest"], len(observations)))
        prev = d["digest"]
    return out


def prepare(client, device=DEVICE, seq_per_batch=(4, 3, 3), **kw):
    client.post("/library/import", json=LIBRARY)
    return upload_chain(client, device, make_flashes(seq_per_batch, **kw))


def create_judgment(client, device, key, frm, to):
    return client.post(
        f"/field-devices/{device}/batch-judgments",
        json={"idempotency_key": key, "from_sequence": frm, "to_sequence": to},
    )


def register_ack_archive(client, device, digests, through):
    client.post(f"/field-devices/{device}/consumers", json={"consumer_id": "arch"})
    for seq, digest in enumerate(digests, start=1):
        r = client.post(
            f"/field-devices/{device}/acknowledgments",
            json={"consumer_id": "arch", "sequence": seq, "batch_digest": digest},
        )
        assert r.status_code in (200, 201), r.text
    r = client.post(f"/field-devices/{device}/archive", json={"through_sequence": through})
    assert r.status_code == 201, r.text


# ---------------- 创建与不可变来源 ----------------


def test_create_batch_judgment_merges_and_judges(client):
    meta = prepare(client)
    r = create_judgment(client, DEVICE, "k1", 1, 3)
    assert r.status_code == 201, r.text
    d = r.json()

    assert d["batch_judgment_id"] == 1
    assert d["idempotency_key"] == "k1"
    assert d["device_id"] == DEVICE
    assert (d["from_sequence"], d["to_sequence"]) == (1, 3)
    assert d["library_version"] == 1
    assert d["observation_count"] == sum(m[3] for m in meta)
    assert d["thresholds"] == THRESHOLDS
    assert d["source_judgment_id"] == d["judgment"]["judgment_id"] == 1

    # 判定复用 /judge 规则：合并观测与 A 精确匹配
    j = d["judgment"]
    assert j["status"] == "matched"
    assert j["best"]["aid_id"] == "A"
    assert j["library_source"] == "stored"
    assert j["library_version"] == 1
    assert j["best"]["matched"] == d["observation_count"]

    # 各批摘要作为不可变来源保存：序号、batch_id、前序/本批摘要、条数
    assert d["batches"] == [
        {
            "sequence": seq,
            "batch_id": bid,
            "previous_digest": "" if seq == 1 else meta[seq - 2][2],
            "digest": digest,
            "observation_count": count,
        }
        for seq, bid, digest, count in meta
    ]

    # 合并观测按 (captured_at, sequence, local_id) 排序并带来源序号
    obs = d["observations"]
    assert len(obs) == d["observation_count"]
    keys = [(o["captured_at"], o["sequence"], o["local_id"]) for o in obs]
    assert keys == sorted(keys)
    assert {o["sequence"] for o in obs} == {1, 2, 3}


def test_judgment_uses_merged_timestamps_same_as_direct_judge(client):
    prepare(client)
    r = create_judgment(client, DEVICE, "k1", 1, 3)
    merged_ts = [o["captured_at"] for o in r.json()["observations"]]
    direct = client.post("/judge", json={"observations": merged_ts, **THRESHOLDS})
    assert direct.status_code == 200
    assert direct.json()["best"]["aid_id"] == r.json()["judgment"]["best"]["aid_id"]
    assert direct.json()["best"]["matched"] == r.json()["judgment"]["best"]["matched"]


def test_observation_merge_order_with_equal_timestamps(client):
    """不同批次同一 captured_at 时以 sequence 区分先后；批内保持采集顺序。"""
    from app.batches import merge_batch_observations

    rows = [
        {"sequence": 2, "observations": [
            {"captured_at": 10.0, "local_id": "z"},
            {"captured_at": 12.0, "local_id": "a"},
        ]},
        {"sequence": 1, "observations": [
            {"captured_at": 10.0, "local_id": "q"},
            {"captured_at": 11.0, "local_id": "r"},
        ]},
    ]
    merged = merge_batch_observations(rows)
    assert [(o["captured_at"], o["sequence"], o["local_id"]) for o in merged] == [
        (10.0, 1, "q"),
        (10.0, 2, "z"),
        (11.0, 1, "r"),
        (12.0, 2, "a"),
    ]

    # 完全相同三元组只保留一条（防御性去重）
    dup = merge_batch_observations([
        {"sequence": 1, "observations": [{"captured_at": 1.0, "local_id": "x"}]},
        {"sequence": 1, "observations": [{"captured_at": 1.0, "local_id": "x"}]},
    ])
    assert dup == [{"captured_at": 1.0, "sequence": 1, "local_id": "x"}]


# ---------------- 幂等 ----------------


def test_same_idempotency_key_retry_returns_original(client):
    prepare(client)
    r1 = create_judgment(client, DEVICE, "k1", 1, 2)
    assert r1.status_code == 201
    first = r1.json()

    r2 = create_judgment(client, DEVICE, "k1", 1, 2)
    assert r2.status_code == 200
    again = r2.json()
    assert again["batch_judgment_id"] == first["batch_judgment_id"]
    assert again["source_judgment_id"] == first["source_judgment_id"]
    assert again["created_at"] == first["created_at"]
    assert again["batches"] == first["batches"]
    assert again["judgment"] == first["judgment"]


def test_same_key_different_range_conflicts(client):
    prepare(client)
    assert create_judgment(client, DEVICE, "k1", 1, 2).status_code == 201
    r = create_judgment(client, DEVICE, "k1", 2, 3)
    assert r.status_code == 409
    d = r.json()["detail"]
    assert d["error"] == "batch_judgment_conflict"
    assert d["reason"] == "idempotency_key_in_use"
    assert d["occupied_device_id"] == DEVICE
    assert (d["occupied_from_sequence"], d["occupied_to_sequence"]) == (1, 2)
    assert d["occupied_by_batch_judgment_id"] == 1


def test_same_key_different_device_conflicts(client):
    prepare(client)
    assert create_judgment(client, DEVICE, "k1", 1, 2).status_code == 201
    r = create_judgment(client, "dev-02", "k1", 1, 2)
    assert r.status_code == 409
    assert r.json()["detail"]["occupied_device_id"] == DEVICE


def test_concurrent_same_key_single_judgment(client):
    prepare(client)
    barrier = threading.Barrier(4)
    results, lock = [], threading.Lock()

    def worker():
        barrier.wait()
        r = create_judgment(client, DEVICE, "race-key", 1, 3)
        with lock:
            results.append((r.status_code, r.json()["batch_judgment_id"]))

    threads = [threading.Thread(target=worker) for _ in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    statuses = {s for s, _ in results}
    assert statuses <= {200, 201}
    assert 201 in statuses
    assert {bid for _, bid in results} == {1}


# ---------------- 区间拒绝（整体不落判定） ----------------


def test_missing_batch_in_range_rejected(client):
    prepare(client)  # 只有 seq 1..3
    r = create_judgment(client, DEVICE, "k1", 1, 4)
    assert r.status_code == 409
    d = r.json()["detail"]
    assert d["reason"] == "range_not_continuous"
    assert d["missing_sequences"] == [4]
    assert d["head_sequence"] == 3
    assert client.get("/judgments?limit=50").json()["count"] == 0


def test_gap_inside_range_rejected(client):
    # 设备无任何批次时从 1 起即缺（head_sequence=0），整体拒绝
    r = create_judgment(client, "dev-empty", "k1", 1, 1)
    assert r.status_code == 409
    d = r.json()["detail"]
    assert d["reason"] == "range_not_continuous"
    assert d["missing_sequences"] == [1]
    assert d["head_sequence"] == 0
    assert client.get("/judgments?limit=50").json()["count"] == 0


def test_archived_tombstone_in_range_rejected(client):
    batches = make_flashes((2, 2, 2))
    meta = upload_chain(client, DEVICE, batches)
    register_ack_archive(client, DEVICE, [m[2] for m in meta], through=1)

    r = create_judgment(client, DEVICE, "k1", 1, 2)
    assert r.status_code == 409
    d = r.json()["detail"]
    assert d["reason"] == "range_archived"
    assert d["archived_sequences"] == [1]
    assert client.get("/judgments?limit=50").json()["count"] == 0

    # 完全落在未归档区间仍可判定；明细不受先前拒绝影响
    ok = create_judgment(client, DEVICE, "k2", 2, 3)
    assert ok.status_code == 201
    assert ok.json()["observation_count"] == 8


def test_thresholds_must_match_across_batches(client):
    batches = make_flashes((2, 2))
    upload_chain(
        client,
        DEVICE,
        batches,
        thresholds_by_seq={2: {**THRESHOLDS, "tolerance": THRESHOLDS["tolerance"] + 0.1}},
    )
    r = create_judgment(client, DEVICE, "k1", 1, 2)
    assert r.status_code == 409
    d = r.json()["detail"]
    assert d["reason"] == "thresholds_mismatch"
    assert d["reference_sequence"] == 1
    assert d["mismatched_sequences"] == [2]
    assert client.get("/judgments?limit=50").json()["count"] == 0

    # 单一批次（无从比较）仍可判定
    assert create_judgment(client, DEVICE, "k2", 2, 2).status_code == 201


def test_from_greater_than_to_is_422(client):
    prepare(client)
    r = create_judgment(client, DEVICE, "k1", 3, 1)
    assert r.status_code == 422


# ---------------- 激活版本固定 ----------------


def test_library_version_pinned_at_request_start(client):
    meta = prepare(client)
    v1 = client.get("/library").json()["library_version"]
    r = create_judgment(client, DEVICE, "k1", 1, 3)
    assert r.status_code == 201
    assert r.json()["library_version"] == v1

    # 激活新版本后再判定：新判定固定新版本，旧判定仍指向旧版本
    client.post(
        "/library/import",
        json={"aids": [{"aid_id": "X", "period": 3.0, "flashes": [0.0]}], "mode": "replace"},
    )
    v2 = client.get("/library").json()["library_version"]
    assert v2 != v1
    r2 = create_judgment(client, DEVICE, "k2", 1, 3)
    assert r2.status_code == 201
    assert r2.json()["library_version"] == v2
    old = client.get(
        f"/field-devices/{DEVICE}/batch-judgments/{r.json()['batch_judgment_id']}"
    ).json()
    assert old["library_version"] == v1
    assert old["judgment"]["library_version"] == v1


def test_no_active_library_returns_no_candidate_but_persists(client):
    upload_chain(client, DEVICE, make_flashes((2, 2)))
    r = create_judgment(client, DEVICE, "k1", 1, 2)
    assert r.status_code == 201
    d = r.json()
    assert d["library_version"] is None
    assert d["judgment"]["status"] == "no_candidate"
    assert d["judgment"]["best"] is None


# ---------------- 与归档/上传并发：不引用半个范围 ----------------


def test_concurrent_archive_either_full_judgment_or_rejection(client):
    batches = make_flashes((3, 3))
    meta = upload_chain(client, DEVICE, batches)
    register_ack_archive(client, DEVICE, [m[2] for m in meta], through=1)
    # 归档水位已到 1；再登记一个尚未确认的消费者，使 seq2 暂时不能归档。
    # 两个线程：判定 1..2 vs 归档到 2（先补确认再归档，制造与判定的真实竞争）。
    client.post(f"/field-devices/{DEVICE}/consumers", json={"consumer_id": "late"})
    barrier = threading.Barrier(2)
    outcomes = []
    lock = threading.Lock()

    def do_judge():
        barrier.wait()
        r = create_judgment(client, DEVICE, "kj", 1, 2)
        with lock:
            outcomes.append(("judge", r.status_code, r.json()))

    def do_archive():
        # 让 late 消费者也确认 seq1、seq2，再尝试归档到 2
        for seq, m in enumerate(meta, start=1):
            client.post(
                f"/field-devices/{DEVICE}/acknowledgments",
                json={"consumer_id": "late", "sequence": seq, "batch_digest": m[2]},
            )
        barrier.wait()
        r = client.post(
            f"/field-devices/{DEVICE}/archive", json={"through_sequence": 2}
        )
        with lock:
            outcomes.append(("archive", r.status_code, r.json()))

    threads = [threading.Thread(target=do_judge), threading.Thread(target=do_archive)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    judge_outcome = next(o for o in outcomes if o[0] == "judge")
    archive_outcome = next(o for o in outcomes if o[0] == "archive")
    assert archive_outcome[1] in (200, 201)
    if judge_outcome[1] == 201:
        # 判定先于归档：保存的来源必须覆盖完整范围，两条批次摘要都在、
        # observation_count 为两批之和
        d = judge_outcome[2]
        assert len(d["batches"]) == 2
        assert d["observation_count"] == sum(m[3] for m in meta)
        assert d["observations"] and all(o["sequence"] in (1, 2) for o in d["observations"])
    else:
        # 归档先于判定：seq1 已成墓碑 → 整体拒绝，不落判定
        assert judge_outcome[1] == 409
        assert judge_outcome[2]["detail"]["reason"] == "range_archived"

    # 无论谁先完成，已保存判定（若有）引用的观测条数恒等于其各批条数之和，
    # 绝不会是半个范围
    listed = client.get(f"/field-devices/{DEVICE}/batch-judgments").json()["batch_judgments"]
    for item in listed:
        full = client.get(
            f"/field-devices/{DEVICE}/batch-judgments/{item['batch_judgment_id']}"
        ).json()
        assert full["observation_count"] == sum(b["observation_count"] for b in full["batches"])


def test_concurrent_upload_cannot_half_fill_range(client):
    # 链上仅有 seq1：判定 1..2 与上传 seq2 并发。
    upload_chain(client, DEVICE, make_flashes((2,)))
    seq2 = make_flashes((0, 2))[1]
    barrier = threading.Barrier(2)
    results = []
    lock = threading.Lock()

    def do_judge():
        barrier.wait()
        r = create_judgment(client, DEVICE, "ku", 1, 2)
        with lock:
            results.append(("judge", r.status_code, r.json()))

    def do_upload():
        d1 = client.get(f"/field-devices/{DEVICE}/cursor").json()["head_digest"]
        barrier.wait()
        r = client.post(
            f"/field-devices/{DEVICE}/batches",
            json={
                "batch_id": "b-2", "sequence": 2, "previous_digest": d1,
                "observations": seq2, **THRESHOLDS,
            },
        )
        with lock:
            results.append(("upload", r.status_code, r.json()))

    threads = [threading.Thread(target=do_judge), threading.Thread(target=do_upload)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    judge_outcome = next(o for o in results if o[0] == "judge")
    if judge_outcome[1] == 201:
        # 上传先提交：判定必须覆盖完整两批（不是只看到 seq1 的半个范围）
        d = judge_outcome[2]
        assert (d["from_sequence"], d["to_sequence"]) == (1, 2)
        assert [b["sequence"] for b in d["batches"]] == [1, 2]
        assert d["observation_count"] == sum(b["observation_count"] for b in d["batches"])
    else:
        assert judge_outcome[1] == 409
        assert judge_outcome[2]["detail"]["reason"] == "range_not_continuous"


# ---------------- 归档后已保存来源不变 ----------------


def test_saved_judgment_survives_archiving_its_batches(client):
    meta = prepare(client)
    r = create_judgment(client, DEVICE, "k1", 1, 3)
    assert r.status_code == 201
    bid = r.json()["batch_judgment_id"]
    saved_observations = r.json()["observations"]
    saved_batches = r.json()["batches"]

    register_ack_archive(client, DEVICE, [m[2] for m in meta], through=3)
    # 批次明细已成墓碑
    assert client.get(f"/field-devices/{DEVICE}/batches/1").json()["archived"] is True

    full = client.get(f"/field-devices/{DEVICE}/batch-judgments/{bid}")
    assert full.status_code == 200
    d = full.json()
    assert d["observations"] == saved_observations
    assert d["batches"] == saved_batches
    assert d["judgment"]["status"] == "matched"
    # 底层判定记录仍可按 judgment_id 回溯
    j = client.get(f"/judgments/{d['source_judgment_id']}")
    assert j.status_code == 200
    assert j.json()["response"]["best"]["aid_id"] == "A"


# ---------------- 查询 ----------------


def test_list_and_detail_queries(client):
    prepare(client)
    create_judgment(client, DEVICE, "k1", 1, 2)
    create_judgment(client, DEVICE, "k2", 2, 3)

    page = client.get(f"/field-devices/{DEVICE}/batch-judgments")
    assert page.status_code == 200
    items = page.json()["batch_judgments"]
    assert [i["batch_judgment_id"] for i in items] == [1, 2]
    # 列表为摘要：不含各批摘要数组与判定证据
    assert "batches" not in items[0]
    assert "judgment" not in items[0]
    assert items[0]["from_sequence"] == 1 and items[0]["to_sequence"] == 2
    assert items[0]["source_judgment_id"] == 1
    assert items[0]["thresholds"] == THRESHOLDS

    # 游标分页
    p1 = client.get(f"/field-devices/{DEVICE}/batch-judgments?limit=1").json()
    assert p1["count"] == 1 and p1["next_cursor"] == 1
    p2 = client.get(
        f"/field-devices/{DEVICE}/batch-judgments?limit=1&after_id=1"
    ).json()
    assert [i["batch_judgment_id"] for i in p2["batch_judgments"]] == [2]
    assert p2["next_cursor"] is None

    # 详情 404：不存在 / 属于其他设备
    assert client.get(f"/field-devices/{DEVICE}/batch-judgments/999").status_code == 404
    assert client.get(f"/field-devices/dev-02/batch-judgments/1").status_code == 404


def test_rejected_requests_do_not_persist(client):
    prepare(client)
    create_judgment(client, DEVICE, "k-bad", 1, 9)  # 缺批次 → 409
    assert client.get(f"/field-devices/{DEVICE}/batch-judgments").json()["count"] == 0
    # 也没有底层 judgments 行
    assert client.get("/judgments?limit=50").json()["count"] == 0
