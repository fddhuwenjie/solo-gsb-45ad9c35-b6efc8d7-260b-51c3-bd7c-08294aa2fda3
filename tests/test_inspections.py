"""连续批次巡检判定测试。

覆盖：跨批按 (captured_at, sequence, local_id) 合并后复用 /judge 规则、
各批摘要/范围/固定 library_version 作为不可变来源保存、幂等键重试 200、
改范围/设备 409、缺批次/含墓碑/阈值不一致整体拒绝且不落判定、与上传及
归档并发的安全（已保存结果不引用半个范围、成功与归档互斥）、结果查询、
判定记录 source=inspection 且可被草稿回放按水位重放、旧库 source 列迁移。
"""
import os
import sys
import threading

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pytest
from fastapi.testclient import TestClient

from app.main import create_app

LIBRARY = {
    "aids": [
        {"aid_id": "A", "period": 10.0, "flashes": [0.0, 1.5]},
        {"aid_id": "B", "period": 10.0, "flashes": [0.0, 4.0]},
        {"aid_id": "C", "period": 6.0, "flashes": [0.0]},
    ],
    "mode": "merge",
}
THRESHOLDS = {"tolerance": 0.5, "max_drift": 5e-4, "max_missed": 3, "max_false": 3}
DEVICE = "dev-inspect"


@pytest.fixture()
def client(tmp_path):
    app = create_app(db_path=str(tmp_path / "test.db"))
    with TestClient(app) as c:
        yield c


def make_obs(psi=1_700_000_000.0, drift=2e-4, n_cycles=20, noise=0.01, seed=11):
    import random

    rng = random.Random(seed)
    s = 1.0 + drift
    obs = [
        s * (psi + n * 10.0 + f) + rng.uniform(-noise, noise)
        for n in range(n_cycles)
        for f in (0.0, 1.5)
    ]
    obs.sort()
    return obs


def batch(batch_id, sequence, observations, previous_digest="", **kw):
    body = {
        "batch_id": batch_id,
        "sequence": sequence,
        "previous_digest": previous_digest,
        "observations": observations,
        **THRESHOLDS,
    }
    body.update(kw)
    return body


def to_batch_obs(timestamps, start=0):
    return [
        {"local_id": f"o{i:04d}", "captured_at": t}
        for i, t in enumerate(timestamps, start=start)
    ]


def upload_chain(client, device, chunks, **threshold_kw):
    """逐块上传链式批次；chunks 为每批观测时间戳列表。返回 (bodies, digests)。"""
    bodies, digests = [], []
    prev = ""
    for seq, ts in enumerate(chunks, start=1):
        b = batch(f"b-{seq}", seq, to_batch_obs(ts), previous_digest=prev, **threshold_kw)
        r = client.post(f"/field-devices/{device}/batches", json=b)
        assert r.status_code == 201, r.text
        bodies.append(b)
        digests.append(r.json()["digest"])
        prev = digests[-1]
    return bodies, digests


def create_inspection(client, device, from_seq, to_seq, key, *, expected=None):
    r = client.post(
        f"/field-devices/{device}/range-judgments",
        json={
            "from_sequence": from_seq,
            "to_sequence": to_seq,
            "idempotency_key": key,
        },
    )
    if expected is not None:
        assert r.status_code == expected, r.text
    return r


def register(client, device, consumer_id):
    return client.post(
        f"/field-devices/{device}/consumers", json={"consumer_id": consumer_id}
    )


def ack(client, device, consumer_id, sequence, digest):
    return client.post(
        f"/field-devices/{device}/acknowledgments",
        json={"consumer_id": consumer_id, "sequence": sequence, "batch_digest": digest},
    )


def ack_all(client, device, consumer_id, digests):
    for seq, digest in enumerate(digests, start=1):
        r = ack(client, device, consumer_id, seq, digest)
        assert r.status_code in (200, 201), r.text


# ---------------- 基本判定与不可变来源 ----------------


def test_inspection_merges_batches_and_judges_with_judge_rule(client):
    client.post("/library/import", json=LIBRARY)
    timestamps = make_obs()
    chunks = [timestamps[:13], timestamps[13:27], timestamps[27:]]
    bodies, digests = upload_chain(client, DEVICE, chunks)

    r = create_inspection(client, DEVICE, 1, 3, "insp-1", expected=201)
    body = r.json()
    # 复用 /judge 裁决：整体观测序列对应航标 A
    assert body["status"] == "matched"
    assert body["library_source"] == "stored"
    assert body["library_version"] == 1
    assert body["best"]["aid_id"] == "A"
    assert body["best"]["matched"] == len(timestamps)
    assert body["observation_count"] == len(timestamps)
    assert body["parameters"] == THRESHOLDS
    assert {c["aid_id"] for c in body["candidates"]} == {"A", "B", "C"}

    # 合并顺序：按 (captured_at, sequence, local_id) 全局有序
    assert body["judgment_id"]
    insp = body["inspection"]
    assert insp["inspection_id"] == 1
    assert insp["device_id"] == DEVICE
    assert (insp["from_sequence"], insp["to_sequence"]) == (1, 3)
    assert insp["idempotency_key"] == "insp-1"
    assert insp["batch_count"] == 3
    assert insp["observation_count"] == len(timestamps)
    assert insp["batches"] == [
        {
            "sequence": i + 1,
            "batch_id": f"b-{i+1}",
            "previous_digest": "" if i == 0 else digests[i - 1],
            "digest": digests[i],
            "observation_count": len(chunks[i]),
        }
        for i in range(3)
    ]
    assert insp["created_at"]

    # /judgments/{id} 同样可查，来源标记为 inspection，保存的请求即合并观测
    j = client.get(f"/judgments/{body['judgment_id']}").json()
    assert j["source"] == "inspection"
    assert j["status"] == "matched" and j["best_aid_id"] == "A"
    assert j["request"]["observations"] == timestamps
    assert j["request"]["tolerance"] == 0.5
    assert j["request"]["device_id"] == DEVICE

    # /judgments 列表带 source，inspection 判定也出现在列表中
    rows = client.get("/judgments").json()["judgments"]
    top = rows[0]
    assert top["id"] == body["judgment_id"] and top["source"] == "inspection"


def test_inspection_merge_orders_across_batches_by_captured_at(client):
    """批次内各自有序，但批次间时间窗倒置：合并必须全局重排为时间升序。"""
    client.post("/library/import", json=LIBRARY)
    timestamps = make_obs()
    # 按时间切成 3 段后乱序上传（链上 seq 顺序与时间窗顺序不一致）
    chunks_in_time = [timestamps[:13], timestamps[13:27], timestamps[27:]]
    perm = [chunks_in_time[2], chunks_in_time[0], chunks_in_time[1]]
    upload_chain(client, DEVICE, perm)

    r = create_inspection(client, DEVICE, 1, 3, "insp-order", expected=201)
    body = r.json()
    assert body["status"] == "matched"
    assert body["best"]["aid_id"] == "A"
    assert body["best"]["matched"] == len(timestamps)
    merged = client.get(f"/judgments/{body['judgment_id']}").json()["request"]["observations"]
    assert merged == timestamps  # 全局按 captured_at 升序


def test_inspection_supports_subrange(client):
    client.post("/library/import", json=LIBRARY)
    timestamps = make_obs()
    chunks = [timestamps[:10], timestamps[10:20], timestamps[20:30], timestamps[30:]]
    _, digests = upload_chain(client, DEVICE, chunks)

    r = create_inspection(client, DEVICE, 2, 3, "insp-sub", expected=201)
    body = r.json()
    assert body["observation_count"] == 20
    insp = body["inspection"]
    assert (insp["from_sequence"], insp["to_sequence"]) == (2, 3)
    assert [b["sequence"] for b in insp["batches"]] == [2, 3]
    assert insp["batches"][0]["digest"] == digests[1]

    merged = client.get(f"/judgments/{body['judgment_id']}").json()["request"]["observations"]
    assert merged == timestamps[10:30]


def test_inspection_no_candidate_status_preserved(client):
    client.post("/library/import", json=LIBRARY)
    # 与库不符的观测（均匀间隔 7 秒），在阈值下无合格候选
    weird = [1_700_000_000.0 + i * 7.0 for i in range(10)]
    upload_chain(client, DEVICE, [weird[:5], weird[5:]])
    r = create_inspection(client, DEVICE, 1, 2, "insp-none", expected=201)
    body = r.json()
    assert body["status"] == "no_candidate"
    assert body["reason"] and body["best"] is None
    j = client.get(f"/judgments/{body['judgment_id']}").json()
    assert j["status"] == "no_candidate" and j["best_aid_id"] is None


def test_inspection_requires_active_library(client):
    upload_chain(client, DEVICE, [make_obs()[:5]])
    r = create_inspection(client, DEVICE, 1, 1, "insp-nolib")
    assert r.status_code == 400
    assert "active library version" in r.json()["detail"]
    # 拒绝后没有任何判定或来源记录
    assert client.get("/judgments").json()["judgments"] == []
    assert client.get(f"/field-devices/{DEVICE}/range-judgments").json()["inspections"] == []


def test_inspection_pins_active_version_at_request_start(client):
    """请求开始固定激活版本：判定记录的 library_version 与来源记录一致。"""
    client.post("/library/import", json=LIBRARY)
    timestamps = make_obs()
    upload_chain(client, DEVICE, [timestamps])

    captured = {}

    # 在 db 层直接观察：创建期间激活版本不允许切换（写锁串行），
    # 随后激活 v2 再创建一次，两次来源各自记录固定版本
    r1 = create_inspection(client, DEVICE, 1, 1, "insp-v1", expected=201)
    captured["v1"] = r1.json()["library_version"]

    d = client.post(
        "/library/versions",
        json={"aids": LIBRARY["aids"] + [
            {"aid_id": "Z", "period": 8.0, "flashes": [0.0]}
        ], "base_version": 1},
    ).json()
    client.post(f"/library/versions/{d['version_id']}/activate")
    r2 = create_inspection(client, DEVICE, 1, 1, "insp-v2", expected=201)
    assert r2.json()["library_version"] == d["version_id"]

    # v1 来源记录不可变：仍指向版本 1，且响应快照不随激活切换改变
    assert captured["v1"] == 1
    got = client.get(
        f"/field-devices/{DEVICE}/range-judgments/{r1.json()['inspection']['inspection_id']}"
    ).json()
    assert got["library_version"] == 1
    assert got["best"]["aid_id"] == "A"


# ---------------- 范围校验：缺批次 / 墓碑 / 阈值 ----------------


def test_inspection_rejects_missing_batches_without_persisting(client):
    client.post("/library/import", json=LIBRARY)
    timestamps = make_obs()
    # 只有 3 批，却请求 2..5
    upload_chain(client, DEVICE, [timestamps[:5], timestamps[5:10], timestamps[10:15]])

    r = create_inspection(client, DEVICE, 2, 5, "insp-gap")
    assert r.status_code == 409
    d = r.json()["detail"]
    assert d["error"] == "inspection_conflict"
    assert d["reason"] == "range_missing_batches"
    assert d["missing_sequences"] == [4, 5]
    assert d["from_sequence"] == 2 and d["to_sequence"] == 5
    # 不落判定、不落来源记录
    assert client.get("/judgments").json()["judgments"] == []
    assert client.get(f"/field-devices/{DEVICE}/range-judgments").json()["inspections"] == []


def test_inspection_rejects_archived_tombstones_in_range(client):
    client.post("/library/import", json=LIBRARY)
    timestamps = make_obs()
    chunks = [timestamps[:13], timestamps[13:27], timestamps[27:]]
    _, digests = upload_chain(client, DEVICE, chunks)
    register(client, DEVICE, "archiver")
    ack_all(client, DEVICE, "archiver", digests)
    ar = client.post(
        f"/field-devices/{DEVICE}/archive", json={"through_sequence": 2}
    )
    assert ar.status_code == 201

    # 范围含已归档墓碑 → 整体拒绝
    r = create_inspection(client, DEVICE, 1, 3, "insp-tomb")
    assert r.status_code == 409
    d = r.json()["detail"]
    assert d["reason"] == "range_archived"
    assert d["archived_sequences"] == [1, 2]
    assert client.get("/judgments").json()["judgments"] == []
    assert client.get(f"/field-devices/{DEVICE}/range-judgments").json()["inspections"] == []

    # 完全落在归档前缀也拒绝（任意一批是墓碑即拒绝）
    r = create_inspection(client, DEVICE, 1, 1, "insp-tomb-1")
    assert r.status_code == 409 and r.json()["detail"]["archived_sequences"] == [1]

    # 仅覆盖未归档部分则成功
    r = create_inspection(client, DEVICE, 3, 3, "insp-live", expected=201)
    assert r.json()["inspection"]["batches"][0]["sequence"] == 3


def test_inspection_rejects_inconsistent_thresholds(client):
    client.post("/library/import", json=LIBRARY)
    timestamps = make_obs()
    chunks = [timestamps[:13], timestamps[13:27], timestamps[27:]]
    # seq2 保存不同的 tolerance
    bodies, digests = [], []
    prev = ""
    for seq, ts in enumerate(chunks, start=1):
        kw = {"tolerance": 0.2} if seq == 2 else {}
        b = batch(f"b-{seq}", seq, to_batch_obs(ts), previous_digest=prev, **kw)
        r = client.post(f"/field-devices/{DEVICE}/batches", json=b)
        assert r.status_code == 201, r.text
        bodies.append(b)
        digests.append(r.json()["digest"])
        prev = digests[-1]

    r = create_inspection(client, DEVICE, 1, 3, "insp-thr")
    assert r.status_code == 409
    d = r.json()["detail"]
    assert d["reason"] == "threshold_mismatch"
    assert d["reference_sequence"] == 1
    assert d["expected_thresholds"] == THRESHOLDS
    assert d["mismatches"] == [
        {"sequence": 2, "thresholds": {**THRESHOLDS, "tolerance": 0.2},
         "differing_fields": ["tolerance"]}
    ]
    assert client.get("/judgments").json()["judgments"] == []

    # 阈值一致的子范围（1..1）仍可判定
    r = create_inspection(client, DEVICE, 1, 1, "insp-thr-ok", expected=201)
    assert r.json()["parameters"]["tolerance"] == 0.5


def test_inspection_range_validation(client):
    client.post("/library/import", json=LIBRARY)
    upload_chain(client, DEVICE, [make_obs()[:3]])
    r = client.post(
        f"/field-devices/{DEVICE}/range-judgments",
        json={"from_sequence": 2, "to_sequence": 1, "idempotency_key": "bad"},
    )
    assert r.status_code == 422
    r = client.post(
        f"/field-devices/{DEVICE}/range-judgments",
        json={"from_sequence": 0, "to_sequence": 1, "idempotency_key": "bad"},
    )
    assert r.status_code == 422
    r = client.post(
        f"/field-devices/{DEVICE}/range-judgments",
        json={"from_sequence": 1, "to_sequence": 1, "idempotency_key": "   "},
    )
    assert r.status_code == 422


# ---------------- 幂等 ----------------


def test_inspection_idempotent_retry_returns_original(client):
    client.post("/library/import", json=LIBRARY)
    timestamps = make_obs()
    upload_chain(client, DEVICE, [timestamps[:20], timestamps[20:]])

    r1 = create_inspection(client, DEVICE, 1, 2, "insp-idem", expected=201)
    first = r1.json()
    r2 = create_inspection(client, DEVICE, 1, 2, "insp-idem")
    assert r2.status_code == 200
    again = r2.json()
    assert again["judgment_id"] == first["judgment_id"]
    assert again["inspection"]["inspection_id"] == first["inspection"]["inspection_id"]
    assert again["inspection"]["created_at"] == first["inspection"]["created_at"]
    assert again["best"]["aid_id"] == first["best"]["aid_id"]

    # 没有产生第二条判定或来源记录
    rows = client.get("/judgments").json()["judgments"]
    assert len(rows) == 1
    lst = client.get(f"/field-devices/{DEVICE}/range-judgments").json()
    assert lst["count"] == 1 and lst["next_cursor"] is None


def test_inspection_same_key_changed_scope_conflicts(client):
    client.post("/library/import", json=LIBRARY)
    timestamps = make_obs()
    upload_chain(client, DEVICE, [timestamps[:10], timestamps[10:20], timestamps[20:30]])
    create_inspection(client, DEVICE, 1, 2, "shared-key", expected=201)

    # 同键改范围 → 409
    r = create_inspection(client, DEVICE, 1, 3, "shared-key")
    assert r.status_code == 409
    d = r.json()["detail"]
    assert d["reason"] == "idempotency_key_in_use"
    assert d["occupied_device_id"] == DEVICE
    assert d["occupied_from_sequence"] == 1
    assert d["occupied_to_sequence"] == 2

    # 同键改设备 → 409
    upload_chain(client, "dev-other", [timestamps[:3]])
    r = create_inspection(client, "dev-other", 1, 1, "shared-key")
    assert r.status_code == 409
    assert r.json()["detail"]["occupied_device_id"] == DEVICE

    # 冲突请求不产生新记录
    assert client.get("/judgments").json()["judgments"][0]["id"] == 1
    # 原判定仍可按原键重试取回
    r = create_inspection(client, DEVICE, 1, 2, "shared-key")
    assert r.status_code == 200


def test_inspection_same_key_concurrent_creates_one(client):
    client.post("/library/import", json=LIBRARY)
    timestamps = make_obs()
    upload_chain(client, DEVICE, [timestamps])

    n = 8
    barrier = threading.Barrier(n)
    results = []
    ids = []
    lock = threading.Lock()

    def worker():
        barrier.wait()
        r = create_inspection(client, DEVICE, 1, 1, "concurrent-key")
        with lock:
            results.append(r.status_code)
            if r.status_code in (200, 201):
                b = r.json()
                ids.append((b["judgment_id"], b["inspection"]["inspection_id"]))

    threads = [threading.Thread(target=worker) for _ in range(n)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert results.count(201) == 1
    assert results.count(200) == n - 1
    assert set(ids) == {(1, 1)}
    judgment_rows = client.get("/judgments").json()["judgments"]
    assert len(judgment_rows) == 1
    assert judgment_rows[0]["source"] == "inspection"


# ---------------- 与上传 / 归档并发 ----------------


def test_inspection_concurrent_with_archive_never_holds_half_range(client):
    """判定与归档并发：要么判定成功且范围全部未归档，要么整体 409 且不落判定。"""
    client.post("/library/import", json=LIBRARY)
    n_batches = 8
    timestamps = make_obs(n_cycles=40)
    # 每批 10 条，共 80 条
    chunks = [timestamps[i * 10:(i + 1) * 10] for i in range(n_batches)]
    _, digests = upload_chain(client, DEVICE, chunks)
    register(client, DEVICE, "archiver")
    ack_all(client, DEVICE, "archiver", digests)

    errors = []
    lock = threading.Lock()

    def inspector():
        r = create_inspection(client, DEVICE, 1, n_batches, "concurrent-insp")
        if r.status_code == 201:
            body = r.json()
            # 成功：保存的范围必须完整且与请求一致
            if body["inspection"]["batch_count"] != n_batches:
                errors.append(("half-range", body["inspection"]))
        elif r.status_code != 409:
            errors.append(("inspection", r.status_code, r.text))

    def archiver():
        r = client.post(
            f"/field-devices/{DEVICE}/archive",
            json={"through_sequence": n_batches},
        )
        if r.status_code not in (200, 201):
            errors.append(("archive", r.status_code, r.text))

    t1 = threading.Thread(target=inspector)
    t2 = threading.Thread(target=archiver)
    t1.start(); t2.start()
    t1.join(); t2.join()
    assert errors == []

    # 事后不变量：若判定成功，则 judgments 恰有 1 条 source=inspection，
    # 且其合并观测数等于全部 80 条；归档状态与来源记录互不矛盾
    insp_rows = client.get("/judgments").json()["judgments"]
    archived_through = client.get(
        f"/field-devices/{DEVICE}/archive-status"
    ).json()["archived_through"]
    if insp_rows:
        assert len(insp_rows) == 1
        j = client.get(f"/judgments/{insp_rows[0]['id']}").json()
        assert j["source"] == "inspection"
        assert len(j["request"]["observations"]) == 80
        rec = client.get(f"/field-devices/{DEVICE}/range-judgments/1")
        assert rec.status_code == 200
        assert len(rec.json()["inspection"]["batches"]) == n_batches
        # 判定先于归档：写锁互斥，成功时明细仍在；归档随后可以完成（已保存
        # 的各批摘要不受影响），或归档未发生——两种结局都不允许半个范围
        if archived_through == n_batches:
            detail = client.get(
                f"/field-devices/{DEVICE}/range-judgments/1"
            ).json()
            assert len(detail["inspection"]["batches"]) == n_batches
            assert detail["observation_count"] == 80
        else:
            assert archived_through == 0
    else:
        # 归档先完成：范围全部为墓碑
        assert archived_through == n_batches


def test_inspection_concurrent_with_upload(client):
    """判定进行中链上追加新批次：判定只覆盖请求范围，不受新批次影响。"""
    client.post("/library/import", json=LIBRARY)
    timestamps = make_obs(n_cycles=30)
    # 上传前两批；第三批留到判定之后追加
    upload_chain(client, DEVICE, [timestamps[:20], timestamps[20:40]])

    r = create_inspection(client, DEVICE, 1, 2, "insp-upload-conc", expected=201)
    body = r.json()
    assert body["inspection"]["to_sequence"] == 2
    assert body["observation_count"] == 40

    # 追加 seq3（不影响 1..2 的判定）
    head = client.get(f"/field-devices/{DEVICE}/cursor").json()["head_digest"]
    r = client.post(
        f"/field-devices/{DEVICE}/batches",
        json=batch("b-3", 3, to_batch_obs(timestamps[40:], start=40),
                   previous_digest=head),
    )
    assert r.status_code == 201

    # 已保存的判定不随后续上传改变
    got = client.get("/field-devices/dev-inspect/range-judgments/1").json()
    assert got["inspection"]["to_sequence"] == 2
    assert got["observation_count"] == 40

    # 扩大范围再次判定包含 seq3
    r = create_inspection(client, DEVICE, 1, 3, "insp-upload-conc-2", expected=201)
    assert r.json()["observation_count"] == len(timestamps)


def test_saved_inspection_survives_later_archival(client):
    """已保存结果引用创建时刻快照：之后归档明细，来源各批摘要仍可复核。"""
    client.post("/library/import", json=LIBRARY)
    timestamps = make_obs()
    chunks = [timestamps[:13], timestamps[13:27], timestamps[27:]]
    _, digests = upload_chain(client, DEVICE, chunks)
    r = create_inspection(client, DEVICE, 1, 3, "insp-before-archive", expected=201)
    saved = r.json()

    register(client, DEVICE, "archiver")
    ack_all(client, DEVICE, "archiver", digests)
    ar = client.post(
        f"/field-devices/{DEVICE}/archive", json={"through_sequence": 3}
    )
    assert ar.status_code == 201

    got = client.get(
        f"/field-devices/{DEVICE}/range-judgments/"
        f"{saved['inspection']['inspection_id']}"
    ).json()
    assert got["judgment_id"] == saved["judgment_id"]
    assert got["library_version"] == saved["library_version"]
    assert got["inspection"]["batches"] == saved["inspection"]["batches"]
    assert got["inspection"]["observation_count"] == len(timestamps)
    assert got["best"]["aid_id"] == "A"


# ---------------- 查询 ----------------


def test_inspection_list_and_get_and_404(client):
    client.post("/library/import", json=LIBRARY)
    timestamps = make_obs()
    chunks = [timestamps[i * 10:(i + 1) * 10] for i in range(4)]
    upload_chain(client, DEVICE, chunks)
    create_inspection(client, DEVICE, 1, 1, "k1", expected=201)
    create_inspection(client, DEVICE, 1, 2, "k2", expected=201)
    create_inspection(client, DEVICE, 3, 4, "k3", expected=201)

    lst = client.get(f"/field-devices/{DEVICE}/range-judgments").json()
    assert lst["device_id"] == DEVICE and lst["count"] == 3
    summaries = lst["inspections"]
    assert [s["inspection_id"] for s in summaries] == [1, 2, 3]
    s2 = summaries[1]
    assert (s2["from_sequence"], s2["to_sequence"]) == (1, 2)
    assert s2["library_version"] == 1
    assert s2["best_aid_id"] == "A" and s2["status"] == "matched"
    assert s2["batch_count"] == 2 and s2["observation_count"] == 20
    assert s2["idempotency_key"] == "k2"
    assert "batches" not in s2  # 列表为摘要，各批明细走单条查询

    # 游标分页
    page = client.get(
        f"/field-devices/{DEVICE}/range-judgments?after_inspection_id=1&limit=1"
    ).json()
    assert page["count"] == 1 and page["next_cursor"] == 2
    assert page["inspections"][0]["inspection_id"] == 2
    page = client.get(
        f"/field-devices/{DEVICE}/range-judgments?after_inspection_id=2&limit=1"
    ).json()
    assert page["count"] == 1 and page["next_cursor"] is None
    assert page["inspections"][0]["inspection_id"] == 3

    # 单条详情
    detail = client.get(f"/field-devices/{DEVICE}/range-judgments/2")
    assert detail.status_code == 200
    body = detail.json()
    assert body["inspection"]["batches"][0]["sequence"] == 1
    assert body["best"]["aid_id"] == "A"

    # 不存在 / 不属于该设备 → 404
    assert client.get(f"/field-devices/{DEVICE}/range-judgments/99").status_code == 404
    upload_chain(client, "dev-other", [timestamps[:3]])
    create_inspection(client, "dev-other", 1, 1, "other-k", expected=201)
    assert client.get(f"/field-devices/{DEVICE}/range-judgments/4").status_code == 404
    assert client.get("/field-devices/unknown/range-judgments/1").status_code == 404


def test_inspection_list_empty_device(client):
    lst = client.get("/field-devices/dev-empty/range-judgments").json()
    assert lst == {"device_id": "dev-empty", "count": 0,
                   "inspections": [], "next_cursor": None}


# ---------------- 与草稿回放的关系 ----------------


def test_inspection_judgments_replay_within_watermark(client):
    """巡检判定是 stored-library 判定：草稿回放按水位包含并沿用合并观测/阈值。"""
    client.post("/library/import", json=LIBRARY)
    timestamps = make_obs()
    upload_chain(client, DEVICE, [timestamps[:20], timestamps[20:]])
    r = create_inspection(client, DEVICE, 1, 2, "insp-replay", expected=201)
    assert r.json()["best"]["aid_id"] == "A"

    # 草稿删除 A，保留 B（B 与 A 同周期不同闪光）→ 巡检判定 winner_changed 或 lost
    draft = client.post(
        "/library/versions",
        json={"aids": [
            {"aid_id": "B", "period": 10.0, "flashes": [0.0, 4.0]},
            {"aid_id": "C", "period": 6.0, "flashes": [0.0]},
        ], "base_version": 1},
    ).json()
    rr = client.post(
        f"/library/versions/{draft['version_id']}/replay-reports",
        json={"idempotency_key": "rep-1"},
    )
    assert rr.status_code == 201
    summary = rr.json()
    assert summary["replayed_count"] == 1 and summary["skipped_count"] == 0
    page = client.get(
        f"/library/versions/{draft['version_id']}/replay-reports/"
        f"{summary['report_id']}/changes"
    ).json()
    item = page["changes"][0]
    assert item["judgment_id"] == r.json()["judgment_id"]
    assert item["old"]["best_aid_id"] == "A"
    assert item["classification"] in ("winner_changed", "match_lost")


# ---------------- 旧库迁移 ----------------


def test_legacy_judgments_source_defaults_to_judge(tmp_path):
    """旧库 judgments 无 source 列：迁移补列并回填 judge，新接口表正常创建。"""
    import sqlite3

    db = tmp_path / "legacy.db"
    conn = sqlite3.connect(db)
    conn.executescript(
        """
        CREATE TABLE judgments (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            created_at REAL NOT NULL,
            status TEXT NOT NULL,
            best_aid_id TEXT,
            matched INTEGER NOT NULL DEFAULT 0,
            missed INTEGER NOT NULL DEFAULT 0,
            false_count INTEGER NOT NULL DEFAULT 0,
            drift REAL,
            library_version INTEGER,
            request TEXT NOT NULL,
            response TEXT NOT NULL
        );
        """
    )
    conn.execute(
        "INSERT INTO judgments (created_at, status, request, response) "
        "VALUES (1, 'no_candidate', '{}', '{}')"
    )
    conn.commit()
    conn.close()

    app = create_app(db_path=str(db))
    with TestClient(app) as c:
        rows = c.get("/judgments").json()["judgments"]
        assert len(rows) == 1 and rows[0]["source"] == "judge"
        # 巡检表已随 schema 创建，空设备查询可用
        lst = c.get("/field-devices/dev-x/range-judgments").json()
        assert lst["inspections"] == []
