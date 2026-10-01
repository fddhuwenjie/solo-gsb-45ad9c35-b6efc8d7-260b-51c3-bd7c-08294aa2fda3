"""批次归档（清除已被全部必需消费者处理的连续前缀明细载荷）测试。

覆盖：归档前置校验（越界 409 附当前水位与阻塞消费者）、归档清空明细仅留
墓碑骨架、列表/详情/游标/状态均返回 archived_through、重复与更小目标幂等、
归档不影响链头摘要与后续上传、已归档批次按摘要重放并累计次数（载荷变化仍
409）、正确摘要确认幂等且不倒退水位、归档后登记的新消费者从
archived_through 起步（首个缺口指向下一批）、并发确认/登记/归档不越过未
确认批次且 archived_through 不回退，以及旧 SQLite 数据库升级兼容。
"""
import os
import sys
import threading

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pytest
from fastapi.testclient import TestClient

from app.main import create_app

THRESHOLDS = {"tolerance": 0.5, "max_drift": 5e-4, "max_missed": 3, "max_false": 3}
DEVICE = "dev-01"


@pytest.fixture()
def client(tmp_path):
    app = create_app(db_path=str(tmp_path / "test.db"))
    with TestClient(app) as c:
        yield c


def obs(n, base=1_700_000_000.0, step=0.5):
    return [
        {"local_id": f"o{i:03d}", "captured_at": base + i * step}
        for i in range(n)
    ]


def batch(batch_id, sequence, previous_digest="", observations=None, **kw):
    body = {
        "batch_id": batch_id,
        "sequence": sequence,
        "previous_digest": previous_digest,
        "observations": observations if observations is not None else obs(3),
        **THRESHOLDS,
    }
    body.update(kw)
    return body


def upload_chain(client, device, n):
    """上传 n 个链式批次，返回各批次请求体与摘要（下标 0 = seq1）。"""
    bodies, digests = [], []
    prev = ""
    for seq in range(1, n + 1):
        b = batch(f"b-{seq}", seq, previous_digest=prev)
        r = client.post(f"/field-devices/{device}/batches", json=b)
        assert r.status_code == 201
        bodies.append(b)
        digests.append(r.json()["digest"])
        prev = digests[-1]
    return bodies, digests


def register(client, device, consumer_id):
    return client.post(
        f"/field-devices/{device}/consumers", json={"consumer_id": consumer_id}
    )


def ack(client, device, consumer_id, sequence, digest):
    return client.post(
        f"/field-devices/{device}/acknowledgments",
        json={"consumer_id": consumer_id, "sequence": sequence, "batch_digest": digest},
    )


def ack_all(client, device, consumer_id, digests, seqs=None):
    seqs = seqs if seqs is not None else range(1, len(digests) + 1)
    for seq in seqs:
        r = ack(client, device, consumer_id, seq, digests[seq - 1])
        assert r.status_code in (200, 201), r.text


def status(client, device):
    return client.get(f"/field-devices/{device}/archive-status").json()


def archive(client, device, through):
    return client.post(
        f"/field-devices/{device}/archive", json={"through_sequence": through}
    )


def detail(client, device, seq):
    return client.get(f"/field-devices/{device}/batches/{seq}")


# ---------------- 前置校验：目标不得越过 safe_sequence ----------------


def test_archive_requires_consumers_and_acks(client):
    upload_chain(client, DEVICE, 2)

    # 无消费者：safe_sequence=0，归档任何序号都 409
    r = archive(client, DEVICE, 1)
    assert r.status_code == 409
    d = r.json()["detail"]
    assert d["error"] == "archive_conflict"
    assert d["reason"] == "beyond_safe_sequence"
    assert d["requested_through"] == 1
    assert d["safe_sequence"] == 0
    assert d["archived_through"] == 0
    assert d["head_sequence"] == 2
    assert d["first_gap"] == 1
    assert d["blocking_consumers"] == []

    # 登记消费者但尚未确认：safe 仍为 0（阻塞者为该消费者，水位落后链头）
    register(client, DEVICE, "archiver")
    r = archive(client, DEVICE, 1)
    assert r.status_code == 409
    d = r.json()["detail"]
    assert d["safe_sequence"] == 0 and d["first_gap"] == 1
    assert d["blocking_consumers"] == ["archiver"]


def test_archive_beyond_safe_sequence_conflict_with_blockers(client):
    _, digests = upload_chain(client, DEVICE, 4)
    register(client, DEVICE, "archiver")
    register(client, DEVICE, "ml")
    # archiver 全部确认，ml 只确认到 1 → safe=1
    ack_all(client, DEVICE, "archiver", digests)
    ack(client, DEVICE, "ml", 1, digests[0])

    r = archive(client, DEVICE, 2)
    assert r.status_code == 409
    d = r.json()["detail"]
    assert d["requested_through"] == 2
    assert d["safe_sequence"] == 1
    assert d["archived_through"] == 0
    assert d["head_sequence"] == 4
    assert d["first_gap"] == 2
    assert d["blocking_consumers"] == ["ml"]

    # 冲突不产生任何归档效果
    st = status(client, DEVICE)
    assert st["archived_through"] == 0 and st["safe_sequence"] == 1


def test_archive_beyond_chain_head_is_safe_violation(client):
    """目标超过链头必然超过 safe_sequence（safe ≤ head），仍是 409 而非 500。"""
    _, digests = upload_chain(client, DEVICE, 1)
    register(client, DEVICE, "a")
    ack(client, DEVICE, "a", 1, digests[0])
    r = archive(client, DEVICE, 99)
    assert r.status_code == 409
    assert r.json()["detail"]["safe_sequence"] == 1
    assert r.json()["detail"]["head_sequence"] == 1


def test_archive_validation_rejects_non_positive(client):
    assert archive(client, DEVICE, 0).status_code == 422
    assert archive(client, DEVICE, -3).status_code == 422


# ---------------- 归档执行：墓碑骨架 ----------------


def test_archive_prefix_clears_payload_keeps_skeleton(client):
    bodies, digests = upload_chain(client, DEVICE, 3)
    register(client, DEVICE, "archiver")
    ack_all(client, DEVICE, "archiver", digests)

    r = archive(client, DEVICE, 2)
    assert r.status_code == 201
    body = r.json()
    assert body["status"] == "archived"
    assert body["requested_through"] == 2
    assert body["archived_through"] == 2
    assert body["archived_at"] is not None
    assert body["newly_archived_count"] == 2
    assert body["head_sequence"] == 3 and body["safe_sequence"] == 3
    assert body["first_gap"] == 4
    assert body["blocking_consumers"] == []

    # seq1/seq2 成为墓碑：只保留 batch_id、序号、前序/本批摘要、条数、时间、
    # 首次受理响应与重放统计；观测与阈值明细被清空
    d1 = detail(client, DEVICE, 1).json()
    assert d1["status"] == "archived"
    assert d1["archived"] is True
    assert d1["archived_at"] == body["archived_at"]
    assert d1["batch_id"] == "b-1"
    assert d1["sequence"] == 1
    assert d1["digest"] == digests[0]
    assert d1["previous_digest"] == ""
    assert d1["observation_count"] == 3
    assert d1["observations"] is None
    assert d1["thresholds"] is None
    assert d1["replay_count"] == 0 and d1["last_replayed_at"] is None
    assert d1["created_at"] is not None

    d2 = detail(client, DEVICE, 2).json()
    assert d2["status"] == "archived" and d2["archived"] is True
    assert d2["batch_id"] == "b-2"
    assert d2["previous_digest"] == digests[0]
    assert d2["digest"] == digests[1]
    assert d2["observations"] is None and d2["thresholds"] is None

    # seq3 未归档：明细完整、status=accepted
    d3 = detail(client, DEVICE, 3).json()
    assert d3["status"] == "accepted" and d3["archived"] is False
    assert d3["observations"] == bodies[2]["observations"]
    assert d3["thresholds"] == THRESHOLDS
    assert d3["archived_at"] is None


def test_archive_status_list_cursor_report_archived_through(client):
    _, digests = upload_chain(client, DEVICE, 3)
    register(client, DEVICE, "archiver")
    ack_all(client, DEVICE, "archiver", digests)
    archive(client, DEVICE, 2)

    st = status(client, DEVICE)
    assert st["archived_through"] == 2
    assert st["archived_at"] is not None
    assert st["safe_sequence"] == 3 and st["head_sequence"] == 3

    lst = client.get(f"/field-devices/{DEVICE}/batches").json()
    assert lst["archived_through"] == 2
    by_seq = {b["sequence"]: b for b in lst["batches"]}
    assert by_seq[1]["status"] == "archived" and by_seq[1]["archived"] is True
    assert by_seq[1]["archived_at"] is not None
    assert by_seq[2]["status"] == "archived"
    assert by_seq[3]["status"] == "accepted" and by_seq[3]["archived"] is False
    # 列表墓碑也不携带明细（本来就只有骨架字段）
    assert set(["observations", "thresholds"]) & set(by_seq[1].keys()) == set()
    assert by_seq[1]["digest"] == digests[0] and by_seq[1]["observation_count"] == 3

    cur = client.get(f"/field-devices/{DEVICE}/cursor").json()
    assert cur["archived_through"] == 2
    assert cur["archived_at"] is not None
    # 链头摘要与计数不变
    assert cur["last_sequence"] == 3
    assert cur["head_digest"] == digests[2]
    assert cur["batch_count"] == 3
    assert cur["expected_previous_digest"] == digests[2]
    assert cur["next_sequence"] == 4


def test_archive_does_not_change_head_or_block_uploads(client):
    bodies, digests = upload_chain(client, DEVICE, 2)
    register(client, DEVICE, "archiver")
    ack_all(client, DEVICE, "archiver", digests)
    archive(client, DEVICE, 2)

    # 后续上传照常链接到原链头
    r = client.post(
        f"/field-devices/{DEVICE}/batches",
        json=batch("b-3", 3, previous_digest=digests[1]),
    )
    assert r.status_code == 201
    assert r.json()["digest"]  # 新摘要正常
    cur = client.get(f"/field-devices/{DEVICE}/cursor").json()
    assert cur["last_sequence"] == 3 and cur["batch_count"] == 3
    assert cur["archived_through"] == 2  # 新批次不推进归档水位

    # 用过期链头上传 seq4 → 409，期望值基于未变的链头历史
    bad = client.post(
        f"/field-devices/{DEVICE}/batches",
        json=batch("b-4", 4, previous_digest=digests[0]),
    )
    assert bad.status_code == 409
    assert bad.json()["detail"]["expected_sequence"] == 4
    assert bad.json()["detail"]["expected_previous_digest"] == r.json()["digest"]


def test_archive_partial_prefix_then_extend(client):
    _, digests = upload_chain(client, DEVICE, 3)
    register(client, DEVICE, "a")
    ack_all(client, DEVICE, "a", digests[:1], seqs=[1])

    r = archive(client, DEVICE, 1)
    assert r.status_code == 201
    assert r.json()["archived_through"] == 1
    assert r.json()["newly_archived_count"] == 1

    # 尚未全部确认到 2 前扩大到 2 → 409，归档水位不回退也不越界
    r = archive(client, DEVICE, 2)
    assert r.status_code == 409
    assert status(client, DEVICE)["archived_through"] == 1

    ack(client, DEVICE, "a", 2, digests[1])
    r = archive(client, DEVICE, 2)
    assert r.status_code == 201
    assert r.json()["archived_through"] == 2
    assert r.json()["newly_archived_count"] == 1  # 只新增归档 seq2
    d1 = detail(client, DEVICE, 1).json()
    d2 = detail(client, DEVICE, 2).json()
    assert d1["archived_at"] != d2["archived_at"]


# ---------------- 幂等：重复 / 更小目标 ----------------


def test_archive_idempotent_same_and_smaller_target(client):
    _, digests = upload_chain(client, DEVICE, 3)
    register(client, DEVICE, "archiver")
    ack_all(client, DEVICE, "archiver", digests)

    r1 = archive(client, DEVICE, 2)
    assert r1.status_code == 201
    first_at = r1.json()["archived_at"]

    # 相同目标：200 already_archived，归档时刻不刷新，计数为 0
    r2 = archive(client, DEVICE, 2)
    assert r2.status_code == 200
    b2 = r2.json()
    assert b2["status"] == "already_archived"
    assert b2["archived_through"] == 2
    assert b2["archived_at"] == first_at
    assert b2["newly_archived_count"] == 0

    # 更小目标同样幂等
    r3 = archive(client, DEVICE, 1)
    assert r3.status_code == 200
    assert r3.json()["status"] == "already_archived"
    assert r3.json()["archived_through"] == 2
    assert r3.json()["archived_at"] == first_at
    assert r3.json()["requested_through"] == 1

    # 墓碑时刻保持首次归档时刻
    assert detail(client, DEVICE, 1).json()["archived_at"] == first_at


# ---------------- 已归档批次的重放 ----------------


def test_replay_archived_batch_same_payload(client):
    bodies, digests = upload_chain(client, DEVICE, 2)
    register(client, DEVICE, "archiver")
    ack_all(client, DEVICE, "archiver", digests)
    archive(client, DEVICE, 2)

    # 相同载荷重放已归档批次：200 replayed、返回原结果、计数累加
    r = client.post(f"/field-devices/{DEVICE}/batches", json=bodies[0])
    assert r.status_code == 200
    rep = r.json()
    assert rep["status"] == "replayed"
    assert rep["digest"] == digests[0]
    assert rep["sequence"] == 1 and rep["batch_id"] == "b-1"
    assert rep["chain"]["head_sequence"] == 1  # 原响应快照即首次受理时的链头

    d1 = detail(client, DEVICE, 1).json()
    assert d1["status"] == "archived"  # 重放不解除墓碑
    assert d1["replay_count"] == 1
    assert d1["last_replayed_at"] is not None
    assert d1["observations"] is None and d1["thresholds"] is None

    # 再重放一次：计数继续累加，链头与归档水位不变
    client.post(f"/field-devices/{DEVICE}/batches", json=bodies[0])
    d1 = detail(client, DEVICE, 1).json()
    assert d1["replay_count"] == 2
    cur = client.get(f"/field-devices/{DEVICE}/cursor").json()
    assert cur["last_sequence"] == 2 and cur["archived_through"] == 2
    assert cur["last_event_status"] == "replayed"
    assert cur["last_event_sequence"] == 1


def test_replay_archived_batch_changed_payload_conflict(client):
    bodies, digests = upload_chain(client, DEVICE, 2)
    register(client, DEVICE, "archiver")
    ack_all(client, DEVICE, "archiver", digests)
    archive(client, DEVICE, 2)

    # 同 batch_id/sequence 但观测变化 → 明细已清空也必须按摘要拒绝：409
    changed = batch("b-1", 1, observations=obs(4))
    r = client.post(f"/field-devices/{DEVICE}/batches", json=changed)
    assert r.status_code == 409
    d = r.json()["detail"]
    assert d["reason"] == "payload_changed"
    assert d["existing_batch"] == {
        "sequence": 1, "batch_id": "b-1", "digest": digests[0],
    }
    # 冲突不计数
    assert detail(client, DEVICE, 1).json()["replay_count"] == 0


# ---------------- 已归档批次的确认 ----------------


def test_ack_archived_batch_idempotent_and_no_regression(client):
    _, digests = upload_chain(client, DEVICE, 3)
    register(client, DEVICE, "archiver")
    ack(client, DEVICE, "archiver", 1, digests[0])
    archive(client, DEVICE, 1)  # safe 此时只有 1
    before = status(client, DEVICE)
    assert before["archived_through"] == 1
    assert before["consumers"][0]["watermark"] == 1

    # 对已归档批次的正确摘要确认幂等（duplicate），水位/归档水位都不倒退
    r = ack(client, DEVICE, "archiver", 1, digests[0])
    assert r.status_code == 200
    b = r.json()
    assert b["status"] == "duplicate"
    assert b["watermark"] == 1
    assert b["safe_sequence"] == 1
    assert b["archived_through"] == 1

    after = status(client, DEVICE)
    assert after["archived_through"] == 1
    assert after["consumers"][0]["watermark"] == 1


def test_ack_archived_batch_wrong_digest_conflict(client):
    _, digests = upload_chain(client, DEVICE, 2)
    register(client, DEVICE, "archiver")
    ack(client, DEVICE, "archiver", 1, digests[0])
    archive(client, DEVICE, 1)
    r = ack(client, DEVICE, "archiver", 1, "ab" * 32)
    assert r.status_code == 409
    assert r.json()["detail"]["reason"] == "digest_mismatch"
    assert r.json()["detail"]["expected_digest"] == digests[0]


# ---------------- 归档后登记的新消费者 ----------------


def test_new_consumer_after_archive_starts_at_archived_through(client):
    _, digests = upload_chain(client, DEVICE, 4)
    register(client, DEVICE, "early")
    ack_all(client, DEVICE, "early", digests)
    archive(client, DEVICE, 2)

    # 归档后登记的消费者从 archived_through 起步
    r = register(client, DEVICE, "late")
    assert r.status_code == 201
    b = r.json()
    assert b["watermark"] == 2
    assert b["archived_through"] == 2

    st = status(client, DEVICE)
    late = next(c for c in st["consumers"] if c["consumer_id"] == "late")
    assert late["watermark"] == 2
    # 首个缺口指向归档前缀的下一批（seq3），而非 seq1
    assert late["first_gap"] == 3
    # 新消费者尚未追平链头 → 阻塞，safe 回落到 2（但不低于 archived_through）
    assert st["safe_sequence"] == 2
    assert st["first_gap"] == 3
    assert st["blocking_consumers"] == ["late"]

    # 乱序先确认 seq4：水位仍停在 2（seq3 是缺口）
    ack(client, DEVICE, "late", 4, digests[3])
    late = next(c for c in status(client, DEVICE)["consumers"] if c["consumer_id"] == "late")
    assert late["watermark"] == 2 and late["first_gap"] == 3
    # 不能归档越过 safe(=2)
    assert archive(client, DEVICE, 3).status_code == 409

    # 补上 seq3：越过 3,4 到链头，safe 恢复 4，可继续归档
    ack(client, DEVICE, "late", 3, digests[2])
    st = status(client, DEVICE)
    late = next(c for c in st["consumers"] if c["consumer_id"] == "late")
    assert late["watermark"] == 4 and late["first_gap"] is None
    assert st["safe_sequence"] == 4 and st["blocking_consumers"] == []
    r = archive(client, DEVICE, 4)
    assert r.status_code == 201 and r.json()["archived_through"] == 4
    assert r.json()["newly_archived_count"] == 2  # seq3、seq4 新归档


def test_new_consumer_does_not_require_archived_acks(client):
    """新消费者确认归档区间内序号的行为与普通序号一致（存在即可，非必需）。"""
    _, digests = upload_chain(client, DEVICE, 2)
    register(client, DEVICE, "a")
    ack_all(client, DEVICE, "a", digests)
    archive(client, DEVICE, 2)
    register(client, DEVICE, "b")
    # b 起步即 2，已追平链头，first_gap 为 None，不阻塞
    st = status(client, DEVICE)
    b = next(c for c in st["consumers"] if c["consumer_id"] == "b")
    assert b["watermark"] == 2 and b["first_gap"] is None
    assert st["safe_sequence"] == 2 and st["blocking_consumers"] == []
    # 仍可对已归档批次补确认（墓碑摘要可查），结果 duplicate（起步水位已覆盖）
    r = ack(client, DEVICE, "b", 1, digests[0])
    assert r.status_code in (200, 201)


# ---------------- 归档后的链式重放与游标骨架 ----------------


def test_replay_response_preserves_first_acceptance_after_archive(client):
    bodies, digests = upload_chain(client, DEVICE, 1)
    register(client, DEVICE, "archiver")
    ack(client, DEVICE, "archiver", 1, digests[0])
    r = archive(client, DEVICE, 1)
    archived_at = r.json()["archived_at"]

    rp = client.post(f"/field-devices/{DEVICE}/batches", json=bodies[0]).json()
    assert rp["status"] == "replayed"
    # 首次受理响应保留：observation_count/received_at 等仍取自 response 快照
    assert rp["observation_count"] == 3
    assert rp["received_at"] is not None
    # 列表骨架上重放统计可见，墓碑标注不变
    item = client.get(f"/field-devices/{DEVICE}/batches").json()["batches"][0]
    assert item["status"] == "archived" and item["archived"] is True
    assert item["replay_count"] == 1
    assert item["archived_at"] == archived_at


# ---------------- 并发：确认 / 登记 / 归档 ----------------


def test_concurrent_acks_then_archive_never_crosses_unacked(client):
    """多消费者乱序并发确认期间反复尝试归档：成功目标永不超过当时 safe。"""
    n_batches = 12
    _, digests = upload_chain(client, DEVICE, n_batches)
    consumer_ids = ["a", "b", "c"]
    for cid in consumer_ids:
        register(client, DEVICE, cid)

    errors = []
    lock = threading.Lock()

    def confirmer(cid):
        for seq in range(1, n_batches + 1):
            r = ack(client, DEVICE, cid, seq, digests[seq - 1])
            if r.status_code not in (200, 201):
                with lock:
                    errors.append((cid, seq, r.status_code, r.text))

    def archiver():
        # 不断把归档水位向前推到当时 safe，任何一次 409 的请求目标都必须 > safe
        target = 1
        while target <= n_batches:
            r = archive(client, DEVICE, target)
            if r.status_code in (200, 201):
                body = r.json()
                if body["archived_through"] > body["safe_sequence"]:
                    with lock:
                        errors.append(("archive", target, "invariant", body))
                target = body["safe_sequence"] + 1
            elif r.status_code == 409:
                d = r.json()["detail"]
                if d["requested_through"] <= d["safe_sequence"]:
                    with lock:
                        errors.append(("archive", target, "false-conflict", d))
                # 等确认推进后重试同一目标
            else:
                with lock:
                    errors.append(("archive", target, r.status_code, r.text))

    threads = [threading.Thread(target=confirmer, args=(cid,)) for cid in consumer_ids]
    threads.append(threading.Thread(target=archiver))
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert errors == []
    st = status(client, DEVICE)
    assert st["safe_sequence"] == n_batches
    # 归档线程最终应能推到链头
    r = archive(client, DEVICE, n_batches)
    assert r.status_code in (200, 201)
    assert status(client, DEVICE)["archived_through"] == n_batches
    for seq in range(1, n_batches + 1):
        d = detail(client, DEVICE, seq).json()
        assert d["status"] == "archived" and d["observations"] is None


def test_concurrent_archive_requests_serialized(client):
    """多个相同目标并发归档：恰好一次 201，其余 200 幂等，墓碑只归档一遍。"""
    n_batches = 6
    _, digests = upload_chain(client, DEVICE, n_batches)
    register(client, DEVICE, "archiver")
    ack_all(client, DEVICE, "archiver", digests)

    n = 10
    barrier = threading.Barrier(n)
    results = []
    counts = []
    lock = threading.Lock()

    def worker():
        barrier.wait()
        r = archive(client, DEVICE, n_batches)
        with lock:
            results.append(r.status_code)
            counts.append(r.json().get("newly_archived_count"))

    threads = [threading.Thread(target=worker) for _ in range(n)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert results.count(201) == 1
    assert results.count(200) == n - 1
    assert sorted(c for c in counts if c is not None) == sorted(
        [n_batches] + [0] * (n - 1)
    )
    assert status(client, DEVICE)["archived_through"] == n_batches


def test_concurrent_new_consumer_registration_caps_archive(client):
    """归档推进到链头的同时登记新消费者：随后归档不得越过新消费者水位。"""
    n_batches = 4
    _, digests = upload_chain(client, DEVICE, n_batches)
    register(client, DEVICE, "old")
    ack_all(client, DEVICE, "old", digests)

    barrier = threading.Barrier(2)
    errors = []

    def do_archive():
        barrier.wait()
        r = archive(client, DEVICE, n_batches)
        # 与注册并发：要么全量成功（注册先于归档时则会 409）
        if r.status_code not in (200, 201, 409):
            errors.append(("archive", r.status_code, r.text))
        elif r.status_code in (200, 201):
            body = r.json()
            if body["archived_through"] > n_batches:
                errors.append(("archive", "regression", body))

    def do_register():
        barrier.wait()
        r = register(client, DEVICE, "new")
        if r.status_code not in (200, 201):
            errors.append(("register", r.status_code, r.text))

    t1 = threading.Thread(target=do_archive)
    t2 = threading.Thread(target=do_register)
    t1.start(); t2.start()
    t1.join(); t2.join()
    assert errors == []

    st = status(client, DEVICE)
    archived = st["archived_through"]
    new = next(c for c in st["consumers"] if c["consumer_id"] == "new")
    # 新消费者水位等于登记时的归档水位；它一旦落后链头，safe 必须回落到该水位，
    # 之后的归档不得越过
    assert new["watermark"] == archived
    assert st["safe_sequence"] == archived
    if archived < n_batches:
        r = archive(client, DEVICE, n_batches)
        assert r.status_code == 409
        assert r.json()["detail"]["blocking_consumers"] == ["new"]


def test_archived_through_monotonic_under_repeat_archives(client):
    _, digests = upload_chain(client, DEVICE, 2)
    register(client, DEVICE, "a")
    ack_all(client, DEVICE, "a", digests)
    archive(client, DEVICE, 2)
    for _ in range(5):
        r = archive(client, DEVICE, 1)
        assert r.status_code == 200 and r.json()["archived_through"] == 2
    assert status(client, DEVICE)["archived_through"] == 2


# ---------------- 旧 SQLite 数据库兼容 ----------------


def test_legacy_db_archive_columns_migrated(tmp_path):
    """旧库（批次/链表均为旧结构）启动后自动补列并可归档。"""
    import json as _json
    import sqlite3

    db = tmp_path / "legacy-archive.db"
    conn = sqlite3.connect(db)
    conn.executescript(
        """
        CREATE TABLE device_chains (
            device_id     TEXT PRIMARY KEY,
            last_sequence INTEGER NOT NULL,
            head_digest   TEXT NOT NULL,
            batch_count   INTEGER NOT NULL,
            updated_at    REAL NOT NULL
        );
        CREATE TABLE device_batches (
            id                INTEGER PRIMARY KEY AUTOINCREMENT,
            device_id         TEXT NOT NULL,
            sequence          INTEGER NOT NULL,
            batch_id          TEXT NOT NULL,
            previous_digest   TEXT NOT NULL,
            digest            TEXT NOT NULL,
            observation_count INTEGER NOT NULL,
            thresholds        TEXT NOT NULL,
            observations      TEXT NOT NULL,
            response          TEXT NOT NULL,
            status            TEXT NOT NULL DEFAULT 'accepted',
            created_at        REAL NOT NULL,
            UNIQUE (device_id, sequence),
            UNIQUE (device_id, batch_id)
        );
        """
    )
    # 直接造一条旧版批次与链记录
    body1 = batch("b-1", 1)
    import hashlib

    payload = {
        "batch_id": "b-1", "device_id": DEVICE,
        "max_drift": body1["max_drift"], "max_false": body1["max_false"],
        "max_missed": body1["max_missed"],
        "observations": [
            {"captured_at": o["captured_at"], "local_id": o["local_id"]}
            for o in body1["observations"]
        ],
        "sequence": 1, "tolerance": body1["tolerance"],
    }
    d1 = hashlib.sha256(
        _json.dumps(payload, ensure_ascii=False, sort_keys=True,
                    separators=(",", ":")).encode("utf-8")
    ).hexdigest()
    ts = 1_700_000_000.0
    thresholds1 = {k: body1[k] for k in ("tolerance", "max_drift", "max_missed", "max_false")}
    conn.execute(
        "INSERT INTO device_batches "
        "(device_id, sequence, batch_id, previous_digest, digest, observation_count, "
        "thresholds, observations, response, status, created_at) "
        "VALUES (?, 1, 'b-1', '', ?, ?, ?, ?, ?, 'accepted', ?)",
        (
            DEVICE, d1, len(body1["observations"]),
            _json.dumps(thresholds1), _json.dumps(body1["observations"]),
            _json.dumps({"status": "accepted", "digest": d1}), ts,
        ),
    )
    conn.execute(
        "INSERT INTO device_chains "
        "(device_id, last_sequence, head_digest, batch_count, updated_at) "
        "VALUES (?, 1, ?, 1, ?)",
        (DEVICE, d1, ts),
    )
    conn.commit()
    conn.close()

    app = create_app(db_path=str(db))
    with TestClient(app) as c:
        # 迁移回填：归档水位为 0，旧批次明细仍可读、未归档
        st = status(c, DEVICE)
        assert st["archived_through"] == 0 and st["archived_at"] is None
        assert st["safe_sequence"] == 0
        d = detail(c, DEVICE, 1).json()
        assert d["status"] == "accepted" and d["archived"] is False
        assert d["observations"] == body1["observations"]
        assert d["thresholds"] == THRESHOLDS
        cur = c.get(f"/field-devices/{DEVICE}/cursor").json()
        assert cur["archived_through"] == 0 and cur["archived_at"] is None

        # 登记 + 确认旧批次后可正常归档：明细被清空、骨架保留
        register(c, DEVICE, "archiver")
        r = ack(c, DEVICE, "archiver", 1, d1)
        assert r.status_code == 201 and r.json()["archived_through"] == 0
        r = archive(c, DEVICE, 1)
        assert r.status_code == 201 and r.json()["archived_through"] == 1
        d = detail(c, DEVICE, 1).json()
        assert d["status"] == "archived" and d["archived"] is True
        assert d["observations"] is None and d["thresholds"] is None
        assert d["digest"] == d1 and d["batch_id"] == "b-1"

        # 归档后重放旧批次仍按摘要工作
        r = c.post(f"/field-devices/{DEVICE}/batches", json=body1)
        assert r.status_code == 200 and r.json()["status"] == "replayed"
        assert detail(c, DEVICE, 1).json()["replay_count"] == 1


def test_fresh_device_status_reports_zero_archive(client):
    st = client.get("/field-devices/dev-fresh/archive-status").json()
    assert st["archived_through"] == 0 and st["archived_at"] is None
    assert st["safe_sequence"] == 0 and st["head_sequence"] == 0
    cur = client.get("/field-devices/dev-fresh/cursor").json()
    assert cur["archived_through"] == 0 and cur["archived_at"] is None
    lst = client.get("/field-devices/dev-fresh/batches").json()
    assert lst["archived_through"] == 0
