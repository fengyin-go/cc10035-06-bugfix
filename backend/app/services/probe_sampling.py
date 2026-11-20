"""探针采样核心：把探针观测独立写入快照台账，并维护采样清单。

这个模块只做数据层的事情，供 ``probe_selector``、``page_snapshot`` 两个领域
服务以及 HTTP 路由复用。核心约束：

* 观测标识由「探针 + 页面 + 选择器 + DOM/文本哈希 + 采样时间」共同决定，
  不再只依赖页面，因此同一页面的价格探针和库存探针不会互相覆盖；
* 快照行和采样清单行在同一个临界区里写入，清单回写快照记录标识；
* 变化比对只使用同一探针「实际确认」的前一份样本；
* 并发提交时由锁串行化，先确认的条目成为后续条目的基线。
"""
from __future__ import annotations

import hashlib
import threading
from datetime import datetime, timezone
from itertools import count
from typing import Any

from app.store import store

SNAPSHOT_MODULE = "page_snapshot"
PROBE_MODULE = "probe_selector"
MANIFEST_MODULE = "sampling_manifest"

# 同一进程内的所有写入都走这把锁。FastAPI 的同步路由在线程池里执行，
# 并发提交会真正落在多个线程上，锁保证确认顺序和清单/台账的一致性。
_LOCK = threading.RLock()
_CONFIRM_SEQUENCE = count(1)
# 每次「运行采样」都要产生新观测，即使与上一次落在同一微秒也不能复用编号。
_TRIGGER_SEQUENCE = count(1)


def now_iso() -> str:
    """带微秒的 UTC 时间，避免同一秒内的两次采样拿到相同标识。"""
    return datetime.now(timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z")


def _next_id(module: str) -> int:
    rows = store.rows(module)
    return max((int(row.get("id", 0)) for row in rows), default=0) + 1


def _pick(payload: dict[str, Any], *names: str, default: Any = "") -> Any:
    """按多个候选字段名取值；空字符串视为未提供，便于中文/英文请求混用。"""
    for name in names:
        if name in payload:
            value = payload[name]
            if value is not None and value != "":
                return value
    return default


def _as_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value)


def unwrap_payload(payload: Any) -> dict[str, Any]:
    """兼容直接字段、``values``、``receipt`` 等常见回执包装。"""
    if not isinstance(payload, dict):
        return {}
    data: dict[str, Any] = {}
    for key in ("observation", "receipt", "snapshot_request", "values", "data", "payload"):
        nested = payload.get(key)
        if isinstance(nested, dict):
            data.update(nested)
    data.update(payload)
    return data


def resolve_probe(probe_ref: Any) -> dict[str, Any] | None:
    """探针既可传数字主键，也可传探针名称/业务探针ID。"""
    if probe_ref is None or probe_ref == "":
        return None
    found = store.find(PROBE_MODULE, int(probe_ref)) if str(probe_ref).isdigit() else None
    if found is not None:
        return found
    expected = str(probe_ref)
    for row in store.rows(PROBE_MODULE):
        if expected in {
            str(row.get("id", "")),
            str(row.get("探针ID", "")),
            str(row.get("探针名称", "")),
        }:
            return row
    return None


def content_fingerprint(
    probe_id: Any,
    page_uri: str,
    selectors: str,
    dom_hash: str,
    text_hash: str,
) -> str:
    """内容指纹：同一探针下用于防止「编号与内容错位」的稳定摘要。"""
    material = "|".join([
        _as_text(probe_id), page_uri, selectors, dom_hash, text_hash,
    ])
    return hashlib.sha256(material.encode("utf-8")).hexdigest()[:16]


def observation_id_for(
    probe_id: Any,
    page_uri: str,
    selectors: str,
    dom_hash: str,
    text_hash: str,
    captured_at: str,
    explicit: Any = None,
) -> str:
    """生成或规范化观测编号。

    显式回执编号优先；否则用内容 + 采样时间生成稳定编号，重送同一份回执
    会得到同一个编号，从而幂等而不是重复写入。
    """
    if explicit not in (None, "") and not isinstance(explicit, dict):
        return _as_text(explicit).strip()
    material = "|".join([
        _as_text(probe_id), page_uri, selectors, dom_hash, text_hash, captured_at,
    ])
    return "obs-" + hashlib.sha256(material.encode("utf-8")).hexdigest()[:16]


def snapshot_code_for(
    probe_id: Any,
    page_uri: str,
    selectors: str,
    dom_hash: str,
    text_hash: str,
    observation_id: str,
    captured_at: str,
) -> str:
    """快照ID不再只依赖页面，探针、选择器和内容都会参与摘要。"""
    material = "|".join([
        _as_text(probe_id), page_uri, selectors, dom_hash, text_hash,
        observation_id, captured_at,
    ])
    return "snap-" + hashlib.sha256(material.encode("utf-8")).hexdigest()[:16]


def _probe_matches(row: dict[str, Any], probe_id: Any) -> bool:
    return _as_text(row.get("探针ID")) == _as_text(probe_id)


def _confirmed(row: dict[str, Any]) -> bool:
    """只有确认过的台账行才能作为基线；历史示例行没有确认序号时按已确认处理。"""
    if row.get("确认") is False:
        return False
    if row.get("status") == "待确认":
        return False
    return True


def _snapshot_by_observation(probe_id: Any, observation_id: str) -> dict[str, Any] | None:
    for row in store.rows(SNAPSHOT_MODULE):
        if (
            _as_text(row.get("观测编号")) == observation_id
            and _probe_matches(row, probe_id)
        ):
            return row
    return None


def _snapshot_by_code(snapshot_code: str) -> dict[str, Any] | None:
    if not snapshot_code:
        return None
    for row in store.rows(SNAPSHOT_MODULE):
        if _as_text(row.get("快照ID")) == snapshot_code:
            return row
    return None


def _baseline_for(
    probe_id: Any,
    page_uri: str,
    exclude_id: int | None = None,
) -> dict[str, Any] | None:
    """取同一探针在确认序上最近的一份样本作为基线。

    页面相同但探针不同（价格/库存）的样本绝不会进入这里；并发提交由锁
    串行化，因此「第一次确认的采样条目」会成为后续条目的基线。
    """
    candidates: list[dict[str, Any]] = []
    for row in store.rows(SNAPSHOT_MODULE):
        if exclude_id is not None and int(row.get("id", 0)) == exclude_id:
            continue
        if not _probe_matches(row, probe_id):
            continue
        if page_uri and _as_text(row.get("页面URI")) != page_uri:
            continue
        if not _confirmed(row):
            continue
        candidates.append(row)
    if not candidates:
        return None
    return max(
        candidates,
        key=lambda row: (int(row.get("确认序号", 0) or 0), int(row.get("id", 0) or 0)),
    )


def comparison_between(
    current: dict[str, Any],
    baseline: dict[str, Any] | None,
) -> dict[str, Any]:
    """生成当前快照与基线快照的差异明细，所有字段都可回溯两侧记录。"""
    current_dom = _as_text(current.get("DOM哈希"))
    current_text = _as_text(current.get("文本哈希"))
    if baseline is None:
        previous_dom = ""
        previous_text = ""
        dom_changed = False
        text_changed = False
    else:
        previous_dom = _as_text(baseline.get("DOM哈希"))
        previous_text = _as_text(baseline.get("文本哈希"))
        dom_changed = current_dom != previous_dom
        text_changed = current_text != previous_text

    selectors = _as_text(current.get("选择器列表"))
    baseline_selectors = _as_text(baseline.get("选择器列表")) if baseline else ""
    return {
        "has_baseline": baseline is not None,
        "dom_changed": dom_changed,
        "text_changed": text_changed,
        "change_count": (1 if dom_changed else 0) + (1 if text_changed else 0),
        "changed": dom_changed or text_changed,
        "page_uri": current.get("页面URI", ""),
        "probe_id": current.get("探针ID"),
        "probe_name": current.get("探针名称", ""),
        "selectors": selectors,
        "captured_at": current.get("采样时间") or current.get("抓取时间", ""),
        "current_snapshot_id": current.get("id"),
        "current_snapshot_code": current.get("快照ID", ""),
        "current_observation_id": current.get("观测编号", ""),
        "current_dom_hash": current_dom,
        "current_text_hash": current_text,
        "current_content_fingerprint": current.get("内容指纹", ""),
        "compared_snapshot_id": baseline.get("id") if baseline else None,
        "compared_snapshot_code": baseline.get("快照ID", "") if baseline else "",
        "compared_observation_id": baseline.get("观测编号", "") if baseline else "",
        "baseline_snapshot_id": baseline.get("id") if baseline else None,
        "baseline_snapshot_code": baseline.get("快照ID", "") if baseline else "",
        "baseline_observation_id": baseline.get("观测编号", "") if baseline else "",
        "baseline_probe_id": baseline.get("探针ID") if baseline else None,
        "baseline_probe_name": baseline.get("探针名称", "") if baseline else "",
        "baseline_selectors": baseline_selectors,
        "baseline_captured_at": (
            baseline.get("采样时间") or baseline.get("抓取时间", "")
        ) if baseline else "",
        "baseline_dom_hash": previous_dom,
        "baseline_text_hash": previous_text,
        "previous_dom_hash": previous_dom,
        "previous_text_hash": previous_text,
        "selectors_changed": bool(baseline) and selectors != baseline_selectors,
        "probe_changed": bool(baseline) and not _probe_matches(baseline, current.get("探针ID")),
    }


def resolve_baseline_for_row(row: dict[str, Any]) -> dict[str, Any] | None:
    """优先沿用写入时记录的基线；缺失时按同一探针的确认序回查。"""
    baseline_id = row.get("基线快照记录ID")
    if baseline_id not in (None, ""):
        found = store.find(SNAPSHOT_MODULE, int(baseline_id))
        if found is not None and int(found.get("id", 0)) != int(row.get("id", 0)):
            return found
    baseline_code = _as_text(row.get("基线快照ID"))
    if baseline_code:
        found = _snapshot_by_code(baseline_code)
        if found is not None and int(found.get("id", 0)) != int(row.get("id", 0)):
            return found

    current_seq = int(row.get("确认序号", 0) or 0)
    current_id = int(row.get("id", 0) or 0)
    candidates: list[dict[str, Any]] = []
    for candidate in store.rows(SNAPSHOT_MODULE):
        if int(candidate.get("id", 0)) == current_id:
            continue
        if not _confirmed(candidate):
            continue
        if row.get("探针ID") not in (None, ""):
            if not _probe_matches(candidate, row.get("探针ID")):
                continue
            candidate_seq = int(candidate.get("确认序号", 0) or 0)
            if candidate_seq and current_seq and candidate_seq >= current_seq:
                continue
        else:
            # 历史示例数据没有探针归属，退化为同页面、主键在前的一条。
            if _as_text(candidate.get("页面URI")) != _as_text(row.get("页面URI")):
                continue
            if int(candidate.get("id", 0)) >= current_id:
                continue
        candidates.append(candidate)
    if not candidates:
        return None
    return max(
        candidates,
        key=lambda item: (int(item.get("确认序号", 0) or 0), int(item.get("id", 0) or 0)),
    )


def comparison_for_row(row: dict[str, Any]) -> dict[str, Any]:
    return comparison_between(row, resolve_baseline_for_row(row))


def _manifest_row_for(observation_id: str, probe_id: Any) -> dict[str, Any] | None:
    for row in store.rows(MANIFEST_MODULE):
        if (
            _as_text(row.get("观测编号")) == observation_id
            and _as_text(row.get("探针ID")) == _as_text(probe_id)
        ):
            return row
    return None


def _create_manifest_row(
    *,
    observation_id: str,
    probe_id: Any,
    probe_name: str,
    page_uri: str,
    selectors: str,
    dom_hash: str,
    text_hash: str,
    captured_at: str,
    received_at: str,
    snapshot_code: str,
    content_hash: str,
) -> dict[str, Any]:
    row: dict[str, Any] = {
        "id": _next_id(MANIFEST_MODULE),
        "status": "待写入",
        "pending": True,
        "abnormal": False,
        "观测编号": observation_id,
        "探针ID": probe_id,
        "探针名称": probe_name,
        "页面URI": page_uri,
        "选择器列表": selectors,
        "DOM哈希": dom_hash,
        "文本哈希": text_hash,
        "采样时间": captured_at,
        "接收时间": received_at,
        "内容指纹": content_hash,
        "是否写入": "否",
        "快照ID": "",
        "快照记录ID": None,
        "基线快照ID": "",
        "基线观测编号": "",
        # 英文别名，便于接口消费方直接读取
        "observation_id": observation_id,
        "probe_id": probe_id,
        "probe_name": probe_name,
        "page_uri": page_uri,
        "selectors": selectors,
        "dom_hash": dom_hash,
        "text_hash": text_hash,
        "captured_at": captured_at,
        "received_at": received_at,
        "written": False,
        "snapshot_id": None,
        "snapshot_code": snapshot_code,
    }
    store.rows(MANIFEST_MODULE).append(row)
    return row


def _write_manifest_result(
    manifest_row: dict[str, Any],
    snapshot: dict[str, Any],
    comparison: dict[str, Any],
) -> None:
    """快照写入成功后，把保存结果同步回采样清单。"""
    manifest_row.update({
        "status": "已写入",
        "pending": False,
        "是否写入": "是",
        "写入时间": snapshot.get("确认时间", ""),
        "快照ID": snapshot.get("快照ID", ""),
        "快照记录ID": snapshot.get("id"),
        "基线快照ID": snapshot.get("基线快照ID", ""),
        "基线观测编号": snapshot.get("基线观测编号", ""),
        "基线快照记录ID": snapshot.get("基线快照记录ID"),
        "是否变更": snapshot.get("是否变更", "否"),
        "写入结果": "已保存",
        "written": True,
        "snapshot_id": snapshot.get("id"),
        "snapshot_code": snapshot.get("快照ID", ""),
        "baseline_snapshot_id": snapshot.get("基线快照ID", ""),
        "baseline_observation_id": snapshot.get("基线观测编号", ""),
        "dom_changed": comparison.get("dom_changed", False),
        "text_changed": comparison.get("text_changed", False),
        "change_count": comparison.get("change_count", 0),
    })


def _normalize_observation(probe: dict[str, Any], payload: Any, *, generate: bool) -> dict[str, Any]:
    data = unwrap_payload(payload)
    probe_id = probe.get("id")
    page_uri = _as_text(_pick(
        data, "page_uri", "页面URI", "target_page", "目标页面", "page", "url",
        default=probe.get("目标页面", ""),
    ))
    selectors = _as_text(_pick(
        data, "selectors", "选择器列表", "selector_list", "selector", "选择器",
        default=probe.get("选择器列表", ""),
    ))
    dom_hash = _as_text(_pick(
        data, "dom_hash", "DOM哈希", "domHash", "dom", "dom_signature", "页面DOM哈希",
    ))
    text_hash = _as_text(_pick(
        data, "text_hash", "文本哈希", "textHash",
    ))
    captured_at = _as_text(_pick(
        data, "captured_at", "采样时间", "抓取时间", "capturedAt", "sampled_at",
        "sample_time", "timestamp", "采集时间",
    ))
    explicit = _pick(
        data, "observation_id", "观测编号", "observationId", "obs_id",
        "receipt_id", "回执编号", "receiptId", "observation_no", "观测号",
    )
    if isinstance(explicit, dict):
        explicit = _pick(explicit, "observation_id", "观测编号", "id", default="")

    if generate:
        trigger_nonce = str(next(_TRIGGER_SEQUENCE))
        if not captured_at:
            captured_at = now_iso()
        if not dom_hash:
            seed = f"{probe_id}|{page_uri}|{selectors}|{captured_at}|{trigger_nonce}"
            dom_hash = hashlib.sha256(seed.encode("utf-8")).hexdigest()[:16]
        if not text_hash:
            seed = f"{probe_id}|{page_uri}|{selectors}|{captured_at}|text|{trigger_nonce}"
            text_hash = hashlib.sha256(seed.encode("utf-8")).hexdigest()[:16]
    else:
        trigger_nonce = ""

    content_hash = content_fingerprint(probe_id, page_uri, selectors, dom_hash, text_hash)
    observation_id = observation_id_for(
        probe_id, page_uri, selectors, dom_hash, text_hash, captured_at, explicit,
    )
    # 显式编号被复用但内容不同：追加内容指纹，避免编号与内容再次错位。
    existing = _snapshot_by_observation(probe_id, observation_id)
    if existing is not None and _as_text(existing.get("内容指纹")) != content_hash:
        observation_id = f"{observation_id}-{content_hash[:8]}"
        existing = _snapshot_by_observation(probe_id, observation_id)
    existing_manifest = _manifest_row_for(observation_id, probe_id)
    if existing_manifest is not None and _as_text(existing_manifest.get("内容指纹")) != content_hash:
        observation_id = f"{observation_id}-{content_hash[:8]}"
        existing_manifest = _manifest_row_for(observation_id, probe_id)
    # 主动触发采样时即使内容与时间完全相同也要产生新观测，用触发序号区分。
    if generate and (existing is not None or existing_manifest is not None):
        observation_id = f"{observation_id}-{trigger_nonce}"

    page_size = _as_text(_pick(data, "page_size", "页面大小", "size", default="45.2KB"))
    return {
        "probe_id": probe_id,
        "probe_name": _as_text(probe.get("探针名称", "")),
        "page_uri": page_uri,
        "selectors": selectors,
        "dom_hash": dom_hash,
        "text_hash": text_hash,
        "captured_at": captured_at,
        "observation_id": observation_id,
        "content_hash": content_hash,
        "page_size": page_size,
        "received_at": now_iso(),
    }


def _result_from_snapshot(
    snapshot: dict[str, Any],
    *,
    duplicate: bool,
    message: str = "",
) -> dict[str, Any]:
    comparison = comparison_for_row(snapshot)
    manifest_row = _manifest_row_for(
        _as_text(snapshot.get("观测编号")), snapshot.get("探针ID"))
    if manifest_row is not None and not manifest_row.get("written"):
        _write_manifest_result(manifest_row, snapshot, comparison)
    observation = {
        "observation_id": snapshot.get("观测编号", ""),
        "观测编号": snapshot.get("观测编号", ""),
        "probe_id": snapshot.get("探针ID"),
        "probe_name": snapshot.get("探针名称", ""),
        "page_uri": snapshot.get("页面URI", ""),
        "selectors": snapshot.get("选择器列表", ""),
        "dom_hash": snapshot.get("DOM哈希", ""),
        "text_hash": snapshot.get("文本哈希", ""),
        "captured_at": snapshot.get("采样时间") or snapshot.get("抓取时间", ""),
        "content_fingerprint": snapshot.get("内容指纹", ""),
        "snapshot_id": snapshot.get("id"),
        "snapshot_code": snapshot.get("快照ID", ""),
        "baseline_snapshot_id": snapshot.get("基线快照ID", ""),
        "baseline_observation_id": snapshot.get("基线观测编号", ""),
        "dom_changed": comparison.get("dom_changed", False),
        "text_changed": comparison.get("text_changed", False),
    }
    receipt = {
        "observation_id": observation["observation_id"],
        "probe_id": observation["probe_id"],
        "probe_name": observation["probe_name"],
        "page_uri": observation["page_uri"],
        "selectors": observation["selectors"],
        "dom_hash": observation["dom_hash"],
        "text_hash": observation["text_hash"],
        "captured_at": observation["captured_at"],
        "snapshot_id": observation["snapshot_id"],
        "snapshot_code": observation["snapshot_code"],
    }
    return {
        "ok": True,
        "message": message or (
            "观测已存在，返回已确认快照" if duplicate else "快照已独立写入并回写采样清单"
        ),
        "duplicate": duplicate,
        "probe_id": snapshot.get("探针ID"),
        "probe_name": snapshot.get("探针名称", ""),
        "target_page": snapshot.get("页面URI", ""),
        "page_uri": snapshot.get("页面URI", ""),
        "observation_id": observation["observation_id"],
        "观测编号": observation["observation_id"],
        "snapshot_id": snapshot.get("id"),
        "snapshot_code": snapshot.get("快照ID", ""),
        "快照ID": snapshot.get("快照ID", ""),
        "captured_at": observation["captured_at"],
        "采样时间": observation["captured_at"],
        "dom_hash": observation["dom_hash"],
        "text_hash": observation["text_hash"],
        "snapshot": snapshot,
        "entry": snapshot,
        "observation": observation,
        "receipt": receipt,
        "manifest": manifest_row,
        "baseline": {
            "snapshot_id": comparison.get("baseline_snapshot_id"),
            "snapshot_code": comparison.get("baseline_snapshot_code", ""),
            "observation_id": comparison.get("baseline_observation_id", ""),
            "probe_id": comparison.get("baseline_probe_id"),
            "captured_at": comparison.get("baseline_captured_at", ""),
            "dom_hash": comparison.get("baseline_dom_hash", ""),
            "text_hash": comparison.get("baseline_text_hash", ""),
        } if comparison.get("has_baseline") else None,
        "comparison": comparison,
        "snapshot_request": {
            "快照ID": snapshot.get("快照ID", ""),
            "页面URI": snapshot.get("页面URI", ""),
            "DOM哈希": snapshot.get("DOM哈希", ""),
            "文本哈希": snapshot.get("文本哈希", ""),
            "抓取时间": observation["captured_at"],
            "页面大小": snapshot.get("页面大小", ""),
            "观测编号": observation["observation_id"],
            "探针ID": snapshot.get("探针ID"),
            "探针名称": snapshot.get("探针名称", ""),
            "选择器列表": snapshot.get("选择器列表", ""),
        },
    }


def stage_observation(probe: dict[str, Any], payload: Any = None) -> dict[str, Any]:
    """只登记采样清单、暂不写快照：用于接收端先入清单、异步落库的场景。"""
    with _LOCK:
        normalized = _normalize_observation(probe, payload, generate=False)
        existing = _snapshot_by_observation(normalized["probe_id"], normalized["observation_id"])
        if existing is not None:
            return _result_from_snapshot(existing, duplicate=True)
        manifest_row = _manifest_row_for(
            normalized["observation_id"], normalized["probe_id"])
        if manifest_row is None:
            manifest_row = _create_manifest_row(
                observation_id=normalized["observation_id"],
                probe_id=normalized["probe_id"],
                probe_name=normalized["probe_name"],
                page_uri=normalized["page_uri"],
                selectors=normalized["selectors"],
                dom_hash=normalized["dom_hash"],
                text_hash=normalized["text_hash"],
                captured_at=normalized["captured_at"],
                received_at=normalized["received_at"],
                snapshot_code=snapshot_code_for(
                    normalized["probe_id"], normalized["page_uri"], normalized["selectors"],
                    normalized["dom_hash"], normalized["text_hash"],
                    normalized["observation_id"], normalized["captured_at"],
                ),
                content_hash=normalized["content_hash"],
            )
        return {
            "ok": True,
            "message": "观测已登记采样清单，等待写入快照台账",
            "staged": True,
            "observation_id": normalized["observation_id"],
            "probe_id": normalized["probe_id"],
            "page_uri": normalized["page_uri"],
            "manifest": manifest_row,
            "snapshot_id": None,
        }


def submit_observation(
    probe: dict[str, Any],
    payload: Any = None,
    *,
    generate: bool = False,
) -> dict[str, Any]:
    """提交一条探针观测：幂等写入快照，并同步采样清单。"""
    with _LOCK:
        normalized = _normalize_observation(probe, payload, generate=generate)
        probe_id = normalized["probe_id"]
        observation_id = normalized["observation_id"]

        existing = _snapshot_by_observation(probe_id, observation_id)
        if existing is not None:
            return _result_from_snapshot(existing, duplicate=True)

        manifest_row = _manifest_row_for(observation_id, probe_id)
        if manifest_row is None:
            manifest_row = _create_manifest_row(
                observation_id=observation_id,
                probe_id=probe_id,
                probe_name=normalized["probe_name"],
                page_uri=normalized["page_uri"],
                selectors=normalized["selectors"],
                dom_hash=normalized["dom_hash"],
                text_hash=normalized["text_hash"],
                captured_at=normalized["captured_at"],
                received_at=normalized["received_at"],
                snapshot_code=snapshot_code_for(
                    probe_id, normalized["page_uri"], normalized["selectors"],
                    normalized["dom_hash"], normalized["text_hash"],
                    observation_id, normalized["captured_at"],
                ),
                content_hash=normalized["content_hash"],
            )

        # 并发提交在锁内串行化：先拿到确认序的条目先落库，后来者以它为基线。
        sequence = next(_CONFIRM_SEQUENCE)
        baseline = _baseline_for(
            probe_id, normalized["page_uri"], exclude_id=None)
        snapshot_code = snapshot_code_for(
            probe_id, normalized["page_uri"], normalized["selectors"],
            normalized["dom_hash"], normalized["text_hash"],
            observation_id, normalized["captured_at"],
        )
        snapshot = {
            "id": _next_id(SNAPSHOT_MODULE),
            "status": "已比对" if baseline is not None else "已保存",
            "pending": False,
            "abnormal": False,
            "快照ID": snapshot_code,
            "页面URI": normalized["page_uri"],
            "DOM哈希": normalized["dom_hash"],
            "文本哈希": normalized["text_hash"],
            "抓取时间": normalized["captured_at"],
            "页面大小": normalized["page_size"],
            "是否变更": "否",
            "观测编号": observation_id,
            "探针ID": probe_id,
            "探针名称": normalized["probe_name"],
            "选择器列表": normalized["selectors"],
            "采样时间": normalized["captured_at"],
            "接收时间": normalized["received_at"],
            "确认时间": normalized["received_at"],
            "确认": True,
            "确认序号": sequence,
            "内容指纹": normalized["content_hash"],
            "基线快照ID": baseline.get("快照ID", "") if baseline else "",
            "基线观测编号": baseline.get("观测编号", "") if baseline else "",
            "基线快照记录ID": baseline.get("id") if baseline else None,
            "基线探针ID": baseline.get("探针ID") if baseline else None,
            "基线采样时间": (
                baseline.get("采样时间") or baseline.get("抓取时间", "")
            ) if baseline else "",
            # 英文别名
            "observation_id": observation_id,
            "probe_id": probe_id,
            "probe_name": normalized["probe_name"],
            "page_uri": normalized["page_uri"],
            "dom_hash": normalized["dom_hash"],
            "text_hash": normalized["text_hash"],
            "captured_at": normalized["captured_at"],
            "snapshot_code": snapshot_code,
            "content_fingerprint": normalized["content_hash"],
        }
        store.rows(SNAPSHOT_MODULE).append(snapshot)

        comparison = comparison_between(snapshot, baseline)
        snapshot.update({
            "DOM变化": comparison["dom_changed"],
            "文本变化": comparison["text_changed"],
            "变化数量": comparison["change_count"],
            "是否变更": "是" if comparison["changed"] else "否",
            "abnormal": comparison["changed"],
            "变化明细": comparison,
            "dom_changed": comparison["dom_changed"],
            "text_changed": comparison["text_changed"],
            "baseline_snapshot_id": comparison["baseline_snapshot_id"],
            "baseline_observation_id": comparison["baseline_observation_id"],
            "compared_snapshot_id": comparison["compared_snapshot_id"],
            "compared_observation_id": comparison["compared_observation_id"],
        })
        _write_manifest_result(manifest_row, snapshot, comparison)
        return _result_from_snapshot(snapshot, duplicate=False)


def refresh_snapshot(snapshot_id: int) -> dict[str, Any]:
    """按写入时确认的基线对重新计算差异，刷新后结果与确认的快照对一致。"""
    with _LOCK:
        snapshot = store.find(SNAPSHOT_MODULE, snapshot_id)
        if snapshot is None:
            return {"ok": False, "reason": "快照不存在"}
        baseline = resolve_baseline_for_row(snapshot)
        comparison = comparison_between(snapshot, baseline)
        snapshot.update({
            "DOM变化": comparison["dom_changed"],
            "文本变化": comparison["text_changed"],
            "变化数量": comparison["change_count"],
            "是否变更": "是" if comparison["changed"] else "否",
            "abnormal": comparison["changed"],
            "变化明细": comparison,
            "dom_changed": comparison["dom_changed"],
            "text_changed": comparison["text_changed"],
            "compared_snapshot_id": comparison["compared_snapshot_id"],
            "compared_observation_id": comparison["compared_observation_id"],
        })
        if comparison["has_baseline"] and snapshot.get("status") == "已保存":
            snapshot["status"] = "已比对"
        manifest_row = _manifest_row_for(
            _as_text(snapshot.get("观测编号")), snapshot.get("探针ID"))
        if manifest_row is not None:
            _write_manifest_result(manifest_row, snapshot, comparison)
        return {
            "ok": True,
            "snapshot_id": snapshot_id,
            "observation_id": snapshot.get("观测编号", ""),
            "page_uri": snapshot.get("页面URI", ""),
            "comparison": comparison,
            "baseline": _baseline_summary(baseline),
            "snapshot": snapshot,
        }


def _baseline_summary(baseline: dict[str, Any] | None) -> dict[str, Any] | None:
    if baseline is None:
        return None
    return {
        "snapshot_id": baseline.get("id"),
        "snapshot_code": baseline.get("快照ID", ""),
        "observation_id": baseline.get("观测编号", ""),
        "probe_id": baseline.get("探针ID"),
        "probe_name": baseline.get("探针名称", ""),
        "captured_at": baseline.get("采样时间") or baseline.get("抓取时间", ""),
        "dom_hash": baseline.get("DOM哈希", ""),
        "text_hash": baseline.get("文本哈希", ""),
        "selectors": baseline.get("选择器列表", ""),
    }


def snapshot_detail(snapshot_id: int) -> dict[str, Any]:
    """快照详情：返回三次观测的编号以及每条观测实际比较的快照。"""
    with _LOCK:
        snapshot = store.find(SNAPSHOT_MODULE, snapshot_id)
        if snapshot is None:
            return {"ok": False, "reason": "快照不存在"}
        page_uri = _as_text(snapshot.get("页面URI"))
        all_rows = [
            row for row in store.rows(SNAPSHOT_MODULE)
            if _as_text(row.get("页面URI")) == page_uri
        ]
        all_rows.sort(key=lambda row: (int(row.get("确认序号", 0) or 0), int(row.get("id", 0) or 0)))
        observations: list[dict[str, Any]] = []
        for row in all_rows:
            comparison = comparison_for_row(row)
            observations.append({
                "observation_id": row.get("观测编号", ""),
                "观测编号": row.get("观测编号", ""),
                "probe_id": row.get("探针ID"),
                "probe_name": row.get("探针名称", ""),
                "selectors": row.get("选择器列表", ""),
                "snapshot_id": row.get("id"),
                "snapshot_code": row.get("快照ID", ""),
                "快照ID": row.get("快照ID", ""),
                "captured_at": row.get("采样时间") or row.get("抓取时间", ""),
                "采样时间": row.get("采样时间") or row.get("抓取时间", ""),
                "dom_hash": row.get("DOM哈希", ""),
                "text_hash": row.get("文本哈希", ""),
                "compared_snapshot_id": comparison.get("compared_snapshot_id"),
                "compared_observation_id": comparison.get("compared_observation_id", ""),
                "compared_snapshot_code": comparison.get("compared_snapshot_code", ""),
                "baseline_snapshot_id": comparison.get("baseline_snapshot_id"),
                "baseline_observation_id": comparison.get("baseline_observation_id", ""),
                "dom_changed": comparison.get("dom_changed", False),
                "text_changed": comparison.get("text_changed", False),
                "change_count": comparison.get("change_count", 0),
            })

        current_comparison = comparison_for_row(snapshot)
        probe_rows = [
            row for row in all_rows
            if _as_text(row.get("探针ID")) == _as_text(snapshot.get("探针ID"))
        ]
        return {
            "ok": True,
            "snapshot_id": snapshot_id,
            "snapshot_code": snapshot.get("快照ID", ""),
            "快照ID": snapshot.get("快照ID", ""),
            "observation_id": snapshot.get("观测编号", ""),
            "观测编号": snapshot.get("观测编号", ""),
            "probe_id": snapshot.get("探针ID"),
            "probe_name": snapshot.get("探针名称", ""),
            "selectors": snapshot.get("选择器列表", ""),
            "page_uri": page_uri,
            "页面URI": page_uri,
            "captured_at": snapshot.get("采样时间") or snapshot.get("抓取时间", ""),
            "采样时间": snapshot.get("采样时间") or snapshot.get("抓取时间", ""),
            "dom_hash": snapshot.get("DOM哈希", ""),
            "text_hash": snapshot.get("文本哈希", ""),
            "comparison": current_comparison,
            "变化明细": current_comparison,
            "baseline": _baseline_summary(resolve_baseline_for_row(snapshot)),
            "compared_snapshot_id": current_comparison.get("compared_snapshot_id"),
            "compared_observation_id": current_comparison.get("compared_observation_id", ""),
            "confirmed_snapshot_pair": {
                "current": {
                    "snapshot_id": snapshot.get("id"),
                    "observation_id": snapshot.get("观测编号", ""),
                    "probe_id": snapshot.get("探针ID"),
                    "captured_at": snapshot.get("采样时间") or snapshot.get("抓取时间", ""),
                    "dom_hash": snapshot.get("DOM哈希", ""),
                },
                "baseline": _baseline_summary(resolve_baseline_for_row(snapshot)),
            },
            "observations": observations,
            "observation_count": len(observations),
            "probe_observations": [
                item for item in observations
                if _as_text(item.get("probe_id")) == _as_text(snapshot.get("探针ID"))
            ],
            "probe_observation_count": len(probe_rows),
            "snapshot": snapshot,
            "entry": snapshot,
        }


def list_manifest(
    probe_id: Any = None,
    *,
    pending_only: bool = False,
) -> dict[str, Any]:
    rows = list(store.rows(MANIFEST_MODULE))
    if probe_id not in (None, ""):
        expected = _as_text(probe_id)
        probe = resolve_probe(probe_id)
        candidates = {expected}
        if probe is not None:
            candidates.add(_as_text(probe.get("id")))
        rows = [row for row in rows if _as_text(row.get("探针ID")) in candidates]
    if pending_only:
        rows = [row for row in rows if not row.get("written")]
    rows.sort(key=lambda row: (int(row.get("id", 0) or 0),))
    return {
        "ok": True,
        "items": rows,
        "rows": rows,
        "total": len(rows),
        "pending": sum(1 for row in rows if not row.get("written")),
        "observation_ids": [row.get("观测编号") for row in rows],
    }


def pending_observations(probe_id: Any = None) -> list[dict[str, Any]]:
    return list_manifest(probe_id, pending_only=True)["items"]


def recover_pending(probe_id: Any = None) -> dict[str, Any]:
    """探针选择根据采样清单找回尚未写入的观测，并补写到快照台账。"""
    recovered: list[dict[str, Any]] = []
    failed: list[dict[str, Any]] = []
    with _LOCK:
        for manifest_row in list(store.rows(MANIFEST_MODULE)):
            if manifest_row.get("written"):
                continue
            if probe_id not in (None, ""):
                expected = {_as_text(probe_id)}
                probe = resolve_probe(probe_id)
                if probe is not None:
                    expected.add(_as_text(probe.get("id")))
                if _as_text(manifest_row.get("探针ID")) not in expected:
                    continue
            probe = resolve_probe(manifest_row.get("探针ID")) or {
                "id": manifest_row.get("探针ID"),
                "探针名称": manifest_row.get("探针名称", ""),
                "目标页面": manifest_row.get("页面URI", ""),
                "选择器列表": manifest_row.get("选择器列表", ""),
            }
            payload = {
                "observation_id": manifest_row.get("观测编号"),
                "dom_hash": manifest_row.get("DOM哈希"),
                "text_hash": manifest_row.get("文本哈希"),
                "captured_at": manifest_row.get("采样时间"),
                "page_uri": manifest_row.get("页面URI"),
                "selectors": manifest_row.get("选择器列表"),
            }
            try:
                result = submit_observation(probe, payload)
            except Exception as exc:  # pragma: no cover - 防御性兜底
                failed.append({"observation_id": manifest_row.get("观测编号"), "reason": str(exc)})
                continue
            if result.get("ok"):
                recovered.append(result)
            else:
                failed.append({"observation_id": manifest_row.get("观测编号"), "reason": result.get("reason")})
    return {
        "ok": True,
        "recovered": recovered,
        "recovered_count": len(recovered),
        "failed": failed,
        "failed_count": len(failed),
        "pending": list_manifest(probe_id, pending_only=True)["items"],
    }


def manifest_ledger_consistency() -> dict[str, Any]:
    """核对采样清单和快照台账里的观测标识是否一致。"""
    mismatches: list[dict[str, Any]] = []
    written = [row for row in store.rows(MANIFEST_MODULE) if row.get("written")]
    ledger_by_observation = {
        _as_text(row.get("观测编号")): row for row in store.rows(SNAPSHOT_MODULE)
        if _as_text(row.get("观测编号"))
    }
    manifest_by_observation = {
        _as_text(row.get("观测编号")): row for row in store.rows(MANIFEST_MODULE)
        if _as_text(row.get("观测编号"))
    }
    for manifest_row in written:
        observation_id = _as_text(manifest_row.get("观测编号"))
        snapshot = ledger_by_observation.get(observation_id)
        if snapshot is None:
            mismatches.append({
                "observation_id": observation_id,
                "reason": "清单已写入但台账缺少对应观测",
            })
            continue
        snapshot_id = manifest_row.get("快照记录ID")
        if snapshot_id is not None and int(snapshot_id) != int(snapshot.get("id", 0)):
            mismatches.append({
                "observation_id": observation_id,
                "reason": "清单与台账的快照记录ID不一致",
                "manifest_snapshot_id": snapshot_id,
                "ledger_snapshot_id": snapshot.get("id"),
            })
        if _as_text(manifest_row.get("快照ID")) != _as_text(snapshot.get("快照ID")):
            mismatches.append({
                "observation_id": observation_id,
                "reason": "清单与台账的快照ID不一致",
                "manifest_snapshot_code": manifest_row.get("快照ID"),
                "ledger_snapshot_code": snapshot.get("快照ID"),
            })
        if _as_text(manifest_row.get("探针ID")) != _as_text(snapshot.get("探针ID")):
            mismatches.append({
                "observation_id": observation_id,
                "reason": "清单与台账的探针归属不一致",
                "manifest_probe_id": manifest_row.get("探针ID"),
                "ledger_probe_id": snapshot.get("探针ID"),
            })
    for observation_id, snapshot in ledger_by_observation.items():
        if snapshot.get("探针ID") in (None, ""):
            continue
        if observation_id not in manifest_by_observation:
            mismatches.append({
                "observation_id": observation_id,
                "reason": "台账里的探针观测缺少采样清单记录",
            })
    return {
        "ok": not mismatches,
        "mismatches": mismatches,
        "mismatch_count": len(mismatches),
        "manifest_count": len(store.rows(MANIFEST_MODULE)),
        "written_count": len(written),
    }


def create_snapshot_from_values(values: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
    """通用登记入口的适配：有探针归属时走独立观测写入，否则保留旧台账行为。"""
    data = unwrap_payload(values)
    probe_ref = _pick(data, "探针ID", "probe_id", "探针名称", "probe_name", "probe")
    probe = resolve_probe(probe_ref)
    if probe is not None:
        result = submit_observation(probe, data, generate=False)
        if not result.get("ok"):
            return None, [result.get("reason", "快照写入失败")]
        return result["snapshot"], []

    missing = [
        field for field in ("快照ID", "页面URI", "DOM哈希")
        if not _as_text(data.get(field)).strip()
    ]
    if missing:
        return None, missing

    rows = store.rows(SNAPSHOT_MODULE)
    entry: dict[str, Any] = {
        "id": max((int(row.get("id", 0)) for row in rows), default=0) + 1,
    }
    from app.services.page_snapshot import PageSnapshotService  # 局部导入避免循环

    for field in PageSnapshotService.LIST_FIELDS:
        entry[field] = data.get(field)
    entry["status"] = PageSnapshotService.STATUS_ORDER[0]
    entry["pending"] = True
    entry["abnormal"] = False
    rows.append(entry)
    return entry, []
