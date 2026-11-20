"""探针采样接口：观测提交、采样清单找回、快照详情与差异刷新。

固定路径必须注册在通用 ``/api/{module}`` 资源路由之前，避免
``probe_selector`` / ``page_snapshot`` 的二级路径被动态路由抢走。
"""
from __future__ import annotations

import json
from typing import Any

from fastapi import APIRouter, Body, Query, Request

from app.services import probe_sampling
from app.services.page_snapshot import PageSnapshotService
from app.services.probe_selector import ProbeSelectorService
from app.store import store

router = APIRouter(prefix="/api", tags=["探针采样"])

_probe_service = ProbeSelectorService()
_snapshot_service = PageSnapshotService()


async def _read_body(request: Request, body: Any) -> Any:
    if body is not None:
        return body
    raw = await request.body()
    if not raw:
        return {}
    try:
        return json.loads(raw)
    except ValueError:
        return {}


def _probe_ref_from(payload: Any) -> Any:
    data = probe_sampling.unwrap_payload(payload)
    return (
        data.get("probe_id")
        or data.get("探针ID")
        or data.get("probe_name")
        or data.get("探针名称")
    )


# ---- 探针选择入口：提交回执 / 运行采样 / 找回观测 -------------------------


@router.post("/probe_selector/observations")
async def submit_observation_by_body(
    request: Request,
    body: Any = Body(default=None),
) -> dict[str, Any]:
    """回执里带探针归属时，由本入口独立写入快照。"""
    payload = await _read_body(request, body)
    return _probe_service.submit_receipt_by_ref(_probe_ref_from(payload), payload)


@router.get("/probe_selector/manifest")
async def global_manifest(
    probe_id: int | None = Query(default=None),
    pending: bool = Query(default=False),
) -> dict[str, Any]:
    return probe_sampling.list_manifest(probe_id, pending_only=pending)


@router.get("/probe_selector/manifest/consistency")
async def manifest_consistency() -> dict[str, Any]:
    """核对采样清单与快照台账中的观测标识是否同步。"""
    return probe_sampling.manifest_ledger_consistency()


@router.post("/probe_selector/{probe_id}/observations")
async def submit_observation(
    probe_id: int,
    request: Request,
    body: Any = Body(default=None),
    defer: bool = Query(default=False),
) -> dict[str, Any]:
    """回执提交入口：探针观测独立写入快照台账并回写采样清单。

    ``defer=true`` 时只登记采样清单，等待探针选择入口找回后补写。
    """
    payload = await _read_body(request, body)
    if defer:
        return _probe_service.stage_receipt(probe_id, payload)
    return _probe_service.submit_receipt(probe_id, payload)


@router.post("/probe_selector/{probe_id}/receipts")
async def submit_receipt(
    probe_id: int,
    request: Request,
    body: Any = Body(default=None),
) -> dict[str, Any]:
    return _probe_service.submit_receipt(probe_id, await _read_body(request, body))


@router.post("/probe_selector/{probe_id}/trigger-snapshot")
async def trigger_snapshot(
    probe_id: int,
    request: Request,
    body: Any = Body(default=None),
) -> dict[str, Any]:
    """探针选择入口运行一次采样，生成新的观测与独立快照。"""
    return _probe_service.trigger_snapshot(probe_id, await _read_body(request, body))


@router.get("/sampling_manifest")
async def sampling_manifest(
    probe_id: int | None = Query(default=None),
    pending: bool = Query(default=False),
) -> dict[str, Any]:
    """采样清单只读入口：列出观测及其快照写入结果。"""
    return probe_sampling.list_manifest(probe_id, pending_only=pending)


@router.get("/sampling_manifest/consistency")
async def sampling_manifest_consistency() -> dict[str, Any]:
    return probe_sampling.manifest_ledger_consistency()


@router.get("/sampling_manifest/{probe_id}")
async def sampling_manifest_for_probe(
    probe_id: int,
    pending: bool = Query(default=False),
) -> dict[str, Any]:
    return probe_sampling.list_manifest(probe_id, pending_only=pending)


@router.post("/probe_selector/{probe_id}/snapshot")
@router.post("/probe_selector/{probe_id}/snapshots")
async def trigger_snapshot_alias(
    probe_id: int,
    request: Request,
    body: Any = Body(default=None),
) -> dict[str, Any]:
    return _probe_service.trigger_snapshot(probe_id, await _read_body(request, body))


@router.post("/probe_selector/{probe_id}/recover")
async def recover_observations(probe_id: int) -> dict[str, Any]:
    """按采样清单找回尚未写入的观测并补写快照。"""
    return _probe_service.recover_observations(probe_id)


@router.get("/probe_selector/{probe_id}/manifest")
async def probe_manifest(
    probe_id: int,
    pending: bool = Query(default=False),
) -> dict[str, Any]:
    if pending:
        return _probe_service.pending_observations(probe_id)
    return _probe_service.sampling_manifest(probe_id)


@router.get("/probe_selector/{probe_id}/pending")
async def probe_pending(probe_id: int) -> dict[str, Any]:
    return _probe_service.pending_observations(probe_id)


# ---- 页面快照入口：详情 / 差异 / 刷新 ------------------------------------


@router.get("/page_snapshot/observations")
async def list_page_observations(
    page_uri: str = Query(default="", alias="页面URI"),
    probe_id: int | None = Query(default=None),
) -> dict[str, Any]:
    rows = [
        row for row in store.rows(probe_sampling.SNAPSHOT_MODULE)
        if (not page_uri or str(row.get("页面URI", "")) == page_uri)
        and (probe_id is None or str(row.get("探针ID", "")) == str(probe_id))
    ]
    rows.sort(key=lambda row: (int(row.get("确认序号", 0) or 0), int(row.get("id", 0) or 0)))
    return {"ok": True, "items": rows, "total": len(rows)}


@router.post("/page_snapshot/observations")
async def submit_snapshot_observation(
    request: Request,
    body: Any = Body(default=None),
) -> dict[str, Any]:
    """按回执里的探针归属独立写入一份采集快照。"""
    payload = await _read_body(request, body)
    return _probe_service.submit_receipt_by_ref(_probe_ref_from(payload), payload)


@router.get("/page_snapshot/{snapshot_id}/detail")
async def snapshot_detail(snapshot_id: int) -> dict[str, Any]:
    """快照详情：同页面各次观测的编号与实际比较的快照。"""
    return _snapshot_service.snapshot_detail(snapshot_id)


@router.get("/page_snapshot/{snapshot_id}/comparison")
async def snapshot_comparison(snapshot_id: int) -> dict[str, Any]:
    return _snapshot_service.compare_snapshots(snapshot_id)


@router.post("/page_snapshot/{snapshot_id}/refresh")
async def refresh_comparison(snapshot_id: int) -> dict[str, Any]:
    """刷新变化明细：结果必须与写入时确认的快照对一致。"""
    return _snapshot_service.refresh_comparison(snapshot_id)
