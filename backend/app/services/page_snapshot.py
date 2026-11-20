"""页面快照领域服务：保存和比对页面状态快照。

快照的比对基线不再是「同页面上一条」，而是同一探针「实际确认」的前一份样本，
避免价格探针和库存探针在同一页面上互相错配。
"""
from __future__ import annotations

from typing import Any

from app.services import probe_sampling
from app.services.base import BaseModuleService
from app.store import store


class PageSnapshotService(BaseModuleService):
    MODULE = "page_snapshot"
    ENTITY = "快照记录"
    OPERATION_TERM = "采集"
    LIST_FIELDS = [
        "快照ID", "页面URI", "DOM哈希", "文本哈希", "抓取时间", "页面大小", "是否变更",
        "观测编号", "探针ID", "探针名称", "选择器列表",
    ]
    REQUIRED_FIELDS = ["快照ID", "页面URI", "DOM哈希"]
    STATUS_ORDER = ["已保存", "已比对", "已过期"]
    ACTION_RULES = {"执行比对": "已保存", "标记过期": "已比对"}
    NEGATIVE_ACTIONS = []

    def create_entry(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
        """带探针归属的登记走独立观测写入；否则按普通台账登记。"""
        return probe_sampling.create_snapshot_from_values(values)

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return None, f"{self.ENTITY} {entry_id} 不存在或已归档"
        if action == "执行比对":
            refresh = probe_sampling.refresh_snapshot(entry_id)
            if not refresh.get("ok"):
                return None, str(refresh.get("reason"))
            return entry, f"{self.ENTITY}已{action}，差异按确认基线重新计算"
        return super().run_action(entry_id, action)

    def get_entry(self, entry_id: int) -> dict[str, Any] | None:
        """详情接口在快照行上附带确认基线、差异明细和同页观测清单。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return None
        detail = probe_sampling.snapshot_detail(entry_id)
        result = dict(entry)
        if detail.get("ok"):
            result["比较基线"] = detail.get("baseline")
            result["变化明细"] = detail.get("comparison")
            result["观测清单"] = detail.get("observations", [])
            result["实际比较观测编号"] = detail.get("compared_observation_id", "")
        return result

    def compare_snapshots(self, snapshot_id: int) -> dict[str, Any]:
        """比对两次快照变化，供质量检查和探针选择联动。

        差异以同一探针确认的上一份快照为基线；没有基线时视为首次采集，
        不拿其它探针的样本充数。
        """
        snapshot = store.find(self.MODULE, snapshot_id)
        if snapshot is None:
            return {"ok": False, "reason": "快照不存在"}

        comparison = probe_sampling.comparison_for_row(snapshot)
        return {
            "ok": True,
            "snapshot_id": snapshot_id,
            "observation_id": snapshot.get("观测编号", ""),
            "page_uri": snapshot.get("页面URI"),
            "probe_id": snapshot.get("探针ID"),
            "probe_name": snapshot.get("探针名称", ""),
            "selectors": snapshot.get("选择器列表", ""),
            "captured_at": snapshot.get("采样时间") or snapshot.get("抓取时间", ""),
            "dom_changed": comparison["dom_changed"],
            "text_changed": comparison["text_changed"],
            "change_count": comparison["change_count"],
            "current_dom_hash": comparison["current_dom_hash"],
            "previous_dom_hash": comparison["previous_dom_hash"],
            "current_text_hash": comparison["current_text_hash"],
            "previous_text_hash": comparison["previous_text_hash"],
            "compared_snapshot_id": comparison["compared_snapshot_id"],
            "compared_snapshot_code": comparison["compared_snapshot_code"],
            "compared_observation_id": comparison["compared_observation_id"],
            "baseline_snapshot_id": comparison["baseline_snapshot_id"],
            "baseline_observation_id": comparison["baseline_observation_id"],
            "baseline_probe_id": comparison["baseline_probe_id"],
            "baseline_captured_at": comparison["baseline_captured_at"],
            "has_baseline": comparison["has_baseline"],
            "details": comparison,
        }

    def snapshot_detail(self, snapshot_id: int) -> dict[str, Any]:
        """快照详情：返回同页面观测编号与各自实际比较的快照。"""
        return probe_sampling.snapshot_detail(snapshot_id)

    def refresh_comparison(self, snapshot_id: int) -> dict[str, Any]:
        """刷新后返回的变化明细与写入时确认的快照对保持一致。"""
        return probe_sampling.refresh_snapshot(snapshot_id)

    def feed_quality_check(self, snapshot_id: int) -> dict[str, Any]:
        """将快照比对结果提供给质量检查。"""
        compare_result = self.compare_snapshots(snapshot_id)
        if not compare_result["ok"]:
            return compare_result

        return {
            "ok": True,
            "snapshot_id": snapshot_id,
            "page_uri": compare_result["page_uri"],
            "probe_id": compare_result["probe_id"],
            "observation_id": compare_result["observation_id"],
            "compared_observation_id": compare_result["compared_observation_id"],
            "has_changes": compare_result["change_count"] > 0,
            "check_items": [
                {"name": "DOM一致性", "passed": not compare_result["dom_changed"], "detail": "DOM哈希比对"},
                {"name": "文本一致性", "passed": not compare_result["text_changed"], "detail": "文本哈希比对"},
            ],
            "recommend_rule_update": compare_result["dom_changed"],
        }

    def compute_staleness(self) -> dict[str, Any]:
        """计算快照过时率。"""
        rows = store.rows(self.MODULE)
        total = len(rows)
        expired = sum(1 for r in rows if r.get("status") == "已过期")
        changed = sum(1 for r in rows if r.get("是否变更") == "是" or r.get("是否变更") == "true")
        return {
            "total_snapshots": total,
            "expired_count": expired,
            "changed_count": changed,
            "staleness_ratio": round(expired / total, 2) if total > 0 else 0.0,
        }

    def domain_snapshot(self) -> dict[str, Any]:
        base = super().domain_snapshot()
        base["staleness"] = self.compute_staleness()
        base["ledger_manifest_consistency"] = probe_sampling.manifest_ledger_consistency()
        return base
