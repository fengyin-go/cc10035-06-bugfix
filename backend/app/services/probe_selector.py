"""探针选择领域服务：管理页面元素的自动化探测探针。"""
from __future__ import annotations

from typing import Any

from app.services import probe_sampling
from app.services.base import BaseModuleService
from app.store import store


class ProbeSelectorService(BaseModuleService):
    MODULE = "probe_selector"
    ENTITY = "探针配置"
    OPERATION_TERM = "校验"
    LIST_FIELDS = ["探针名称", "目标页面", "选择器列表", "验证关键字", "采样频率", "超时秒数", "重试次数"]
    REQUIRED_FIELDS = ["探针名称", "目标页面", "选择器列表"]
    STATUS_ORDER = ["草稿", "探测中", "已验证", "已废弃"]
    ACTION_RULES = {"启动探测": "草稿", "验证探针": "探测中", "废弃探针": "已验证"}
    NEGATIVE_ACTIONS = []

    def _require_probe(self, probe_id: int) -> dict[str, Any] | None:
        return store.find(self.MODULE, probe_id)

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        """探针选择入口的动作：运行采样会生成一份新观测和独立快照。"""
        if action in (
            "运行采样", "触发采样", "触发快照", "重新采样", "运行探针",
        ):
            probe = self._require_probe(entry_id)
            if probe is None:
                return None, f"探针配置 {entry_id} 不存在或已归档"
            result = self.trigger_snapshot(entry_id)
            if not result.get("ok"):
                return None, str(result.get("reason"))
            probe["status"] = "探测中"
            probe["最近采样时间"] = result.get("captured_at", "")
            probe["最近观测编号"] = result.get("observation_id", "")
            probe["最近快照ID"] = result.get("snapshot_code", "")
            return probe, f"探针已运行采样，生成观测 {result.get('observation_id')}"
        if action in ("提交回执", "提交观测"):
            return self._submit_receipt_action(entry_id, None)
        if action in ("找回观测", "补写观测", "找回未写入"):
            probe = self._require_probe(entry_id)
            if probe is None:
                return None, f"探针配置 {entry_id} 不存在或已归档"
            result = self.recover_observations(entry_id)
            return probe, (
                f"已从采样清单找回并写入 {result['recovered_count']} 条观测"
            )
        return super().run_action(entry_id, action)

    def _submit_receipt_action(
        self,
        entry_id: int,
        payload: Any,
    ) -> tuple[dict[str, Any] | None, str]:
        probe = self._require_probe(entry_id)
        if probe is None:
            return None, f"探针配置 {entry_id} 不存在或已归档"
        if payload is None:
            return probe, "请在 /api/probe_selector/{id}/observations 提交回执内容"
        values = payload.merged_values() if hasattr(payload, "merged_values") else payload
        if not isinstance(values, dict):
            values = {}
        result = self.submit_receipt(entry_id, values)
        if not result.get("ok"):
            return None, str(result.get("reason"))
        return result.get("snapshot"), f"探针回执已写入观测 {result.get('observation_id')}"

    def run_action_with_payload(
        self,
        entry_id: int,
        action: str,
        payload: Any = None,
    ) -> tuple[dict[str, Any] | None, str]:
        """「提交回执」可以带请求体，直接把观测写进快照台账。"""
        if action in ("提交回执", "提交观测"):
            return self._submit_receipt_action(entry_id, payload)
        return self.run_action(entry_id, action)

    def get_entry(self, entry_id: int) -> dict[str, Any] | None:
        """详情接口附带该探针的采样清单、未写入观测和最近一次观测。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return None
        result = dict(entry)
        manifest = probe_sampling.list_manifest(entry_id)
        result["采样清单"] = manifest["items"]
        result["未写入观测"] = [row for row in manifest["items"] if not row.get("written")]
        observations = [
            row for row in store.rows("page_snapshot")
            if str(row.get("探针ID", "")) == str(entry_id)
        ]
        observations.sort(key=lambda row: int(row.get("确认序号", 0) or 0))
        result["观测记录"] = observations
        result["最近观测编号"] = observations[-1].get("观测编号", "") if observations else ""
        return result

    def trigger_snapshot(self, probe_id: int, payload: Any = None) -> dict[str, Any]:
        """触发页面快照：探针→快照的跨模块联动。被页面快照消费。

        每次触发都会生成一份独立的观测编号和快照ID：标识由探针、选择器、
        DOM/文本哈希与采样时间共同决定，不再只依赖目标页面。
        """
        probe = self._require_probe(probe_id)
        if probe is None:
            return {"ok": False, "reason": "探针不存在"}
        result = probe_sampling.submit_observation(probe, payload, generate=True)
        if not result.get("ok"):
            return result
        result["triggered"] = True
        result["triggered_at"] = result.get("captured_at")
        return result

    def submit_receipt(self, probe_id: int, payload: Any = None) -> dict[str, Any]:
        """提交一份探针观测回执，独立写入采集快照数据集。"""
        probe = self._require_probe(probe_id)
        if probe is None:
            return {"ok": False, "reason": "探针不存在"}
        return probe_sampling.submit_observation(probe, payload, generate=False)

    def submit_observation(self, probe_id: int, payload: Any = None) -> dict[str, Any]:
        return self.submit_receipt(probe_id, payload)

    def stage_receipt(self, probe_id: int, payload: Any = None) -> dict[str, Any]:
        """回执先登记到采样清单，稍后由 recover 找回并补写快照。"""
        probe = self._require_probe(probe_id)
        if probe is None:
            return {"ok": False, "reason": "探针不存在"}
        return probe_sampling.stage_observation(probe, payload)

    def submit_receipt_by_ref(self, probe_ref: Any, payload: Any = None) -> dict[str, Any]:
        """按探针主键或名称提交，便于从回执里的 probe_id 直接回送。"""
        probe = probe_sampling.resolve_probe(probe_ref)
        if probe is None:
            return {"ok": False, "reason": "探针不存在"}
        return probe_sampling.submit_observation(probe, payload, generate=False)

    def pending_observations(self, probe_id: int | None = None) -> dict[str, Any]:
        """采样清单中尚未写入快照台账的观测。"""
        return probe_sampling.list_manifest(probe_id, pending_only=True)

    def sampling_manifest(self, probe_id: int | None = None) -> dict[str, Any]:
        return probe_sampling.list_manifest(probe_id)

    def recover_observations(self, probe_id: int | None = None) -> dict[str, Any]:
        """根据采样清单找回尚未写入的观测并补写快照。"""
        return probe_sampling.recover_pending(probe_id)

    def manifest_consistency(self) -> dict[str, Any]:
        return probe_sampling.manifest_ledger_consistency()

    def compute_probe_health(self) -> dict[str, Any]:
        """探针健康度计算，供聚合视图和抽取指标消费。"""
        rows = store.rows(self.MODULE)
        valid = sum(1 for r in rows if r.get("status") == "已验证")
        active = sum(1 for r in rows if r.get("status") == "探测中")
        dead = sum(1 for r in rows if r.get("status") == "已废弃")
        total = len(rows)
        return {
            "total_probes": total,
            "valid_probes": valid,
            "active_probes": active,
            "dead_probes": dead,
            "health_ratio": round(valid / total, 2) if total > 0 else 0.0,
        }

    def get_probes_for_page(self, page_uri: str) -> list[dict[str, Any]]:
        """按目标页面过滤探针。"""
        return [
            {"id": r.get("id"), "name": r.get("探针名称"),
             "selectors": r.get("选择器列表"), "status": r.get("status")}
            for r in store.rows(self.MODULE)
            if r.get("目标页面") == page_uri
        ]

    def domain_snapshot(self) -> dict[str, Any]:
        base = super().domain_snapshot()
        base["probe_health"] = self.compute_probe_health()
        base["sampling_manifest"] = probe_sampling.list_manifest(pending_only=True)
        return base
