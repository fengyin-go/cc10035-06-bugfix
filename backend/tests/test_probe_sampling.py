"""探针观测 → 采集快照 → 采样清单 的回归测试。

只用标准库 ``unittest``，直接调用领域服务，不依赖 HTTP 客户端：

    cd backend && .venv/bin/python -m unittest discover -s tests -v
"""
from __future__ import annotations

import threading
import unittest

from app.services import probe_sampling
from app.services import SERVICES
from app.services.page_snapshot import PageSnapshotService
from app.services.probe_selector import ProbeSelectorService
from app.store import store

PAGE = "https://shop.example.com/item/42"


def new_probe(name: str, selectors: str, page: str = PAGE) -> dict:
    probe, missing = SERVICES["probe_selector"].create_entry({
        "探针名称": name,
        "目标页面": page,
        "选择器列表": selectors,
    })
    assert probe is not None and not missing
    return probe


class ObservationIdentityTest(unittest.TestCase):
    def setUp(self) -> None:
        self.probes = ProbeSelectorService()
        self.price = new_probe("价格探针", ".price")
        self.stock = new_probe("库存探针", ".stock")

    def test_same_page_different_probe_gets_distinct_ids(self) -> None:
        price = self.probes.submit_receipt(self.price["id"], {
            "dom_hash": "dom-price", "text_hash": "text-price",
            "captured_at": "2026-11-20T10:00:00.000001Z",
        })
        stock = self.probes.submit_receipt(self.stock["id"], {
            "dom_hash": "dom-stock", "text_hash": "text-stock",
            "captured_at": "2026-11-20T10:00:01.000001Z",
        })
        self.assertNotEqual(price["snapshot_code"], stock["snapshot_code"])
        self.assertNotEqual(price["observation_id"], stock["observation_id"])
        self.assertNotEqual(price["snapshot_id"], stock["snapshot_id"])

    def test_same_probe_different_dom_hash_gets_distinct_ids(self) -> None:
        first = self.probes.submit_receipt(self.price["id"], {
            "dom_hash": "dom-1", "text_hash": "text", "captured_at": "2026-11-20T10:00:00Z",
        })
        second = self.probes.submit_receipt(self.price["id"], {
            "dom_hash": "dom-2", "text_hash": "text", "captured_at": "2026-11-20T10:00:01Z",
        })
        self.assertNotEqual(first["snapshot_code"], second["snapshot_code"])
        self.assertNotEqual(first["observation_id"], second["observation_id"])

    def test_receipt_resubmission_is_idempotent(self) -> None:
        payload = {
            "dom_hash": "dom-1", "text_hash": "text", "captured_at": "2026-11-20T10:00:00Z",
        }
        first = self.probes.submit_receipt(self.price["id"], payload)
        again = self.probes.submit_receipt(self.price["id"], dict(payload))
        self.assertTrue(again["duplicate"])
        self.assertEqual(first["snapshot_id"], again["snapshot_id"])

    def test_explicit_observation_id_with_new_content_is_separated(self) -> None:
        first = self.probes.submit_receipt(self.price["id"], {
            "observation_id": "obs-fixed", "dom_hash": "dom-1",
            "captured_at": "2026-11-20T10:00:00Z",
        })
        second = self.probes.submit_receipt(self.price["id"], {
            "observation_id": "obs-fixed", "dom_hash": "dom-2",
            "captured_at": "2026-11-20T10:00:00Z",
        })
        self.assertNotEqual(first["observation_id"], second["observation_id"])
        self.assertTrue(second["observation_id"].startswith("obs-fixed"))


class BaselineTest(unittest.TestCase):
    def setUp(self) -> None:
        self.probes = ProbeSelectorService()
        self.snapshots = PageSnapshotService()
        self.price = new_probe("价格探针", ".price")
        self.stock = new_probe("库存探针", ".stock")

    def test_each_probe_compares_against_its_own_previous_sample(self) -> None:
        price1 = self.probes.submit_receipt(self.price["id"], {
            "dom_hash": "p1", "text_hash": "t1", "captured_at": "2026-11-20T10:00:00Z",
        })
        stock1 = self.probes.submit_receipt(self.stock["id"], {
            "dom_hash": "s1", "text_hash": "s1", "captured_at": "2026-11-20T10:00:01Z",
        })
        price2 = self.probes.submit_receipt(self.price["id"], {
            "dom_hash": "p2", "text_hash": "t1", "captured_at": "2026-11-20T10:01:00Z",
        })
        self.assertEqual(
            price2["comparison"]["compared_observation_id"], price1["observation_id"])
        self.assertNotEqual(
            price2["comparison"]["compared_observation_id"], stock1["observation_id"])

        stock2 = self.probes.submit_receipt(self.stock["id"], {
            "dom_hash": "s2", "text_hash": "s1", "captured_at": "2026-11-20T10:02:00Z",
        })
        comparison = stock2["comparison"]
        self.assertEqual(comparison["compared_observation_id"], stock1["observation_id"])
        self.assertTrue(comparison["dom_changed"])
        self.assertFalse(comparison["text_changed"])
        self.assertEqual(comparison["baseline_captured_at"], "2026-11-20T10:00:01Z")
        self.assertEqual(comparison["baseline_probe_id"], self.stock["id"])

    def test_refresh_keeps_confirmed_snapshot_pair(self) -> None:
        first = self.probes.submit_receipt(self.stock["id"], {
            "dom_hash": "s1", "text_hash": "s1", "captured_at": "2026-11-20T10:00:00Z",
        })
        second = self.probes.submit_receipt(self.stock["id"], {
            "dom_hash": "s2", "text_hash": "s2", "captured_at": "2026-11-20T10:05:00Z",
        })
        refreshed = self.snapshots.refresh_comparison(second["snapshot_id"])
        self.assertEqual(
            refreshed["comparison"]["compared_observation_id"], first["observation_id"])
        self.assertEqual(
            refreshed["comparison"]["current_snapshot_id"], second["snapshot_id"])
        self.assertEqual(
            refreshed["baseline"]["snapshot_id"], first["snapshot_id"])
        self.assertEqual(
            refreshed["baseline"]["captured_at"], "2026-11-20T10:00:00Z")

    def test_trigger_from_probe_entry_uses_latest_confirmed_sample(self) -> None:
        first = self.probes.trigger_snapshot(self.stock["id"])
        second = self.probes.trigger_snapshot(self.stock["id"])
        self.assertNotEqual(first["snapshot_code"], second["snapshot_code"])
        self.assertEqual(
            second["comparison"]["compared_observation_id"], first["observation_id"])
        detail = self.snapshots.snapshot_detail(second["snapshot_id"])
        stock_observations = [
            item for item in detail["observations"]
            if item["probe_id"] == self.stock["id"]
        ]
        self.assertEqual(len(stock_observations), 2)
        self.assertEqual(
            stock_observations[-1]["compared_observation_id"], first["observation_id"])
        self.assertEqual(
            stock_observations[-1]["compared_snapshot_id"], first["snapshot_id"])

    def test_interleaved_observations_report_three_ids_and_baselines(self) -> None:
        price1 = self.probes.submit_receipt(self.price["id"], {
            "dom_hash": "p1", "text_hash": "t1", "captured_at": "2026-11-20T10:00:00Z",
        })
        stock1 = self.probes.submit_receipt(self.stock["id"], {
            "dom_hash": "s1", "text_hash": "u1", "captured_at": "2026-11-20T10:01:00Z",
        })
        price2 = self.probes.submit_receipt(self.price["id"], {
            "dom_hash": "p2", "text_hash": "t2", "captured_at": "2026-11-20T10:02:00Z",
        })
        stock2 = self.probes.trigger_snapshot(self.stock["id"])

        detail = self.snapshots.snapshot_detail(stock2["snapshot_id"])
        stock_chain = [
            item for item in detail["observations"]
            if item["probe_id"] == self.stock["id"]
        ]
        self.assertEqual(
            [item["observation_id"] for item in stock_chain],
            [stock1["observation_id"], stock2["observation_id"]],
        )
        self.assertEqual(
            stock_chain[-1]["compared_observation_id"], stock1["observation_id"])
        self.assertEqual(
            stock_chain[-1]["compared_snapshot_id"], stock1["snapshot_id"])

        price_chain = [
            item for item in detail["observations"]
            if item["probe_id"] == self.price["id"]
        ]
        self.assertEqual(
            price_chain[-1]["compared_observation_id"], price1["observation_id"])
        self.assertEqual(
            price_chain[-1]["compared_snapshot_id"], price1["snapshot_id"])
        self.assertNotIn(
            price2["observation_id"], [item["observation_id"] for item in stock_chain])


class ConcurrencyTest(unittest.TestCase):
    def test_concurrent_submissions_chain_off_first_confirmed_entry(self) -> None:
        probe = new_probe("并发探针", ".x", page="https://shop.example.com/item/77")
        results: list[dict] = []

        def submit(index: int) -> None:
            results.append(probe_sampling.submit_observation(probe, {
                "dom_hash": f"dom-{index}",
                "text_hash": "text",
                "captured_at": f"2026-11-20T08:0{index}:00Z",
            }))

        threads = [threading.Thread(target=submit, args=(index,)) for index in range(6)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        self.assertEqual(len({item["snapshot_id"] for item in results}), 6)
        ordered = sorted(results, key=lambda item: item["snapshot"]["确认序号"])
        self.assertIsNone(ordered[0]["baseline"])
        for previous, current in zip(ordered, ordered[1:]):
            self.assertEqual(
                current["comparison"]["compared_observation_id"],
                previous["observation_id"],
            )
        self.assertTrue(probe_sampling.manifest_ledger_consistency()["ok"])


class SamplingManifestTest(unittest.TestCase):
    def setUp(self) -> None:
        self.probes = ProbeSelectorService()
        self.probe = new_probe("清单探针", ".m", page="https://shop.example.com/item/55")

    def test_snapshot_write_back_fills_manifest(self) -> None:
        result = self.probes.submit_receipt(self.probe["id"], {
            "dom_hash": "dom", "text_hash": "text", "captured_at": "2026-11-20T06:00:00Z",
        })
        manifest = self.probes.sampling_manifest(self.probe["id"])
        row = next(
            item for item in manifest["items"]
            if item["观测编号"] == result["observation_id"])
        self.assertEqual(row["是否写入"], "是")
        self.assertEqual(row["快照记录ID"], result["snapshot_id"])
        self.assertEqual(row["快照ID"], result["snapshot_code"])
        self.assertTrue(probe_sampling.manifest_ledger_consistency()["ok"])

    def test_pending_observation_is_recovered_by_probe_selection(self) -> None:
        staged = self.probes.stage_receipt(self.probe["id"], {
            "dom_hash": "dom-late", "text_hash": "text",
            "captured_at": "2026-11-20T06:10:00Z",
        })
        self.assertTrue(staged["staged"])
        pending = self.probes.pending_observations(self.probe["id"])
        self.assertIn(staged["observation_id"], pending["observation_ids"])

        recovered = self.probes.recover_observations(self.probe["id"])
        self.assertEqual(recovered["recovered_count"], 1)
        self.assertNotIn(
            staged["observation_id"],
            self.probes.pending_observations(self.probe["id"])["observation_ids"],
        )
        manifest = self.probes.sampling_manifest(self.probe["id"])
        row = next(
            item for item in manifest["items"]
            if item["观测编号"] == staged["observation_id"])
        self.assertEqual(row["是否写入"], "是")
        self.assertIsNotNone(row["快照记录ID"])


class LegacySnapshotTest(unittest.TestCase):
    def test_snapshot_without_probe_keeps_plain_registration(self) -> None:
        service = PageSnapshotService()
        entry, missing = service.create_entry({
            "快照ID": "snap-legacy",
            "页面URI": "https://legacy.example.com/1",
            "DOM哈希": "dom",
            "文本哈希": "text",
            "抓取时间": "2026-11-20",
            "页面大小": "10KB",
            "是否变更": "否",
        })
        self.assertFalse(missing)
        self.assertIsNotNone(entry)
        self.assertEqual(store.find("page_snapshot", entry["id"])["快照ID"], "snap-legacy")


if __name__ == "__main__":
    unittest.main()
