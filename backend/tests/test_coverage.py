"""聚合缓存覆盖核验链路测试：分区/度量覆盖、批次一致性与复用。"""
from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from app.main import app
from app.services.aggregate_cache import AggregateCacheService
from app.services.cache_coverage import (
    CONFLICT,
    PARTIAL,
    PENDING,
    VERIFIED,
    plan_cube_read,
    verify_cube_coverage,
)
from app.services.olap_cube import OlapCubeService
from app.store import store

client = TestClient(app)


@pytest.fixture()
def cube_factory():
    svc = OlapCubeService()

    def make(
        partitions: list[str] | None = None,
        measures: str = "revenue,orders",
        dims: str = "region,category",
    ) -> dict:
        entry, missing = svc.create_entry({
            "立方编号": "TEST-COVG", "立方名称": "覆盖核验立方",
            "维度列表": dims, "度量列表": measures, "聚合类型": "SUM",
            "分区策略": "dt", "刷新周期": "3600", "数据行数": "0",
        })
        assert entry is not None, missing
        entry["分区列表"] = partitions or ["2026-11-01", "2026-11-02", "2026-11-03"]
        entry["清单版本"] = "v1"
        return entry

    return make


def _active_entries(cube_id: int) -> list[dict]:
    return [
        entry for entry in store.rows("aggregate_cache")
        if str(entry.get("目标立方")) == f"olap_cube:{cube_id}"
        and not entry.get("被整版替代")
    ]


class TestCoverageVerification:
    def test_full_batch_proves_partition_and_measure_coverage(self, cube_factory):
        svc = OlapCubeService()
        cube = cube_factory()
        build = svc.build_cube(cube["id"])
        assert build["ok"]

        result = svc.verify_coverage(cube["id"])
        assert result["ok"]
        batch = result["batch_entries"][0]
        assert batch["status"] == VERIFIED
        assert batch["complete"] is True
        assert batch["verified_partitions"] == cube["分区列表"]
        assert set(batch["covered_measures"]) == {"revenue", "orders"}

    def test_same_total_rows_cannot_hide_missing_partition(self, cube_factory):
        """两个分区行数相同且总行数对得上，但缓存缺一个分区仍不能算完整。"""
        svc = OlapCubeService()
        cube = cube_factory(["2026-11-01", "2026-11-02"])
        svc.build_cube(cube["id"])

        entry = _active_entries(cube["id"])[-1]
        original_total = entry["缓存行数"]
        removed = [g for g in entry["缓存清单"] if g["分区"] == "2026-11-02"]
        # 把缺失分区的行数补到另一个分区上，让总行数仍然一致。
        donor = next(g for g in entry["缓存清单"] if g["分区"] == "2026-11-01")
        donor["行数"] += sum(g["行数"] for g in removed)
        entry["缓存清单"] = [g for g in entry["缓存清单"] if g["分区"] != "2026-11-02"]
        entry["缓存行数"] = str(int(original_total))

        result = verify_cube_coverage(cube["id"])
        batch = result["batch_entries"][0]
        assert batch["complete"] is False
        assert "2026-11-02" in batch["reason"]
        assert batch["status"] in (PARTIAL, CONFLICT)

    def test_two_partitions_same_rows_one_misdirected_to_same_day(self, cube_factory):
        """两个缓存分区行数相同，其中一份误指向同一天：总行数相符也要判冲突。"""
        svc = OlapCubeService()
        cube = cube_factory(["2026-11-01", "2026-11-02"])
        svc.build_cube(cube["id"])

        entry = _active_entries(cube["id"])[-1]
        misdirected = [g for g in entry["缓存清单"] if g["分区"] == "2026-11-02"]
        donor_rows = sum(g["行数"] for g in misdirected)
        for group in misdirected:
            group["分区"] = "2026-11-01"
            group["分区标识"] = "2026-11-01"
        # 行数总量保持不变，仅看总计会误判为一致。
        assert int(entry["缓存行数"]) == sum(g["行数"] for g in entry["缓存清单"])

        result = svc.verify_coverage(cube["id"])
        batch = result["batch_entries"][0]
        assert batch["complete"] is False
        assert "缺少分区" in batch["reason"]
        assert donor_rows >= 0

        # 读回覆盖并核对立方总计。
        plan = svc.read_cube(cube["id"])
        assert "2026-11-02" in plan["uncovered_partitions"]
        assert plan["scope"] != "完整读取"
        batch_row = next(
            row for row in store.rows("cache_coverage_batch")
            if row["立方ID"] == cube["id"] and row["批次"] == batch["batch"]
        )
        assert batch_row["立方总计行数"] == int(cube["数据行数"])
        assert plan["cube_total"] == int(cube["数据行数"])

    def test_duplicate_groups_count_once(self, cube_factory):
        svc = OlapCubeService()
        cube = cube_factory(["2026-11-01"])
        svc.build_cube(cube["id"])

        entry = _active_entries(cube["id"])[-1]
        entry["缓存清单"] = entry["缓存清单"] + [dict(entry["缓存清单"][0])]

        result = svc.verify_coverage(cube["id"])
        batch = result["batch_entries"][0]
        assert batch["status"] == VERIFIED
        assert batch["complete"] is True

        row = next(
            row for row in store.rows("cache_coverage_batch")
            if row["立方ID"] == cube["id"] and row["批次"] == batch["batch"]
        )
        assert row["重复分组数"] >= 1
        assert row["已核验总行数"] == int(cube["数据行数"])


class TestBatchAndReuse:
    def test_manifest_batch_must_match_entry_batch(self, cube_factory):
        svc = OlapCubeService()
        cube = cube_factory(["2026-11-01"])
        svc.build_cube(cube["id"])

        entry = _active_entries(cube["id"])[-1]
        for group in entry["缓存清单"]:
            group["批次"] = "OTHER-BATCH"

        result = svc.verify_coverage(cube["id"])
        batch = result["batch_entries"][0]
        assert batch["status"] == PENDING
        assert "批次" in batch["reason"]

    def test_unchanged_groups_reuse_prior_verification(self, cube_factory):
        svc = OlapCubeService()
        cube = cube_factory(["2026-11-01"])
        svc.build_cube(cube["id"])

        # 预热签发另一份同源清单：分组签名不变，应复用已核验结果。
        prewarm = AggregateCacheService().prewarm_cube_cache(cube["id"])
        assert prewarm["ok"]
        result = svc.verify_coverage(cube["id"])

        batch_row = next(
            row for row in store.rows("cache_coverage_batch")
            if row["立方ID"] == cube["id"] and row["批次"] == prewarm["batch"]
        )
        assert batch_row["复用分组数"] == batch_row["已核验总行数"] or batch_row["复用分组数"] > 0
        assert batch_row["重核分组数"] == 0
        assert batch_row["总体状态"] == VERIFIED

    def test_failed_update_forces_recheck(self, cube_factory):
        svc = OlapCubeService()
        cube = cube_factory(["2026-11-01"])
        svc.build_cube(cube["id"])
        svc.verify_coverage(cube["id"])

        entry = _active_entries(cube["id"])[-1]
        entry["更新失败"] = True

        result = svc.verify_coverage(cube["id"])
        batch = result["batch_entries"][0]
        assert batch["status"] == PENDING
        rows = [
            row for row in store.rows("cache_coverage")
            if row["立方ID"] == cube["id"] and row["批次"] == entry["覆盖批次"]
        ]
        assert rows and all(row["状态"] == PENDING for row in rows)

    def test_legacy_cache_without_manifest_stays_pending(self, cube_factory):
        svc = OlapCubeService()
        cube = cube_factory(["2026-11-01"])
        svc.build_cube(cube["id"])

        legacy = {
            "id": 99901,
            "配置编号": "AGGR-LEGACY",
            "目标立方": f"olap_cube:{cube['id']}",
            "缓存大小": "1024",
            "status": "生效中",
            "pending": True,
            "abnormal": False,
            "cached_data": [],
        }
        store.rows("aggregate_cache").append(legacy)

        result = svc.verify_coverage(cube["id"])
        legacy_result = next(
            be for be in result["batch_entries"] if be["entry_id"] == 99901
        )
        assert legacy_result["status"] == PENDING
        assert legacy_result["complete"] is False

        records = svc.coverage_records(cube["id"])
        assert records["ok"]

    def test_same_version_two_batches_cannot_prove_full_version(self, cube_factory):
        svc = OlapCubeService()
        cube = cube_factory(["2026-11-01", "2026-11-02", "2026-11-03"])
        svc.build_cube(cube["id"])

        refreshed = svc.refresh_cube(cube["id"], partitions=["2026-11-02"])
        coverage = refreshed["coverage"]
        version = coverage["versions"][cube["清单版本"]]
        assert version["批次数"] == 2
        assert version["整版完整"] is False
        assert "多个刷新批次" in version["整版原因"]

        # 两个批次各自只能证明局部覆盖，不能拼成整版。
        for batch in coverage["batch_entries"]:
            assert batch["complete"] is False
            assert batch["verified_partitions"]
        assert len(version["批次"]) == 2

        plan = plan_cube_read(cube["id"])
        assert plan["scope"] == "局部读取"
        # 只能选一个批次证明，未被该批次核验的分区不能读。
        assert plan["uncovered_partitions"] == ["2026-11-02"]

    def test_full_refresh_restores_single_batch_proof(self, cube_factory):
        svc = OlapCubeService()
        cube = cube_factory(["2026-11-01", "2026-11-02"])
        svc.build_cube(cube["id"])
        svc.refresh_cube(cube["id"], partitions=["2026-11-02"])

        refreshed = svc.refresh_cube(cube["id"])
        assert refreshed["coverage"]["versions"][cube["清单版本"]]["整版完整"] is True

        plan = svc.read_cube(cube["id"])
        assert plan["scope"] == "完整读取"
        assert plan["uncovered_partitions"] == []


class TestReadPlanAndWriteback:
    def test_partial_measure_request_is_local_read(self, cube_factory):
        svc = OlapCubeService()
        cube = cube_factory(["2026-11-01"], measures="revenue,orders")
        svc.build_cube(cube["id"])

        plan = svc.read_cube(cube["id"], measures=["revenue", "missing_metric"])
        assert plan["scope"] == "局部读取"
        assert plan["measures"] == ["revenue"]
        assert plan["uncovered_measures"] == ["missing_metric"]

    def test_read_plan_writes_back_to_cube_refresh_manifest(self, cube_factory):
        svc = OlapCubeService()
        cube = cube_factory(["2026-11-01"])
        svc.build_cube(cube["id"])
        svc.verify_coverage(cube["id"])

        plan = svc.read_cube(cube["id"])
        assert plan["scope"] == "完整读取"
        assert cube["立方刷新清单"][-1]["读取范围"] == "完整读取"
        assert cube["立方刷新清单"][-1]["批次"] == plan["batch"]
        assert cube["最近覆盖核验"]["读取范围"] == "完整读取"
        assert len(plan["coverage_record_ids"]) > 0

    def test_read_plan_without_proof_reads_nothing(self, cube_factory):
        svc = OlapCubeService()
        cube = cube_factory(["2026-11-01"])
        # 未构建/未核验时没有任何可复用记录。
        plan = svc.read_cube(cube["id"])
        assert plan["scope"] == "无法证明"
        assert plan["partitions"] == []
        assert plan["uncovered_partitions"] == ["2026-11-01"]


class TestCoverageApi:
    def test_verify_and_records_api(self, cube_factory):
        svc = OlapCubeService()
        cube = cube_factory(["2026-11-01"])
        svc.build_cube(cube["id"])

        resp = client.post(f"/api/olap_cube/{cube['id']}/coverage-verify")
        assert resp.status_code == 200
        assert resp.json()["batch_entries"][0]["status"] == VERIFIED

        resp = client.get(f"/api/olap_cube/{cube['id']}/coverage-records")
        assert resp.status_code == 200
        payload = resp.json()
        assert payload["verified_groups"] > 0

    def test_read_plan_api(self, cube_factory):
        svc = OlapCubeService()
        cube = cube_factory(["2026-11-01", "2026-11-02"])
        svc.build_cube(cube["id"])
        svc.verify_coverage(cube["id"])

        resp = client.post(
            f"/api/olap_cube/{cube['id']}/read-plan",
            params={"partitions": "2026-11-01", "measures": "revenue"},
        )
        assert resp.status_code == 200
        payload = resp.json()
        assert payload["scope"] == "完整读取"
        assert payload["partitions"] == ["2026-11-01"]

    def test_partial_refresh_api_reports_version_conflict(self, cube_factory):
        svc = OlapCubeService()
        cube = cube_factory(["2026-11-01", "2026-11-02"])
        svc.build_cube(cube["id"])

        resp = client.post(
            f"/api/olap_cube/{cube['id']}/refresh",
            params={"partitions": "2026-11-02"},
        )
        assert resp.status_code == 200
        payload = resp.json()
        version = payload["coverage"]["versions"][cube["清单版本"]]
        assert version["批次数"] == 2
        assert version["整版完整"] is False

    def test_mark_update_failed_api(self, cube_factory):
        svc = OlapCubeService()
        cube = cube_factory(["2026-11-01"])
        svc.build_cube(cube["id"])
        entry = _active_entries(cube["id"])[-1]

        resp = client.post(f"/api/aggregate_cache/{entry['id']}/mark-update-failed")
        assert resp.status_code == 200
        payload = resp.json()
        assert payload["ok"]
        assert payload["entry"]["更新失败"] is True
        assert payload["coverage"]["batch_entries"][-1]["status"] == PENDING
