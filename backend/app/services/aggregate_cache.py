"""聚合缓存领域服务：缓存预热、命中统计、淘汰策略与跨模块缓存一致性管理。"""
from __future__ import annotations

import hashlib
import time
import uuid
from typing import Any

from app.services.base import BaseModuleService
from app.store import store


class AggregateCacheService(BaseModuleService):
    MODULE = "aggregate_cache"
    ENTITY = "缓存配置"
    OPERATION_TERM = "查询"
    LIST_FIELDS = ["配置编号", "目标立方", "缓存粒度", "TTL秒数", "预热策略", "命中阈值", "淘汰算法", "缓存大小"]
    REQUIRED_FIELDS = ["配置编号", "目标立方"]
    STATUS_ORDER = ["草稿", "生效中", "淘汰中", "已停用"]
    ACTION_RULES = {"启效缓存": "生效中", "强制淘汰": "淘汰中", "停用缓存": "已停用"}
    NEGATIVE_ACTIONS = ["强制淘汰", "停用缓存"]

    def check_cache_consistency(self, cube_id: int) -> dict[str, Any]:
        """检查缓存与源立方的一致性：按分区/分组/度量核验，而不是只比总行数。"""
        cube = store.find("olap_cube", cube_id)
        if cube is None:
            return {"ok": False, "message": "立方不存在"}

        from app.services.cache_coverage import verify_cube_coverage

        verification = verify_cube_coverage(cube_id)
        if not verification.get("ok"):
            return verification

        stale_count = 0
        for batch_entry in verification.get("batch_entries", []):
            if batch_entry["status"] == "冲突" or (
                batch_entry["status"] == "局部核验"
            ):
                entry = store.find(self.MODULE, batch_entry["entry_id"])
                if entry is not None and entry.get("status") == "生效中":
                    entry["status"] = "淘汰中"
                    entry["pending"] = True
                    stale_count += 1

        return {
            "ok": True,
            "cube_id": cube_id,
            "stale_entries": stale_count,
            "cube_row_count": cube.get("数据行数"),
            "batches": verification.get("batches", []),
            "versions": verification.get("versions", {}),
            "entries": verification.get("batch_entries", []),
        }

    def prewarm_cube_cache(self, cube_id: int) -> dict[str, Any]:
        """预热立方缓存：读取立方的分区聚合明细，签发同源清单与覆盖批次。"""
        cube = store.find("olap_cube", cube_id)
        if cube is None:
            return {"ok": False, "message": "立方不存在"}

        dims = cube.get("维度列表", "id").split(",")
        dims = [d.strip() for d in dims if d.strip()]
        meas = cube.get("度量列表", "value").split(",")
        meas = [m.strip() for m in meas if m.strip()]

        detail = cube.get("聚合明细")
        if not isinstance(detail, list) or not detail:
            return {
                "ok": False,
                "message": "立方缺少分区聚合明细，无法签发可核验的缓存清单",
                "cache_entry_id": None,
            }

        batch = f"PW-{cube_id:04d}-{uuid.uuid4().hex[:6]}"
        version = str(cube.get("清单版本") or "v1")
        manifest = [
            {
                "分区": str(row.get("分区", "默认")),
                "分区标识": str(row.get("分区", "默认")),
                "分组": row.get("分组"),
                "度量": list(row.get("度量") or meas),
                "行数": int(row.get("行数", 0)),
                "批次": batch,
                "清单版本": version,
            }
            for row in detail
        ]
        data = cube.get("聚合明细", [])

        rows = store.rows(self.MODULE)
        cache_id = max((int(r.get("id", 0)) for r in rows), default=0) + 1
        data_str = str(data)
        cache = {
            "id": cache_id,
            "配置编号": f"AGGR-PW{cube_id:04d}",
            "目标立方": f"olap_cube:{cube_id}",
            "缓存粒度": f"{len(dims)}维 x {len(meas)}度",
            "TTL秒数": "7200",
            "预热策略": "EAGER",
            "命中阈值": "0.85",
            "淘汰算法": "LRU",
            "缓存大小": str(len(data_str)),
            "覆盖批次": batch,
            "清单版本": version,
            "覆盖分区": sorted({item["分区"] for item in manifest}),
            "缓存清单": manifest,
            "缓存行数": str(sum(item["行数"] for item in manifest)),
            "status": "生效中",
            "pending": True,
            "abnormal": False,
            "cached_data": data,
        }
        rows.append(cache)

        # 也写入 result_cache
        result_rows = store.rows("result_cache")
        result_id = max((int(r.get("id", 0)) for r in result_rows), default=0) + 1
        result = {
            "id": result_id,
            "条目编号": f"RESU-PW{cube_id:04d}",
            "查询摘要": f"prewarm_cube_{cube_id}",
            "结果哈希": hashlib.md5(data_str.encode()).hexdigest()[:12],
            "缓存命中": "0",
            "过期时间": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time() + 7200)),
            "存储大小": str(len(data_str)),
            "创建时间": time.strftime("%Y-%m-%d %H:%M:%S"),
            "最后命中": time.strftime("%Y-%m-%d %H:%M:%S"),
            "status": "有效",
            "pending": True,
            "abnormal": False,
            "result_data": data,
        }
        result_rows.append(result)

        from app.services.cache_coverage import verify_cube_coverage

        return {
            "ok": True,
            "cube_id": cube_id,
            "prewarmed_rows": len(manifest),
            "batch": batch,
            "cache_entry_id": cache_id,
            "result_cache_id": result_id,
            "coverage": verify_cube_coverage(cube_id),
        }

    def evict_stale_cache(self, cube_id: int, partitions: list[str] | None = None) -> dict[str, Any]:
        """淘汰指定立方的缓存；``partitions`` 非空时只淘汰覆盖这些分区的清单。"""
        evicted = []
        wanted = {str(p) for p in partitions} if partitions else None

        # 淘汰 aggregate_cache
        agg_rows = store.rows(self.MODULE)
        for entry in agg_rows:
            if str(cube_id) not in str(entry.get("目标立方", "")):
                continue
            if wanted is not None and not (set(map(str, entry.get("覆盖分区", []))) & wanted):
                continue
            if entry.get("status") in ("生效中", "草稿"):
                entry["status"] = "淘汰中"
                entry["abnormal"] = True
                evicted.append({
                    "type": "aggregate_cache",
                    "id": entry["id"],
                    "配置编号": entry.get("配置编号"),
                })

        # 淘汰 result_cache（结果缓存是整版结果，只在整版淘汰时联动）
        result_rows = store.rows("result_cache")
        for entry in result_rows:
            if wanted is not None:
                break
            if f"cube_{cube_id}" in str(entry.get("查询摘要", "")) and entry.get("status") == "有效":
                entry["status"] = "已过期"
                evicted.append({
                    "type": "result_cache",
                    "id": entry["id"],
                    "条目编号": entry.get("条目编号"),
                })

        # 检查数据新鲜度并通知
        _check_freshness_after_eviction(cube_id, len(evicted))

        # 写入审计
        _audit_cache_eviction(cube_id, len(evicted))

        return {
            "ok": True,
            "cube_id": cube_id,
            "evicted_count": len(evicted),
            "entries": evicted[:10],
        }


def _check_freshness_after_eviction(cube_id: int, evicted_count: int) -> None:
    fresh_rows = store.rows("freshness_mon")
    for fr in fresh_rows:
        cube_ref = fr.get("目标数据集", "")
        if str(cube_id) in str(cube_ref) or f"cube_{cube_id}" in str(cube_ref):
            if evicted_count > 0:
                fr["status"] = "已告警"
                fr["触发次数"] = str(int(fr.get("触发次数", "0")) + 1)
                fr["最后检查"] = time.strftime("%Y-%m-%d %H:%M:%S")


def _audit_cache_eviction(cube_id: int, count: int) -> None:
    rows = store.rows("query_audit")
    audit_id = max((int(r.get("id", 0)) for r in rows), default=0) + 1
    audit = {
        "id": audit_id,
        "记录编号": f"AUDIT-{uuid.uuid4().hex[:8]}",
        "查询语句": f"CACHE_EVICT cube_id={cube_id}",
        "执行耗时": "100ms",
        "扫描行数": str(count),
        "返回行数": str(count),
        "数据来源": "aggregate_cache",
        "操作用户": "cache_evictor",
        "查询时间": time.strftime("%Y-%m-%d %H:%M:%S"),
        "status": "正常",
        "pending": False,
        "abnormal": False,
    }
    rows.append(audit)