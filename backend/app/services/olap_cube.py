"""多维立方领域服务：立方构建、聚合计算、缓存预热与分区策略。"""
from __future__ import annotations

import hashlib
import random
import time
import uuid
from typing import Any

from app.services.base import BaseModuleService
from app.store import store


class OlapCubeService(BaseModuleService):
    MODULE = "olap_cube"
    ENTITY = "OLAP立方"
    OPERATION_TERM = "聚合"
    LIST_FIELDS = ["立方编号", "立方名称", "维度列表", "度量列表", "聚合类型", "分区策略", "刷新周期", "数据行数"]
    REQUIRED_FIELDS = ["立方编号", "立方名称", "维度列表"]
    STATUS_ORDER = ["草稿", "构建中", "可用", "构建失败"]
    ACTION_RULES = {"启动构建": "构建中", "标记可用": "可用", "标记失败": "构建失败"}
    NEGATIVE_ACTIONS = []

    def build_cube(
        self, cube_id: int, data_sources: list[str] | None = None,
        partitions: list[str] | None = None, verify: bool = True,
    ) -> dict[str, Any]:
        """构建多维立方、聚合计算并写入 aggregate_cache、result_cache、query_audit。

        ``partitions`` 只重建指定分区，其余分区的聚合明细原样保留；不传时整版重建。
        """
        cube = store.find(self.MODULE, cube_id)
        if cube is None:
            return {"ok": False, "message": "立方不存在"}

        dims = cube.get("维度列表", "").split(",")
        dims = [d.strip() for d in dims if d.strip()]
        meas = cube.get("度量列表", "").split(",")
        meas = [m.strip() for m in meas if m.strip()]

        if not dims or not meas:
            return {"ok": False, "message": "维度和度量定义不能为空"}

        # 从语义模型获取字段映射
        models = _get_models_for_cube(dims)
        field_map = {}
        for m in models:
            aliases = m.get("字段别名", "").split(",")
            for a in aliases:
                a = a.strip()
                if a:
                    field_map[a] = a

        partition_field = _partition_field(cube)
        existing_detail = _load_detail(cube)
        rebuild_all = partitions is None
        target_partitions = (
            _partition_values(cube) if rebuild_all
            else [str(p) for p in partitions if str(p)]
        )

        # 从数据源获取原始数据
        sources = data_sources or _get_registered_sources()
        raw_data = _fetch_source_data(
            sources, dims, partition_field, target_partitions
        )

        # 执行聚合计算
        t0 = time.time()
        aggregated = _compute_aggregates(
            raw_data, dims, meas, cube.get("聚合类型", "SUM"), partition_field
        )
        elapsed_ms = (time.time() - t0) * 1000

        # 回写聚合明细：局部刷新只替换目标分区，其余分区保留。
        if rebuild_all:
            detail = _detail_from_aggregates(aggregated, dims, meas, partition_field)
        else:
            rebuilt = _detail_from_aggregates(aggregated, dims, meas, partition_field)
            keep = {str(p) for p in target_partitions}
            detail = [row for row in existing_detail if str(row.get("分区")) not in keep]
            detail.extend(rebuilt)
        cube["聚合明细"] = detail

        # 写入多维立方
        cube["数据行数"] = str(sum(int(row.get("行数", 0)) for row in detail))
        cube["status"] = "可用"
        cube["pending"] = False

        # 每次刷新生成独立覆盖批次，清单与批次同源。
        batch = _new_batch(cube_id)
        version = str(cube.get("清单版本") or "")
        if not version:
            version = "v1"
        elif rebuild_all:
            # 整版重建开启新版本；局部刷新沿用原版本，交由核验判定整版是否成立。
            version = _bump_version(version)
        cube["清单版本"] = version
        cube["最近刷新批次"] = batch

        # 写入聚合缓存（只覆盖本次重建的分区）
        _write_aggregate_cache(cube_id, dims, meas, aggregated, cube, batch, version,
                               target_partitions if not rebuild_all else None)

        # 写入结果缓存
        _write_result_cache(cube_id, aggregated)

        # 写入审计
        _write_cube_audit(cube_id, elapsed_ms, len(aggregated))

        # 更新相关指标工厂
        _update_metrics(cube_id, meas, aggregated)

        # 构建完成即按本批次核验覆盖，生成可复用记录。
        coverage = None
        if verify:
            from app.services.cache_coverage import verify_cube_coverage
            coverage = verify_cube_coverage(cube_id)

        return {
            "ok": True,
            "cube_id": cube_id,
            "dims": len(dims),
            "measures": len(meas),
            "aggregated_rows": len(aggregated),
            "partitions": target_partitions,
            "batch": batch,
            "elapsed_ms": elapsed_ms,
            "sample": aggregated[:5],
            **({"coverage": coverage} if coverage is not None else {}),
        }

    def refresh_cube(
        self, cube_id: int, partitions: list[str] | None = None
    ) -> dict[str, Any]:
        """刷新立方：按分区淘汰旧 cache、重建、刷新依赖集市。

        只刷新部分分区时保留其余分区缓存，交回覆盖核验判断整版能否成立。
        """
        cube = store.find(self.MODULE, cube_id)
        if cube is None:
            return {"ok": False, "message": "立方不存在"}

        # 淘汰旧缓存（局部刷新只淘汰目标分区）
        _evict_cube_cache(cube_id, partitions)

        # 重新构建（统一在刷新末尾核验，避免同一批次重复核验）
        result = self.build_cube(cube_id, partitions=partitions, verify=False)

        # 更新数据集市（如果存在依赖）
        _refresh_dependent_marts(cube_id)

        # 刷新后立刻按同一批次核验覆盖
        from app.services.cache_coverage import (
            freeze_superseded_versions, verify_cube_coverage,
        )
        if result.get("ok"):
            if partitions is None:
                freeze_superseded_versions(cube_id, str(cube.get("清单版本") or ""))
            result["coverage"] = verify_cube_coverage(cube_id)

        return result

    def read_cube(
        self, cube_id: int,
        partitions: list[str] | None = None,
        measures: list[str] | None = None,
    ) -> dict[str, Any]:
        """按覆盖核验记录裁定完整/局部读取范围，并回写立方刷新清单。"""
        from app.services.cache_coverage import plan_cube_read
        return plan_cube_read(cube_id, partitions=partitions, measures=measures)

    def verify_coverage(self, cube_id: int) -> dict[str, Any]:
        """核验立方缓存的分区与度量覆盖，生成可复用的核验记录。"""
        from app.services.cache_coverage import verify_cube_coverage
        return verify_cube_coverage(cube_id)

    def coverage_records(self, cube_id: int) -> dict[str, Any]:
        """读取已生成的覆盖核验记录。"""
        from app.services.cache_coverage import coverage_records
        return coverage_records(cube_id)


    def estimate_partition_cost(
        self, cube_id: int, partition_count: int, partition_rows: int
    ) -> dict[str, Any]:
        """估算分区构建代价，写入 cost_govern。"""
        cube = store.find(self.MODULE, cube_id)
        if cube is None:
            return {"ok": False, "message": "立方不存在"}

        total_rows = partition_count * partition_rows
        estimated_memory = total_rows * 0.5  # MB
        estimated_time = total_rows / 10000  # seconds

        # 写入成本治理
        cost_rows = store.rows("cost_govern")
        cost_id = max((int(r.get("id", 0)) for r in cost_rows), default=0) + 1
        cost = {
            "id": cost_id,
            "策略编号": f"COST-C{cube_id:04d}",
            "适用角色": "olap_cube_builder",
            "查询上限": str(total_rows),
            "并发上限": str(partition_count),
            "月度配额": str(total_rows * 8),
            "告警阈值": str(int(estimated_memory * 1.5)),
            "限流动作": "monitor",
            "生效时段": time.strftime("%Y-%m-%d"),
            "status": "已生效" if estimated_memory < 1000 else "已告警",
            "pending": estimated_memory >= 1000,
            "abnormal": estimated_memory >= 2000,
        }
        cost_rows.append(cost)

        return {
            "ok": True,
            "estimated_rows": total_rows,
            "estimated_memory_mb": estimated_memory,
            "estimated_time_s": estimated_time,
            "cost_entry_id": cost_id,
        }


# ---------- 跨模块辅助函数 ----------

def _get_models_for_cube(dims: list[str]) -> list[dict[str, Any]]:
    models = store.rows("semantic_model")
    return [m for m in models if m.get("status") == "已投产"]


def _get_registered_sources() -> list[str]:
    sources = store.rows("source_registry")
    return [s.get("源编号", f"src_{i}") for i, s in enumerate(sources) if s.get("status") == "已注册"]


def _partition_field(cube: dict[str, Any]) -> str:
    field = str(cube.get("分区字段") or cube.get("分区策略") or "dt").strip()
    return field or "dt"


def _partition_values(cube: dict[str, Any]) -> list[str]:
    listed = cube.get("分区列表")
    if isinstance(listed, (list, tuple)) and listed:
        return [str(item) for item in listed]
    detail = cube.get("聚合明细")
    if isinstance(detail, (list, tuple)) and detail:
        values = [str(row.get("分区")) for row in detail if isinstance(row, dict) and row.get("分区")]
        if values:
            return sorted(set(values))
    return [str(cube.get("默认分区") or "2026-11-21")]


def _load_detail(cube: dict[str, Any]) -> list[dict[str, Any]]:
    detail = cube.get("聚合明细")
    if isinstance(detail, (list, tuple)):
        return [dict(row) for row in detail if isinstance(row, dict)]
    return []


def _new_batch(cube_id: int) -> str:
    return f"CB-{cube_id:04d}-{uuid.uuid4().hex[:6]}"


def _bump_version(version: str) -> str:
    digits = "".join(ch for ch in version if ch.isdigit())
    return f"v{int(digits) + 1}" if digits else f"{version}.1"


def _fetch_source_data(
    sources: list[str],
    dims: list[str],
    partition_field: str = "",
    partitions: list[str] | None = None,
) -> list[dict[str, Any]]:
    """模拟从数据源读取原始数据。"""
    data = []
    partitions = partitions or [""]
    for i in range(random.randint(20, 200)):
        row = {"_source": sources[i % len(sources)] if sources else "local"}
        for dim in dims:
            row[dim] = f"{dim}_val_{random.randint(1, 20)}"
        row["_value"] = random.randint(1, 10000)
        if partition_field:
            row[partition_field] = partitions[i % len(partitions)]
        data.append(row)
    return data


def _compute_aggregates(
    raw: list[dict[str, Any]],
    dims: list[str],
    measures: list[str],
    agg_type: str,
    partition_field: str = "",
) -> list[dict[str, Any]]:
    """按（分区, 维度组合）执行聚合计算。"""
    groups: dict[tuple, list[int]] = {}
    for row in raw:
        key = ((row.get(partition_field, "") if partition_field else ""),) + tuple(
            row.get(d, "?") for d in dims
        )
        groups.setdefault(key, []).append(row.get("_value", 0))

    result = []
    for key, values in groups.items():
        agg_row = {}
        if partition_field:
            agg_row[partition_field] = key[0]
            dims_offset = 1
        else:
            dims_offset = 0
        for i, d in enumerate(dims):
            agg_row[d] = key[i + dims_offset]
        if agg_type == "SUM":
            agg_row["aggregated_value"] = sum(values)
        elif agg_type == "AVG":
            agg_row["aggregated_value"] = sum(values) / len(values)
        elif agg_type == "COUNT":
            agg_row["aggregated_value"] = len(values)
        elif agg_type == "MAX":
            agg_row["aggregated_value"] = max(values)
        elif agg_type == "MIN":
            agg_row["aggregated_value"] = min(values)
        else:
            agg_row["aggregated_value"] = sum(values)
        agg_row["group_count"] = len(values)
        result.append(agg_row)
    return result


def _detail_from_aggregates(
    aggregated: list[dict[str, Any]],
    dims: list[str],
    measures: list[str],
    partition_field: str,
) -> list[dict[str, Any]]:
    """把聚合结果转为（分区, 分组, 度量）明细，作为覆盖核验的唯一事实源。"""
    detail: list[dict[str, Any]] = []
    for row in aggregated:
        detail.append({
            "分区": str(row.get(partition_field, "默认")) if partition_field else "默认",
            "分组": {dim: row.get(dim) for dim in dims},
            "度量": list(measures),
            "行数": int(row.get("group_count", 1)),
        })
    return detail


def _manifest_from_detail(
    detail: list[dict[str, Any]],
    measures: list[str],
    batch: str,
    version: str,
    partitions: list[str] | None = None,
) -> list[dict[str, Any]]:
    """按立方聚合明细生成覆盖清单，清单与批次严格同源。"""
    wanted = {str(item) for item in partitions} if partitions is not None else None
    manifest: list[dict[str, Any]] = []
    for item in detail:
        partition = str(item.get("分区", "默认"))
        if wanted is not None and partition not in wanted:
            continue
        manifest.append({
            "分区": partition,
            "分区标识": partition,
            "分组": item.get("分组"),
            "度量": list(item.get("度量") or measures),
            "行数": int(item.get("行数", 0)),
            "批次": batch,
            "清单版本": version,
        })
    return manifest


def _write_aggregate_cache(
    cube_id: int,
    dims: list[str],
    measures: list[str],
    data: list[dict[str, Any]],
    cube: dict[str, Any] | None = None,
    batch: str = "",
    version: str = "",
    partitions: list[str] | None = None,
) -> None:
    rows = store.rows("aggregate_cache")
    cache_id = max((int(r.get("id", 0)) for r in rows), default=0) + 1
    data_str = str(data)

    # 局部刷新时把旧清单中被替换的分区摘掉，其余分区仍由旧批次证明；
    # 整版刷新则让旧清单整体退出生效。不同批次证明的覆盖不会被拼成整版。
    if cube is not None:
        for existing in rows:
            try:
                existing_cube = int(str(existing.get("目标立方", "")).rsplit(":", 1)[-1])
            except ValueError:
                continue
            if existing_cube != cube_id:
                continue
            if existing.get("被整版替代") or existing.get("被替代分区"):
                continue
            if existing.get("status") in ("已停用",):
                continue
            # 没有覆盖清单的历史条目无法证明覆盖，保留给核验标记“待核验”，
            # 不当作可被整版替代的证明材料。
            if partitions is None and not isinstance(existing.get("缓存清单"), list):
                continue
            if partitions is None:
                existing["status"] = "淘汰中"
                existing["被整版替代"] = True
                continue
            replaced = {str(p) for p in partitions}
            old_manifest = existing.get("缓存清单")
            if not isinstance(old_manifest, list):
                # 历史清单没法切分区，不能继续冒充新批次的分区证明。
                existing["status"] = "淘汰中"
                existing["被整版替代"] = True
                continue
            kept = [
                item for item in old_manifest
                if str(item.get("分区")) not in replaced
            ]
            existing["被替代分区"] = sorted(
                set(existing.get("被替代分区", [])) | replaced
            )
            existing["缓存清单"] = kept
            existing["覆盖分区"] = sorted({str(item.get("分区")) for item in kept})
            existing["缓存行数"] = str(sum(int(item.get("行数", 0)) for item in kept))
            if not kept:
                existing["status"] = "淘汰中"

    detail = _load_detail(cube) if cube is not None else []
    manifest = _manifest_from_detail(
        detail, measures, batch, version, partitions
    ) if cube is not None and batch else None
    cache = {
        "id": cache_id,
        "配置编号": f"AGGR-C{cube_id:04d}",
        "目标立方": f"olap_cube:{cube_id}",
        "缓存粒度": f"{len(dims)}维 x {len(measures)}度",
        "TTL秒数": "3600",
        "预热策略": "LAZY",
        "命中阈值": "0.8",
        "淘汰算法": "LRU",
        "缓存大小": str(len(data_str)),
        "覆盖批次": batch,
        "清单版本": version,
        "覆盖分区": sorted({str(item["分区"]) for item in manifest or []}),
        "缓存清单": manifest or [],
        "缓存大小口径": "覆盖清单行数",
        "status": "生效中",
        "pending": True,
        "abnormal": False,
        "cached_data": data,
    }
    if manifest is not None:
        cache["缓存行数"] = str(sum(int(item["行数"]) for item in manifest))
    rows.append(cache)


def _write_result_cache(cube_id: int, data: list[dict[str, Any]]) -> None:
    rows = store.rows("result_cache")
    cache_id = max((int(r.get("id", 0)) for r in rows), default=0) + 1
    data_str = str(data)
    cache = {
        "id": cache_id,
        "条目编号": f"RESU-C{cube_id:04d}",
        "查询摘要": f"cube_build_{cube_id}",
        "结果哈希": hashlib.md5(data_str.encode()).hexdigest()[:12],
        "缓存命中": str(len(data)),
        "过期时间": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time() + 7200)),
        "存储大小": str(len(data_str)),
        "创建时间": time.strftime("%Y-%m-%d %H:%M:%S"),
        "最后命中": time.strftime("%Y-%m-%d %H:%M:%S"),
        "status": "有效",
        "pending": True,
        "abnormal": False,
    }
    rows.append(cache)


def _write_cube_audit(cube_id: int, elapsed_ms: float, row_count: int) -> None:
    rows = store.rows("query_audit")
    audit_id = max((int(r.get("id", 0)) for r in rows), default=0) + 1
    status = "正常" if elapsed_ms < 10000 else "慢查询"
    audit = {
        "id": audit_id,
        "记录编号": f"AUDIT-{uuid.uuid4().hex[:8]}",
        "查询语句": f"CUBE_BUILD cube_id={cube_id}",
        "执行耗时": f"{elapsed_ms:.0f}ms",
        "扫描行数": str(row_count),
        "返回行数": str(row_count),
        "数据来源": "olap_cube",
        "操作用户": "cube_builder",
        "查询时间": time.strftime("%Y-%m-%d %H:%M:%S"),
        "status": status,
        "pending": False,
        "abnormal": elapsed_ms > 30000,
    }
    rows.append(audit)


def _update_metrics(cube_id: int, measures: list[str], data: list[dict[str, Any]]) -> None:
    """更新指标工厂的计算指标。"""
    metrics = store.rows("metric_factory")
    for m in metrics:
        if m.get("依赖立方", "").find(str(cube_id)) >= 0:
            m["最后产出"] = time.strftime("%Y-%m-%d %H:%M:%S")
            m["status"] = "已产出"


def _evict_cube_cache(cube_id: int, partitions: list[str] | None = None) -> None:
    """淘汰与立方相关的缓存条目；``partitions`` 限定只淘汰目标分区。

    局部刷新时旧清单的分区切分由 ``_write_aggregate_cache`` 完成，这里不整体淘汰。
    """
    if partitions is not None:
        return

    # 淘汰 result_cache（结果缓存是整版结果，只随整版刷新淘汰）；
    # aggregate_cache 的旧清单由 ``_write_aggregate_cache`` 标记为被整版替代。
    result = store.rows("result_cache")
    for r in result:
        if str(cube_id) in str(r.get("查询摘要", "")):
            r["status"] = "已淘汰"


def _refresh_dependent_marts(cube_id: int) -> None:
    """刷新依赖此立方的结果集市。"""
    marts = store.rows("result_mart")
    for mart in marts:
        if str(cube_id) in str(mart.get("数据来源", "")):
            mart["status"] = "刷新中"
            mart["pending"] = True