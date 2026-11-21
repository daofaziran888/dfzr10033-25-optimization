"""聚合缓存覆盖核验引擎。

核验链路不再只比较“立方总行数 / 缓存总行数”，而是把缓存清单拆到
``(分区, 分组, 度量)`` 明细，与立方聚合明细逐组比对：

* 缓存清单与覆盖批次必须来自同一次刷新；
* 同一批次内重复的分组只计一次；
* 无变化分组复用已有核验记录，更新失败的分组强制重核；
* 历史缓存没有分区/批次元数据时保持“待核验”，不误判为可用；
* 同一版本出现多个刷新批次时，只能按单个批次证明局部覆盖，整版完整
  覆盖一定不成立。

核验结果写入两张内部表，供多维立方读取时复用：

``cache_coverage``        分组级核验记录
``cache_coverage_batch``  批次级汇总（可用分区、可用度量、总计核对）
"""
from __future__ import annotations

import hashlib
import json
import time
from collections import defaultdict
from typing import Any, Iterable

from app.store import store

VERIFIED = "已核验"
PARTIAL = "局部核验"
CONFLICT = "冲突"
PENDING = "待核验"

GROUP_TABLE = "cache_coverage"
BATCH_TABLE = "cache_coverage_batch"

_PARTITION_KEYS = ("分区", "分区键", "分区值", "dt", "date", "partition", "partition_key")
_PARTITION_ID_KEYS = ("分区标识", "分区序号", "分区编号", "partition_id", "partition_no", "shard")
_GROUP_KEYS = ("分组", "分组键", "分组值", "group", "group_key")
_DIM_KEYS = ("维度", "维度值", "dims", "dimensions", "dimension_values")
_MEASURE_KEYS = ("度量", "度量值", "度量明细", "measures", "measure_values", "指标")
_ROW_KEYS = ("行数", "记录数", "rows", "row_count", "聚合行数")
_BATCH_KEYS = ("批次", "覆盖批次", "batch", "batch_id", "coverage_batch")
_FAILED_KEYS = ("更新失败", "刷新失败", "update_failed", "failed")
_VERSION_KEYS = ("清单版本", "缓存版本", "版本", "version", "metadata_version")
_PARTITION_LIST_KEYS = ("分区列表", "分区清单", "partitions")
_DETAIL_KEYS = ("聚合明细", "分区聚合", "聚合数据", "分区明细", "cube_aggregates", "aggregates")
_MANIFEST_KEYS = ("缓存清单", "缓存分区清单", "覆盖清单", "manifest", "coverage_manifest")
_CHILD_KEYS = ("分组明细", "分组列表", "groups", "group_details")

_RESERVED_ROW_KEYS = {
    "id", "status", "pending", "abnormal", "_source", "_value", "aggregated_value",
    "group_count", "创建时间", "最后命中", "过期时间", "存储大小", "缓存命中",
    *_PARTITION_KEYS, *_PARTITION_ID_KEYS, *_GROUP_KEYS, *_DIM_KEYS,
    *_MEASURE_KEYS, *_ROW_KEYS, *_BATCH_KEYS, *_FAILED_KEYS,
    *_VERSION_KEYS, *_DETAIL_KEYS, *_MANIFEST_KEYS, *_CHILD_KEYS,
}


# ---------------------------------------------------------------------------
# 规范化辅助
# ---------------------------------------------------------------------------

def _get(row: dict[str, Any], keys: Iterable[str], default: Any = None) -> Any:
    for key in keys:
        if key in row and row[key] is not None and row[key] != "":
            return row[key]
    return default


def _canonical(value: Any) -> str:
    if isinstance(value, (dict, list, tuple, set)):
        return json.dumps(value, sort_keys=True, ensure_ascii=False,
                          separators=(",", ":"), default=str)
    return str(value)


def _signature(*parts: Any) -> str:
    return hashlib.md5(_canonical(list(parts)).encode("utf-8")).hexdigest()[:16]


def _int_or_none(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _load_list(value: Any) -> list[Any] | None:
    if isinstance(value, (list, tuple)):
        return list(value)
    if isinstance(value, str) and value.strip().startswith("["):
        try:
            loaded = json.loads(value)
        except (ValueError, TypeError):
            return None
        if isinstance(loaded, list):
            return loaded
    return None


def measure_set(value: Any) -> frozenset[str]:
    """把度量字段统一为名称集合，支持字符串、列表和 {度量: 值} 字典。"""
    if value is None:
        return frozenset()
    if isinstance(value, dict):
        return frozenset(str(key) for key in value)
    if isinstance(value, (list, tuple, set)):
        names: set[str] = set()
        for item in value:
            if isinstance(item, dict):
                nested = item.get("度量")
                if isinstance(nested, dict):
                    names.update(str(key) for key in nested)
                else:
                    names.update(str(key) for key in item)
            else:
                names.add(str(item))
        return frozenset(names)
    names = set()
    for chunk in str(value).replace("|", ",").split(","):
        chunk = chunk.strip()
        if not chunk:
            continue
        if "=" in chunk:
            chunk = chunk.split("=", 1)[0].strip()
        names.add(chunk)
    return frozenset(names)


def group_key(item: dict[str, Any]) -> str:
    explicit = _get(item, _GROUP_KEYS)
    if explicit not in (None, ""):
        return _canonical(explicit)
    dims = _get(item, _DIM_KEYS)
    if dims not in (None, ""):
        return _canonical(dims)
    leftovers = {
        key: value for key, value in item.items()
        if key not in _RESERVED_ROW_KEYS and not str(key).startswith("_")
    }
    return _canonical(leftovers) if leftovers else ""


def normalize_manifest(
    manifest: Any,
    *,
    default_batch: str = "",
    version: str = "",
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """把缓存清单展开为分组级记录，并在展开阶段做批次/重复检查。"""
    groups: list[dict[str, Any]] = []
    issues: dict[str, Any] = {
        "batch_mismatch": False,
        "missing_batch": not bool(default_batch),
        "duplicates": 0,
        "duplicate_conflicts": [],
        "malformed": 0,
    }

    def walk(items: Any, inherited_partition: str = "", inherited_id: str = "") -> None:
        if not isinstance(items, (list, tuple)):
            issues["malformed"] += 1
            return
        for item in items:
            if not isinstance(item, dict):
                issues["malformed"] += 1
                continue
            item_batch = str(_get(item, _BATCH_KEYS, default_batch) or "")
            if default_batch and item_batch and item_batch != default_batch:
                issues["batch_mismatch"] = True
            partition = str(_get(item, _PARTITION_KEYS, inherited_partition) or inherited_partition)
            partition_id = str(
                _get(item, _PARTITION_ID_KEYS, inherited_id) or inherited_id or partition
            )
            children = _load_list(_get(item, _CHILD_KEYS))
            if children is not None:
                walk(children, partition, partition_id)
                continue
            group = group_key(item)
            measures = measure_set(_get(item, _MEASURE_KEYS))
            rows = _int_or_none(_get(item, _ROW_KEYS))
            failed = bool(_get(item, _FAILED_KEYS, False))
            groups.append({
                "partition": partition,
                "partition_id": partition_id,
                "group": group,
                "measures": measures,
                "rows": rows,
                "failed": failed,
                "batch": item_batch or default_batch,
                "version": version,
                "signature": _signature(version, partition, group, sorted(measures), rows),
            })

    walk(_load_list(manifest))
    return groups, issues


def expected_groups(cube: dict[str, Any]) -> list[dict[str, Any]]:
    """从立方聚合明细得到期望覆盖的分组；没有明细时返回空列表。"""
    detail = _load_list(_get(cube, _DETAIL_KEYS))
    if not detail:
        return []
    groups, _ = normalize_manifest(
        detail,
        default_batch="",
        version=str(_get(cube, _VERSION_KEYS, "") or ""),
    )
    return groups


def _manifest_total(groups: list[dict[str, Any]]) -> int | None:
    known = [group["rows"] for group in groups if group["rows"] is not None]
    if len(known) != len(groups):
        return None
    return sum(known)


def _index(groups: list[dict[str, Any]]) -> tuple[dict[tuple[str, str], dict[str, Any]], dict[str, Any]]:
    index: dict[tuple[str, str], dict[str, Any]] = {}
    issues = {"duplicates": 0, "conflicts": []}
    for group in groups:
        key = (group["partition"], group["group"])
        if key in index:
            if index[key]["signature"] == group["signature"]:
                issues["duplicates"] += 1
            else:
                issues["conflicts"].append(list(key))
            continue
        index[key] = group
    return index, issues


# ---------------------------------------------------------------------------
# 单条缓存清单的核验
# ---------------------------------------------------------------------------

def verify_entry(
    cube: dict[str, Any],
    entry: dict[str, Any],
    prior: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    """核验一条缓存清单，返回可落库的分组记录和批次汇总。"""
    version = str(_get(entry, _VERSION_KEYS, _get(cube, _VERSION_KEYS, "")) or "")
    batch = str(_get(entry, _BATCH_KEYS, "") or "")
    raw_manifest = _load_list(_get(entry, _MANIFEST_KEYS))

    expected = expected_groups(cube)
    base = {
        "cube_id": int(cube.get("id", 0)),
        "entry_id": int(entry.get("id", 0)),
        "batch": batch,
        "version": version,
        "groups": [],
        "expected_group_count": len(expected),
        "verified_partitions": [],
        "partial_partitions": [],
        "missing_partitions": [],
        "extra_partitions": [],
        "covered_measures": [],
        "missing_measures": [],
        "cube_total": None,
        "manifest_total": None,
        "verified_total": None,
        "total_match": None,
        "reason": "",
        "reused_groups": 0,
        "rechecked_groups": 0,
        "pending_groups": 0,
        "conflict_groups": 0,
        "duplicate_groups": 0,
    }

    def unprovable(reason: str) -> dict[str, Any]:
        base.update({
            "status": PENDING,
            "complete": False,
            "reason": reason,
            "group_records": [],
            "duplicate_dates": [],
            "partition_status": {},
        })
        return base

    if raw_manifest is None or len(raw_manifest) == 0:
        return unprovable("历史缓存缺少覆盖清单元数据，无法证明分区与度量覆盖")
    if not batch:
        return unprovable("缓存清单缺少覆盖批次，无法证明覆盖")
    if not expected:
        return unprovable("立方缺少分区聚合明细，无法证明缓存覆盖")

    manifest_groups, issues = normalize_manifest(
        raw_manifest, default_batch=batch, version=version
    )
    base["duplicate_groups"] = issues["duplicates"] + len(issues["duplicate_conflicts"])
    if issues["batch_mismatch"]:
        return unprovable("缓存清单与覆盖批次不一致，无法证明覆盖")
    if not manifest_groups:
        return unprovable("缓存清单没有可核验的分组")

    manifest_index, dup_issues = _index(manifest_groups)
    base["duplicate_groups"] += dup_issues["duplicates"] + len(dup_issues["conflicts"])
    if dup_issues["conflicts"]:
        base["reason"] = "同一批次内重复分组内容不一致"

    # 重复分组只计一次：总计按去重后的清单计算。
    unique_manifest = list(manifest_index.values())
    entry_failed = bool(_get(entry, _FAILED_KEYS, False))

    expected_index, _ = _index(expected)

    # 同一批清单里把同一个物理分区日期指向多份（例如两个分区都指到同一天），
    # 行数可能刚好相抵；这是覆盖证明的结构性冲突。
    partition_ids: dict[str, set[str]] = defaultdict(set)
    for group in manifest_groups:
        partition_ids[group["partition"]].add(group["partition_id"])
    duplicate_dates = sorted(
        partition for partition, ids in partition_ids.items() if len(ids) > 1
    )
    # 分区标识相同的另一面：一个分区标识被写进了多个分区日期，同样不可证明。
    ids_to_partitions: dict[str, set[str]] = defaultdict(set)
    for group in manifest_groups:
        ids_to_partitions[group["partition_id"]].add(group["partition"])
    for partition_id, partition_names in ids_to_partitions.items():
        if len(partition_names) > 1:
            duplicate_dates.extend(sorted(partition_names))
    duplicate_dates = sorted(set(duplicate_dates))

    # 旧批次里被本轮局部刷新替换掉、但新清单没有补齐的分区，同样算缺失。
    replaced = {
        str(item) for item in (entry.get("被替代分区") or [])
        if str(item)
    }
    expected_partitions = {group["partition"] for group in expected}
    manifest_partitions = {partition for partition, _ in manifest_index}
    base["missing_partitions"] = sorted(
        (expected_partitions - manifest_partitions) | (replaced - manifest_partitions)
    )
    base["extra_partitions"] = sorted(manifest_partitions - expected_partitions)

    group_records: list[dict[str, Any]] = []
    verified_measure_map: dict[str, set[str]] = defaultdict(set)
    partition_status: dict[str, str] = {}
    verified_total = 0
    verified_total_known = True

    for key, wanted in expected_index.items():
        partition, group = key
        found = manifest_index.get(key)
        if found is None:
            group_records.append({
                "分区": partition,
                "分区标识": "",
                "分组": group,
                "度量": sorted(wanted["measures"]),
                "行数": None,
                "立方行数": wanted["rows"],
                "状态": PENDING,
                "复用": False,
                "更新失败": False,
                "签名": wanted["signature"],
                "原因": "缓存清单缺少该分组",
            })
            continue

        measures = sorted(found["measures"])
        reason = ""
        status = VERIFIED
        reused = False
        failed = bool(found["failed"]) or entry_failed

        missing_measures = wanted["measures"] - found["measures"]
        if missing_measures:
            status = CONFLICT
            reason = f"度量覆盖不足：{','.join(sorted(missing_measures))}"
        elif (wanted["rows"] is not None and found["rows"] is not None
              and wanted["rows"] != found["rows"]):
            status = CONFLICT
            reason = f"分组行数不一致：立方{wanted['rows']}/缓存{found['rows']}"
        elif failed:
            status = PENDING
            reason = "上次更新失败，需要重新核对" if found["failed"] else "清单更新失败，需要重新核对"
        else:
            previous = prior.get(found["signature"])
            if previous is not None and previous.get("状态") == VERIFIED:
                reused = True
                reason = f"无变化分组复用批次 {previous.get('批次', '-')} 的核验结果"
            else:
                reason = "分组与度量已逐项核对"

        record = {
            "分区": partition,
            "分区标识": found["partition_id"],
            "分组": group,
            "度量": measures,
            "行数": found["rows"],
            "立方行数": wanted["rows"],
            "状态": status,
            "复用": reused,
            "更新失败": failed,
            "签名": found["signature"],
            "原因": reason,
        }
        group_records.append(record)

        if status == VERIFIED:
            verified_measure_map[partition].update(wanted["measures"] & found["measures"])
            if wanted["rows"] is not None:
                verified_total += wanted["rows"]
            else:
                verified_total_known = False

    # 缓存里多出来的分组不算覆盖，但会让所在分区无法判定完整。
    for key, extra in manifest_index.items():
        if key in expected_index:
            continue
        partition, group = key

        group_records.append({
            "分区": partition,
            "分区标识": extra["partition_id"],
            "分组": group,
            "度量": sorted(extra["measures"]),
            "行数": extra["rows"],
            "立方行数": None,
            "状态": CONFLICT,
            "复用": False,
            "更新失败": bool(extra["failed"]),
            "签名": extra["signature"],
            "原因": "缓存分组不在立方聚合明细中",
        })

    # 分区级判定：只有该分区所有期望分组都已核验且没有多余分组才算完整。
    expected_by_partition: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for record in group_records:
        expected_by_partition[record["分区"]].append(record)

    for partition in sorted(expected_partitions):
        records = expected_by_partition.get(partition, [])
        states = {record["状态"] for record in records}
        if CONFLICT in states or partition in duplicate_dates:
            partition_status[partition] = CONFLICT
        elif PENDING in states or partition not in manifest_partitions:
            partition_status[partition] = PARTIAL
        else:
            partition_status[partition] = VERIFIED

    for partition in base["extra_partitions"]:
        partition_status[partition] = CONFLICT

    base["verified_partitions"] = sorted(
        partition for partition, status in partition_status.items()
        if status == VERIFIED and partition in expected_partitions
    )
    base["partial_partitions"] = sorted(
        partition for partition, status in partition_status.items() if status == PARTIAL
    )

    expected_measures = set().union(*(group["measures"] for group in expected)) if expected else set()
    covered_measures = set().union(*verified_measure_map.values()) if verified_measure_map else set()
    base["covered_measures"] = sorted(covered_measures)
    base["missing_measures"] = sorted(expected_measures - covered_measures)

    cube_total = (
        sum(group["rows"] for group in expected if group["rows"] is not None)
        if all(group["rows"] is not None for group in expected) else None
    )
    if cube_total is None:
        cube_total = _int_or_none(cube.get("数据行数"))
    manifest_total = _manifest_total(unique_manifest)
    base["cube_total"] = cube_total
    base["manifest_total"] = manifest_total
    base["verified_total"] = verified_total if verified_total_known else None
    base["total_match"] = (
        cube_total is not None and manifest_total is not None and cube_total == manifest_total
    )

    base["reused_groups"] = sum(1 for record in group_records if record["复用"])
    base["rechecked_groups"] = sum(
        1 for record in group_records
        if record["状态"] == VERIFIED and not record["复用"]
    )
    base["pending_groups"] = sum(1 for record in group_records if record["状态"] == PENDING)
    base["conflict_groups"] = sum(1 for record in group_records if record["状态"] == CONFLICT)

    complete = (
        not base["missing_partitions"]
        and not base["extra_partitions"]
        and not duplicate_dates
        and not base["missing_measures"]
        and base["pending_groups"] == 0
        and base["conflict_groups"] == 0
        and base["total_match"] is True
    )
    base["complete"] = complete

    if complete:
        status = VERIFIED
        reason = "分区、分组与度量覆盖完整，总计相符"
    elif base["conflict_groups"] or base["extra_partitions"] or duplicate_dates:
        status = CONFLICT
        reason = base["reason"] or "分区或分组覆盖冲突"
        if duplicate_dates:
            reason = f"多个缓存分区指向同一分区日期：{','.join(duplicate_dates)}"
        elif base["extra_partitions"]:
            reason = f"缓存多出分区：{','.join(base['extra_partitions'])}"
    elif base["verified_partitions"] or base["reused_groups"]:
        status = PARTIAL
        reason = "仅部分分区/度量可证明覆盖"
    else:
        status = PENDING
        reason = "没有可证明覆盖的分组"

    if base["missing_partitions"]:
        reason += f"；缺少分区：{','.join(base['missing_partitions'])}"
    if base["missing_measures"]:
        reason += f"；缺少度量：{','.join(base['missing_measures'])}"
    if base["total_match"] is False:
        reason += (
            f"；总计不符（立方{base['cube_total']}/缓存{base['manifest_total']}）"
        )
    elif base["total_match"] is True and not complete:
        reason += "；总计相符但分区/度量覆盖不完整"

    base.update({
        "status": status,
        "reason": reason,
        "group_records": group_records,
        "duplicate_dates": duplicate_dates,
        "partition_status": partition_status,
    })
    return base


# ---------------------------------------------------------------------------
# 落库与批次汇总
# ---------------------------------------------------------------------------

def _next_id(table: str) -> int:
    rows = store.rows(table)
    return max((int(row.get("id", 0)) for row in rows), default=0) + 1


def _upsert_group_record(result: dict[str, Any], record: dict[str, Any]) -> int:
    rows = store.rows(GROUP_TABLE)
    cube_id = result["cube_id"]
    batch = result["batch"] or "未标注"
    signature = record["签名"]
    row = next(
        (item for item in rows
         if int(item.get("立方ID", 0)) == cube_id
         and item.get("批次") == batch
         and item.get("签名") == signature),
        None,
    )
    payload = {
        "核验编号": f"COVG-{uuid_short(cube_id, batch, signature)}",
        "立方ID": cube_id,
        "缓存条目ID": result["entry_id"],
        "批次": batch,
        "清单版本": result["version"],
        "分区": record["分区"],
        "分区标识": record["分区标识"],
        "分组": record["分组"],
        "度量": ",".join(record["度量"]),
        "行数": record["行数"],
        "立方行数": record["立方行数"],
        "状态": record["状态"],
        "复用": record["复用"],
        "更新失败": record["更新失败"],
        "签名": signature,
        "原因": record["原因"],
        "核验次数": 1,
        "核验时间": time.strftime("%Y-%m-%d %H:%M:%S"),
        "status": record["状态"],
        "pending": record["状态"] != VERIFIED,
        "abnormal": record["状态"] == CONFLICT,
    }
    if row is None:
        row = {"id": _next_id(GROUP_TABLE), **payload}
        rows.append(row)
    else:
        row.update({
            **payload,
            "核验次数": int(row.get("核验次数", 0)) + 1,
        })
    return int(row["id"])


def uuid_short(cube_id: int, batch: str, signature: str) -> str:
    return _signature(cube_id, batch, signature).upper()[:8]


def record_entry_result(result: dict[str, Any]) -> dict[str, Any]:
    """把单条清单核验结果写入分组记录与批次汇总，返回批次记录。"""
    record_ids = [
        _upsert_group_record(result, record)
        for record in result.get("group_records", [])
    ]
    batch = result["batch"] or "未标注"
    rows = store.rows(BATCH_TABLE)
    row = next(
        (item for item in rows
         if int(item.get("立方ID", 0)) == result["cube_id"]
         and item.get("批次") == batch
         and item.get("清单版本") == result["version"]),
        None,
    )
    payload = {
        "批次": batch,
        "立方ID": result["cube_id"],
        "缓存条目ID": result["entry_id"],
        "清单版本": result["version"],
        "总体状态": result["status"],
        "完整覆盖": result["complete"],
        "可证明整版": result["complete"],
        "期望分组数": result["expected_group_count"],
        "分组总数": len(result.get("group_records", [])),
        "复用分组数": result["reused_groups"],
        "重核分组数": result["rechecked_groups"],
        "待核验分组数": result["pending_groups"],
        "冲突分组数": result["conflict_groups"],
        "重复分组数": result["duplicate_groups"],
        "已核验分区": result["verified_partitions"],
        "局部分区": result["partial_partitions"],
        "缺失分区": result["missing_partitions"],
        "多余分区": result["extra_partitions"],
        "已覆盖度量": result["covered_measures"],
        "缺失度量": result["missing_measures"],
        "立方总计行数": result["cube_total"],
        "缓存总计行数": result["manifest_total"],
        "已核验总行数": result["verified_total"],
        "总计相符": result["total_match"],
        "原因": result["reason"],
        "分组记录": record_ids,
        "核验时间": time.strftime("%Y-%m-%d %H:%M:%S"),
        "status": result["status"],
        "pending": result["status"] != VERIFIED,
        "abnormal": result["status"] == CONFLICT,
    }
    if row is None:
        row = {"id": _next_id(BATCH_TABLE), **payload}
        rows.append(row)
    else:
        # 版本级冲突是跨多次核验累积的结论，重核时不能被单批次原因覆盖。
        if row.get("整版批次冲突"):
            payload["可证明整版"] = False
            if "多个刷新批次" not in payload["原因"]:
                payload["原因"] = (
                    (payload["原因"] + "；" if payload["原因"] else "")
                    + "同一版本存在多个刷新批次，只能按单批次证明局部覆盖，"
                    "整版汇总完整度冲突"
                )
        row.update(payload)
    result["batch_record"] = row
    return row


def freeze_superseded_versions(cube_id: int, active_version: str) -> None:
    """整版刷新后，旧版本的批次退为历史证据，不再参与读取裁定。"""
    for row in store.rows(BATCH_TABLE):
        if int(row.get("立方ID", 0)) != cube_id:
            continue
        if row.get("清单版本") == active_version:
            continue
        row["历史批次"] = True
        row["可证明整版"] = False


def _cache_entries_for_cube(cube: dict[str, Any]) -> list[dict[str, Any]]:
    cube_id = int(cube.get("id", 0))
    active_version = str(cube.get("清单版本") or "")
    entries: list[dict[str, Any]] = []
    for entry in store.rows("aggregate_cache"):
        try:
            entry_cube = int(str(entry.get("目标立方", "")).rsplit(":", 1)[-1])
        except ValueError:
            continue
        if entry_cube != cube_id:
            continue
        # 已被整版刷新替代的清单是历史证据，保持原批次记录，不再按新立方重核。
        if entry.get("被整版替代") or entry.get("status") == "已停用":
            continue
        # 旧版本的清单不再按当前立方重核，避免把历史批次改写成新版本的冲突。
        entry_version = str(_get(entry, _VERSION_KEYS, "") or "")
        if active_version and entry_version and entry_version != active_version:
            continue
        entries.append(entry)
    return entries


def verify_cube_coverage(cube_id: int) -> dict[str, Any]:
    """核验一个立方的全部缓存清单，并汇总版本级完整度。"""
    cube = store.find("olap_cube", cube_id)
    if cube is None:
        return {"ok": False, "message": "立方不存在"}

    prior: dict[str, dict[str, Any]] = {}
    active_version = str(cube.get("清单版本") or "")
    for row in store.rows(GROUP_TABLE):
        if int(row.get("立方ID", 0)) != cube_id:
            continue
        # 只有当前版本的已核验分组才能作为复用来历。
        if active_version and str(row.get("清单版本") or "") not in ("", active_version):
            continue
        if row.get("状态") == VERIFIED and row.get("签名"):
            prior[str(row["签名"])] = row

    entries = sorted(_cache_entries_for_cube(cube), key=lambda item: int(item.get("id", 0)))
    results: list[dict[str, Any]] = []
    for entry in entries:
        result = verify_entry(cube, entry, prior)
        record_entry_result(result)
        results.append(result)
        for record in result.get("group_records", []):
            # 后续批次可以复用本轮刚核验通过的分组，但不会因此合并批次。
            if record["状态"] == VERIFIED:
                prior[record["签名"]] = {
                    "状态": VERIFIED,
                    "批次": result["batch"] or "未标注",
                }

    version_entries = [result for result in results if result["batch"]]
    versions: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for result in version_entries:
        versions[result["version"]].append(result)

    version_summary: dict[str, Any] = {}

    def freeze_version(batch_results: list[dict[str, Any]], reason: str) -> None:
        for frozen in batch_results:
            batch_row = frozen.get("batch_record")
            if batch_row is None:
                continue
            batch_row["可证明整版"] = False
            if batch_row.get("整版批次冲突"):
                continue
            batch_row["整版批次冲突"] = True
            batch_row["原因"] = (batch_row.get("原因") or "") + "；" + reason

    for version, batch_results in versions.items():
        batches = {result["batch"] for result in batch_results}
        # 同一版本多轮刷新：按分组签名去重，重复分组不重复计数。
        seen: set[str] = set()
        unique_rows = 0
        unique_groups = 0
        for result in batch_results:
            for record in result.get("group_records", []):
                if record["状态"] != VERIFIED and not record["复用"]:
                    continue
                if record["签名"] in seen:
                    continue
                seen.add(record["签名"])
                unique_groups += 1
                if record["行数"] is not None:
                    unique_rows += int(record["行数"])
        # 整版完整必须由同一份批次单独证明；把多个批次的已核验部分拼起来
        # 只能得到“复用 + 整版汇总”的假完整，这里一律判为不成立。
        complete = len(batches) == 1 and all(
            result["complete"] for result in batch_results
        )
        reason = ""
        if len(batches) > 1:
            reason = (
                "同一版本存在多个刷新批次，只能按单批次证明局部覆盖，"
                "整版汇总完整度冲突"
            )
            freeze_version(batch_results, reason)
        version_summary[version or "未标注"] = {
            "版本": version or "未标注",
            "批次数": len(batches),
            "批次": sorted(batches),
            "整版完整": complete,
            "整版原因": reason or ("覆盖完整" if complete else "单批次覆盖不完整"),
            "可证明整版": complete,
            "去重分组数": unique_groups,
            "去重行数": unique_rows,
            "分组记录数": sum(len(result.get("group_records", [])) for result in batch_results),
        }

    usable_batches = [
        {
            "批次": result["batch"],
            "清单版本": result["version"],
            "状态": result["status"],
            "完整覆盖": result["complete"],
            "已核验分区": result["verified_partitions"],
            "已覆盖度量": result["covered_measures"],
            "分组记录": list(result.get("batch_record", {}).get("分组记录", [])),
        }
        for result in results if result["batch"]
    ]

    return {
        "ok": True,
        "cube_id": cube_id,
        "entries_checked": len(results),
        "latest_version": cube.get("清单版本"),
        "batch_entries": [
            {
                "entry_id": result["entry_id"],
                "batch": result["batch"],
                "version": result["version"],
                "status": result["status"],
                "complete": result["complete"],
                "reason": result["reason"],
                "verified_partitions": result["verified_partitions"],
                "covered_measures": result["covered_measures"],
            }
            for result in results
        ],
        "batches": usable_batches,
        "versions": version_summary,
        "coverage_records": store.rows(GROUP_TABLE),
        "batch_records": store.rows(BATCH_TABLE),
    }


def coverage_records(cube_id: int) -> dict[str, Any]:
    """读取立方的可复用覆盖核验记录（含历史缺字段的兼容读取）。"""
    groups = [
        row for row in store.rows(GROUP_TABLE)
        if int(row.get("立方ID", 0)) == cube_id
    ]
    batches = [
        row for row in store.rows(BATCH_TABLE)
        if int(row.get("立方ID", 0)) == cube_id
    ]
    return {
        "ok": True,
        "cube_id": cube_id,
        "group_records": groups,
        "batch_records": batches,
        "verified_groups": sum(1 for row in groups if row.get("状态") == VERIFIED),
        "pending_groups": sum(1 for row in groups if row.get("状态") == PENDING),
        "conflict_groups": sum(1 for row in groups if row.get("状态") == CONFLICT),
    }


# ---------------------------------------------------------------------------
# 多维立方读取：按核验记录裁定完整/局部范围
# ---------------------------------------------------------------------------

def _requested_partitions(cube: dict[str, Any], partitions: Iterable[str] | None) -> list[str]:
    if partitions is not None:
        return [str(item) for item in partitions]
    expected = expected_groups(cube)
    if expected:
        return sorted({group["partition"] for group in expected})
    listed = _load_list(_get(cube, _PARTITION_LIST_KEYS))
    if listed:
        return [str(item) for item in listed]
    return []


def plan_cube_read(
    cube_id: int,
    *,
    partitions: Iterable[str] | None = None,
    measures: Iterable[str] | None = None,
) -> dict[str, Any]:
    """用单一批次的核验记录裁定读取范围，并回写立方刷新清单。"""
    cube = store.find("olap_cube", cube_id)
    if cube is None:
        return {"ok": False, "message": "立方不存在"}

    requested_partitions = _requested_partitions(cube, partitions)
    if measures is None:
        requested_measures = sorted(measure_set(cube.get("度量列表", "")))
    else:
        requested_measures = [str(item) for item in measures]

    batch_rows = [
        row for row in store.rows(BATCH_TABLE)
        if int(row.get("立方ID", 0)) == cube_id and row.get("批次")
    ]
    if not batch_rows:
        # 还没有核验记录时先按当前缓存清单核验一轮，之后直接复用。
        if _cache_entries_for_cube(cube):
            verify_cube_coverage(cube_id)
            batch_rows = [
                row for row in store.rows(BATCH_TABLE)
                if int(row.get("立方ID", 0)) == cube_id and row.get("批次")
            ]
    latest_version = str(cube.get("清单版本") or "")
    if not latest_version and batch_rows:
        latest_version = str(batch_rows[-1].get("清单版本") or "")
    history_ids = {
        int(row.get("id", 0)) for row in batch_rows
        if row.get("清单版本") != latest_version
    }
    candidates = [
        row for row in batch_rows
        if int(row.get("id", 0)) not in history_ids
        and row.get("总体状态") != PENDING
    ]
    candidates.sort(key=lambda row: int(row.get("id", 0)), reverse=True)

    chosen: dict[str, Any] | None = None
    best: dict[str, Any] | None = None
    best_score = -1
    for row in candidates:
        verified_partitions = set(row.get("已核验分区", []))
        covered_measures = set(row.get("已覆盖度量", []))
        partition_hits = len(set(requested_partitions) & verified_partitions)
        measure_hits = len(set(requested_measures) & covered_measures)
        score = partition_hits * 1000 + measure_hits
        if score > best_score:
            best, best_score = row, score
        if (
            set(requested_partitions) <= verified_partitions
            and set(requested_measures) <= covered_measures
        ):
            chosen = row
            break

    considered = chosen or best
    if considered is None:
        used_partitions: list[str] = []
        used_measures: list[str] = []
        uncovered_partitions = list(requested_partitions)
        uncovered_measures = list(requested_measures)
        scope = "无法证明"
        batch = ""
        record_ids: list[int] = []
        version = latest_version
    else:
        used_partitions = sorted(set(requested_partitions) & set(considered.get("已核验分区", [])))
        used_measures = sorted(set(requested_measures) & set(considered.get("已覆盖度量", [])))
        uncovered_partitions = sorted(set(requested_partitions) - set(used_partitions))
        uncovered_measures = sorted(set(requested_measures) - set(used_measures))
        scope = "完整读取" if (
            not uncovered_partitions and not uncovered_measures
        ) else "局部读取"
        batch = str(considered.get("批次", ""))
        record_ids = list(considered.get("分组记录", []))
        version = considered.get("清单版本")

    readback = {
        "批次": batch,
        "清单版本": version,
        "读取范围": scope,
        "分区": used_partitions,
        "未覆盖分区": uncovered_partitions,
        "度量": used_measures,
        "未覆盖度量": uncovered_measures,
        "核验记录": record_ids,
        "核验状态": considered.get("总体状态") if considered else PENDING,
        "立方总计行数": considered.get("立方总计行数") if considered else None,
        "缓存总计行数": considered.get("缓存总计行数") if considered else None,
        "总计相符": considered.get("总计相符") if considered else None,
        "核验时间": time.strftime("%Y-%m-%d %H:%M:%S"),
    }

    refresh_manifest = _load_list(cube.get("立方刷新清单")) or []
    refresh_manifest.append(readback)
    cube["立方刷新清单"] = refresh_manifest[-20:]
    cube["最近覆盖核验"] = {
        "批次": batch,
        "读取范围": scope,
        "核验记录数": len(record_ids),
        "时间": readback["核验时间"],
    }

    return {
        "ok": True,
        "cube_id": cube_id,
        "scope": scope,
        "batch": batch,
        "version": version,
        "partitions": used_partitions,
        "uncovered_partitions": uncovered_partitions,
        "measures": used_measures,
        "uncovered_measures": uncovered_measures,
        "coverage_record_ids": record_ids,
        "cube_total": readback["立方总计行数"],
        "cache_total": readback["缓存总计行数"],
        "total_match": readback["总计相符"],
        "readback": readback,
        "cube_refresh_manifest": cube["立方刷新清单"],
    }
