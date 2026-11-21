# 湖仓工坊 · 联邦查询与多维自助分析平台

面向异构数据源联邦查询、语义模型构建、OLAP多维立方物化、聚合缓存预热、时序窗口分析、查询改写优化与数据集市发布的多维自助分析平台。

这是一个前后端分离的管理平台：前端 Vue 3 + Vite + TypeScript，后端 FastAPI（Python）。
两边各自独立启动，前端 dev server 已关掉自动打开页面，启动后按终端打印的地址手工打开。

## 目录结构

```text
.
├── frontend/                 Vue 3 + Vite + TypeScript 前端
│   ├── src/views/WorkbenchView.vue|tsx  全部模块共享工作台
│   ├── src/data/modules.ts              18个业务模块配置
│   ├── src/api/              统一请求封装
│   └── vite.config.ts        dev server 配置（open: false）
├── backend/                  FastAPI（Python） 后端
│   ├── app/routers/resource.py  通用查询/导出/登记/动作接口
│   ├── app/services/<key>.py    每个模块独立领域逻辑
│   ├── app/services/base.py     共享服务骨架
│   └── app/store.py          内存数据仓库与示例数据
├── .gitignore
└── docker-compose.yml
```

## 启动

### 后端

```bash
cd backend
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
./run.sh
```

健康检查：`curl http://127.0.0.1:8000/api/health`

### 前端

```bash
cd frontend
npm ci
npm run dev
```

前端默认监听 `http://127.0.0.1:5173/`，dev server 不会自动打开浏览器，
需要自己访问。`/api` 由 vite 代理到后端 `http://127.0.0.1:8000`。

## 业务模块

| 模块 | 目录 | 业务对象 | 主要字段 |
| --- | --- | --- | --- |
| 数据源注册 | `source_registry` | 注册数据源 | 源编号、源类型、连接地址 |
| 语义模型 | `semantic_model` | 语义定义 | 模型编号、模型名称、源表映射 |
| 联邦查询 | `query_federation` | 联邦计划 | 计划编号、参与来源、关联键 |
| 多维立方 | `olap_cube` | OLAP立方 | 立方编号、立方名称、维度列表 |
| 指标工厂 | `metric_factory` | 计算指标 | 指标编号、指标名称、数据来源 |
| 维度树 | `dimension_tree` | 维度层级 | 层级编号、维度名称、层级路径 |
| 聚合缓存 | `aggregate_cache` | 缓存配置 | 配置编号、目标立方、缓存粒度 |
| 查询改写 | `query_rewrite` | 改写规则 | 规则编号、原始模式、替换模式 |
| 时序窗口 | `time_window` | 窗口定义 | 窗口编号、窗口类型、滑动宽度 |
| 数据切片 | `dataset_slice` | 切片配置 | 切片编号、源立方、维度过筛 |
| 下钻配置 | `drill_config` | 下钻路径 | 路径编号、起始维度、目标维度 |
| 结果集市 | `result_mart` | 数据Mart | 集市编号、集市名称、数据来源 |
| 基准集 | `benchmark_set` | 分析基线 | 基线编号、基线名称、数据范围 |
| 查询审计 | `query_audit` | 审计记录 | 记录编号、查询语句、执行耗时 |
| 成本治理 | `cost_govern` | 配额策略 | 策略编号、适用角色、查询上限 |
| 新鲜度监控 | `freshness_mon` | 新鲜度规则 | 规则编号、目标数据集、SLA秒数 |
| 分析导出 | `data_exporter` | 导出任务 | 任务编号、数据来源、筛选条件 |
| 定时计算 | `schedule_job` | 预计算任务 | 任务编号、目标立方、Cron表达式 |
| 湖仓读取 | `data_lake_read` | 湖仓路径 | 路径编号、存储位置、文件格式 |
| 结果缓存 | `result_cache` | 缓存条目 | 条目编号、查询摘要、结果哈希 |
| 数据契约 | `data_contract` | 契约定义 | 契约编号、数据集、Schema约束 |
| 实验配置 | `ab_config` | AB实验 | 实验编号、实验名称、分流比例 |

## 约定

- 全部模块共享`frontend/src/views/WorkbenchView.vue|tsx`和一套资源路由。
- 每个模块仍保留独立`backend/app/services/<模块>.py`；纯前端保留
  `frontend/src/domain/<模块>.ts`，用于承载真实业务差异并维持36题位容量。
- 列表接口统一返回 `{ items, total, page, size }`，动作接口统一返回 `{ ok, message }`。
- 状态流转只允许在 `app/services` 里改，路由层不做业务判断。

## 聚合缓存覆盖核验

`backend/app/services/cache_coverage.py` 是缓存核验链路的共享引擎。它不再拿
立方总行数和缓存总行数做比较，而是把缓存清单拆到 `(分区, 分组, 度量)`：

- **同源批次**：每条 `aggregate_cache` 的 `缓存清单` 分组都带 `批次`，清单与
  `覆盖批次` 不一致时整条清单保持“待核验”。
- **分区与度量覆盖**：分组按 `(分区, 分组)` 与立方 `聚合明细` 逐项比对；行数
  只作辅助，缺分区、缺度量、多分区、重复指向同一分区日期都会判冲突。
- **重复分组只计一次**：同一批次内的重复分组只保留一份，总计按去重后清单计算。
- **复用与重核**：分组签名一致且历史批次已核验 → 标记 `复用`；`更新失败` 或
  清单级更新失败的分组一律重新核对。
- **历史兼容**：没有清单/批次元数据的旧缓存无法证明覆盖，保持“待核验”。
- **版本完整度**：同一 `清单版本` 出现多个刷新批次时，各批次只能证明局部覆盖，
  整版汇总一律不成立；整版刷新开启新版本后恢复单批次证明。
- **读取裁定**：`POST /api/olap_cube/{id}/read-plan` 只按单一批次的核验记录
  裁定“完整读取/局部读取”，并把实际使用的批次、分区、度量和核验记录回写到
  立方的 `立方刷新清单` 与 `最近覆盖核验`。

相关接口：

| 接口 | 用途 |
| --- | --- |
| `POST /api/olap_cube/{id}/coverage-verify` | 核验并生成分组/批次核验记录 |
| `GET /api/olap_cube/{id}/coverage-records` | 读取可复用的核验记录 |
| `POST /api/olap_cube/{id}/read-plan` | 按核验记录裁定完整/局部读取并回写 |
| `POST /api/olap_cube/{id}/refresh?partitions=...` | 局部/整版刷新并立刻复核覆盖 |
| `POST /api/aggregate_cache/{id}/mark-update-failed` | 标记更新失败，强制重核 |

聚合缓存与多维立方的列表页会显示覆盖面板：批次明细、已核验/缺失分区、度量、
复用与重核分组数、总计核对以及读取范围。


领域：数据工程
子方向：数据分析与数据应用
