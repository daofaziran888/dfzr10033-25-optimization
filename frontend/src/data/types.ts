/** 纯前端数据层的公共类型：与全栈版后端返回的结构保持一致，换回后端时页面不用改。 */

export type EntryRow = {
  id: number
  status: string
  pending: boolean
  abnormal: boolean
  [field: string]: string | number | boolean
}

export type ModuleMeta = {
  key: string
  name: string
  entity: string
  desc: string
  fields: string[]
  statuses: string[]
  actions: string[]
  actionTargets: Record<string, string>
  metrics: string[]
}

export type PageResult = {
  items: EntryRow[]
  total: number
  page: number
  size: number
}

export type ActionResult = {
  ok: boolean
  message: string
}

export type OverviewResult = {
  cards: { label: string; value: number }[]
  modules: { name: string; created: number; pending: number; abnormal: number }[]
}

/** 缓存覆盖核验：分组记录与批次汇总，供聚合缓存/多维立方共用。 */
export type CoverageGroupRecord = {
  id: number
  核验编号: string
  批次: string
  分区: string
  分组: string
  度量: string
  状态: string
  复用: boolean
  更新失败: boolean
  原因: string
}

export type CoverageBatchRecord = {
  id: number
  批次: string
  立方ID: number
  清单版本: string
  总体状态: string
  完整覆盖: boolean
  复用分组数: number
  重核分组数: number
  待核验分组数: number
  冲突分组数: number
  已核验分区: string[]
  缺失分区: string[]
  多余分区: string[]
  已覆盖度量: string[]
  缺失度量: string[]
  立方总计行数: number | null
  缓存总计行数: number | null
  总计相符: boolean | null
  原因: string
}

export type CoverageRecordsResult = {
  ok: boolean
  cube_id: number
  group_records: CoverageGroupRecord[]
  batch_records: CoverageBatchRecord[]
  verified_groups: number
  pending_groups: number
  conflict_groups: number
}

export type ReadPlanResult = {
  ok: boolean
  cube_id: number
  scope: string
  batch: string
  version: string
  partitions: string[]
  uncovered_partitions: string[]
  measures: string[]
  uncovered_measures: string[]
  coverage_record_ids: number[]
  cube_total: number | null
  cache_total: number | null
  total_match: boolean | null
}

