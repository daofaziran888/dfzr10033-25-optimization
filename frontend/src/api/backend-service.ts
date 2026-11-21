import { request } from './client'
import { moduleMeta } from '@/data/modules'
import type {
  ActionResult,
  CoverageRecordsResult,
  EntryRow,
  OverviewResult,
  PageResult,
  ReadPlanResult,
} from '@/data/types'

export { moduleMeta }

export async function listEntries(
  key: string,
  filters: Record<string, string> = {},
): Promise<PageResult> {
  const query = new URLSearchParams(filters).toString()
  const response = await request(`/api/${key}?${query}`)
  if (!response.ok) throw new Error('业务数据读取失败')
  return await response.json()
}

export async function createEntry(
  key: string,
  values: Record<string, string>,
): Promise<ActionResult> {
  const response = await request(`/api/${key}`, {
    method: 'POST',
    body: JSON.stringify({ values }),
  })
  return await response.json()
}

export async function runAction(
  key: string,
  id: number,
  action: string,
): Promise<ActionResult> {
  const response = await request(`/api/${key}/${id}/actions`, {
    method: 'POST',
    body: JSON.stringify({ action }),
  })
  return await response.json()
}

export async function downloadEntries(
  key: string,
  filters: Record<string, string> = {},
): Promise<void> {
  const query = new URLSearchParams(filters).toString()
  const response = await request(`/api/${key}/export?${query}`)
  if (!response.ok) throw new Error('业务数据导出失败')
  const url = URL.createObjectURL(await response.blob())
  const anchor = document.createElement('a')
  anchor.href = url
  anchor.download = `${key}.csv`
  document.body.appendChild(anchor)
  anchor.click()
  document.body.removeChild(anchor)
  URL.revokeObjectURL(url)
}

export async function loadOverview(): Promise<OverviewResult> {
  const response = await request('/api/overview')
  if (!response.ok) throw new Error('运营概览读取失败')
  return await response.json()
}

/** 覆盖核验链路：按立方维度的分区/度量覆盖核验与读取裁定。 */

export async function verifyCoverage(cubeId: number): Promise<ActionResult> {
  const response = await request(`/api/olap_cube/${cubeId}/coverage-verify`, { method: 'POST' })
  if (!response.ok) throw new Error('覆盖核验失败')
  return await response.json()
}

export async function loadCoverageRecords(cubeId: number): Promise<CoverageRecordsResult> {
  const response = await request(`/api/olap_cube/${cubeId}/coverage-records`)
  if (!response.ok) throw new Error('覆盖核验记录读取失败')
  return await response.json()
}

export async function readPlan(
  cubeId: number,
  partitions: string[] = [],
  measures: string[] = [],
): Promise<ReadPlanResult> {
  const query = new URLSearchParams()
  if (partitions.length) query.set('partitions', partitions.join(','))
  if (measures.length) query.set('measures', measures.join(','))
  const response = await request(`/api/olap_cube/${cubeId}/read-plan?${query.toString()}`, {
    method: 'POST',
  })
  if (!response.ok) throw new Error('覆盖读取裁定失败')
  return await response.json()
}

export async function refreshCubePartitions(
  cubeId: number,
  partitions: string[] = [],
): Promise<ActionResult> {
  const query = partitions.length ? `?partitions=${encodeURIComponent(partitions.join(','))}` : ''
  const response = await request(`/api/olap_cube/${cubeId}/refresh${query}`, { method: 'POST' })
  if (!response.ok) throw new Error('立方刷新失败')
  return await response.json()
}

export async function markCacheUpdateFailed(cacheId: number): Promise<ActionResult> {
  const response = await request(`/api/aggregate_cache/${cacheId}/mark-update-failed`, {
    method: 'POST',
  })
  return await response.json()
}
