<template>
  <section class="page" :data-module="meta.key">
    <header class="page-head">
      <div>
        <h2>{{ meta.name }}</h2>
        <p class="page-desc">{{ meta.desc }}</p>
      </div>
      <div class="page-actions">
        <button class="btn primary" data-testid="open-create" type="button" @click="openCreate">
          登记{{ meta.entity }}
        </button>
        <button class="btn" data-testid="export" type="button" @click="exportRows">
          导出{{ meta.name }}清单
        </button>
      </div>
    </header>

    <div class="stat-row">
      <article v-for="item in stats" :key="item.label" class="stat-card">
        <span class="stat-label">{{ item.label }}</span>
        <strong class="stat-value">{{ item.value }}</strong>
      </article>
    </div>

    <p class="status-legend">
      <span v-for="item in statusSummary" :key="item.status" class="legend-item">
        {{ item.status }}：{{ item.count }}
      </span>
    </p>

    <section
      v-if="coverageCubeId"
      class="coverage-panel"
      data-testid="coverage-panel"
    >
      <header class="coverage-head">
        <div>
          <h3>缓存覆盖核验</h3>
          <p class="page-desc">
            按分区、分组和度量逐项比对立方聚合明细；只有同一批次能证明覆盖，
            无变化分组复用已核验结果，更新失败的分组重新核对。
          </p>
        </div>
        <div class="page-actions">
          <button class="btn" data-testid="coverage-verify" type="button" @click="refreshCoverage">
            重新核验覆盖
          </button>
          <button class="btn" data-testid="coverage-read" type="button" @click="runReadPlan">
            按核验结果读取
          </button>
        </div>
      </header>

      <div class="stat-row">
        <article class="stat-card">
          <span class="stat-label">已核验分组</span>
          <strong class="stat-value">{{ coverage.verified_groups ?? 0 }}</strong>
        </article>
        <article class="stat-card">
          <span class="stat-label">待核验分组</span>
          <strong class="stat-value">{{ coverage.pending_groups ?? 0 }}</strong>
        </article>
        <article class="stat-card">
          <span class="stat-label">冲突分组</span>
          <strong class="stat-value">{{ coverage.conflict_groups ?? 0 }}</strong>
        </article>
      </div>

      <table class="data-table" data-testid="coverage-batches">
        <thead>
          <tr>
            <th>覆盖批次</th>
            <th>清单版本</th>
            <th>总体状态</th>
            <th>已核验分区</th>
            <th>缺失分区</th>
            <th>已覆盖度量</th>
            <th>复用/重核</th>
            <th>立方总计/缓存总计</th>
            <th>原因</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="batch in coverage.batch_records" :key="batch.id" :data-status="batch.总体状态">
            <td>{{ batch.批次 }}</td>
            <td>{{ batch.清单版本 }}</td>
            <td>{{ batch.总体状态 }}</td>
            <td>{{ (batch.已核验分区 || []).join('、') || '—' }}</td>
            <td>{{ (batch.缺失分区 || []).join('、') || '—' }}</td>
            <td>{{ (batch.已覆盖度量 || []).join('、') || '—' }}</td>
            <td>{{ batch.复用分组数 }} / {{ batch.重核分组数 }}</td>
            <td>{{ batch.立方总计行数 ?? '—' }} / {{ batch.缓存总计行数 ?? '—' }}</td>
            <td class="coverage-reason">{{ batch.原因 }}</td>
          </tr>
          <tr v-if="!coverage.batch_records?.length">
            <td colspan="9" class="empty-state">暂无覆盖核验记录，可先点击“重新核验覆盖”</td>
          </tr>
        </tbody>
      </table>

      <p v-if="readback" class="coverage-readback" data-testid="coverage-readback">
        读取范围：{{ readback.scope }}；批次 {{ readback.batch || '—' }}；
        可读分区 {{ readback.partitions.join('、') || '无' }}；
        未覆盖分区 {{ readback.uncovered_partitions.join('、') || '无' }}；
        未覆盖度量 {{ readback.uncovered_measures.join('、') || '无' }}；
        总计 {{ readback.cube_total ?? '—' }} / {{ readback.cache_total ?? '—' }}。
      </p>

      <details class="coverage-groups">
        <summary>分组核验明细（{{ coverage.group_records?.length ?? 0 }} 条）</summary>
        <table class="data-table">
          <thead>
            <tr>
              <th>分区</th>
              <th>分组</th>
              <th>度量</th>
              <th>状态</th>
              <th>复用</th>
              <th>更新失败</th>
              <th>原因</th>
            </tr>
          </thead>
          <tbody>
            <tr v-for="group in coverageGroupPreview" :key="group.id" :data-status="group.状态">
              <td>{{ group.分区 || '—' }}</td>
              <td class="coverage-group">{{ group.分组 }}</td>
              <td>{{ group.度量 }}</td>
              <td>{{ group.状态 }}</td>
              <td>{{ group.复用 ? '是' : '否' }}</td>
              <td>{{ group.更新失败 ? '是' : '否' }}</td>
              <td class="coverage-reason">{{ group.原因 }}</td>
            </tr>
          </tbody>
        </table>
      </details>
    </section>

    <form class="filter-bar" @submit.prevent="reload">
      <label v-for="field in filterFields" :key="field" class="filter-item">
        <span>{{ field }}</span>
        <input v-model="filters[field]" :data-testid="`filter-${field}`" :placeholder="`按${field}检索`" />
      </label>
      <button class="btn" data-testid="query" type="submit">查询</button>
      <button class="btn ghost" data-testid="reset" type="button" @click="resetFilters">重置条件</button>
    </form>

    <table class="data-table">
      <thead>
        <tr>
          <th v-for="column in meta.fields" :key="column">{{ column }}</th>
          <th>当前状态</th>
          <th>可执行动作</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="row in rows" :key="String(row.id)" :data-testid="`row-${row.id}`" :data-status="String(row.status)">
          <td v-for="column in meta.fields" :key="column">{{ row[column] ?? '—' }}</td>
          <td>{{ row.status }}</td>
          <td class="row-actions">
            <button
              v-for="action in meta.actions"
              :key="action"
              class="link"
              type="button"
              :data-testid="`action-${row.id}-${action}`"
              @click="runAction(action, row)"
            >
              {{ action }}
            </button>
          </td>
        </tr>
        <tr v-if="!rows.length">
          <td :colspan="meta.fields.length + 2" class="empty-state">
            暂无{{ meta.name }}数据，可先登记{{ meta.entity }}
          </td>
        </tr>
      </tbody>
    </table>

    <div v-if="showCreate" class="modal-backdrop" data-testid="create-modal">
      <form class="modal" @submit.prevent="submitCreate">
        <h3>登记{{ meta.entity }}</h3>
        <label v-for="field in meta.fields" :key="field" class="create-field">
          <span>{{ field }}</span>
          <input v-model="createValues[field]" :data-testid="`create-${field}`" required />
        </label>
        <div class="page-actions">
          <button class="btn" type="button" @click="showCreate = false">取消</button>
          <button class="btn primary" data-testid="submit-create" type="submit" :disabled="creating">
            {{ creating ? '提交中…' : '确认登记' }}
          </button>
        </div>
      </form>
    </div>

    <footer class="page-foot">
      <span>共 {{ total }} 条{{ meta.name }}记录</span>
      <span v-if="errorMessage" class="error-text">{{ errorMessage }}</span>
    </footer>
  </section>
</template>

<script setup lang="ts">
import { computed, onMounted, ref, watch } from 'vue'

import {
  createEntry,
  downloadEntries,
  listEntries,
  loadCoverageRecords,
  moduleMeta,
  readPlan,
  runAction as applyAction,
  verifyCoverage,
} from '@/api/backend-service'
import type { CoverageRecordsResult, EntryRow, ReadPlanResult } from '@/data/types'

const props = defineProps<{ moduleKey: string }>()
const meta = moduleMeta(props.moduleKey)
const rows = ref<EntryRow[]>([])
const total = ref(0)
const errorMessage = ref('')
const filters = ref<Record<string, string>>({})
const filterFields = meta.fields.slice(0, 3)
const showCreate = ref(false)
const creating = ref(false)
const createValues = ref<Record<string, string>>({})
const coverage = ref<Partial<CoverageRecordsResult>>({})
const readback = ref<ReadPlanResult | null>(null)
const COVERAGE_MODULES = ['aggregate_cache', 'olap_cube']
const coverageEnabled = computed(() => COVERAGE_MODULES.includes(meta.key))
const coverageCubeId = computed(() => {
  if (!coverageEnabled.value) return 0
  if (meta.key === 'olap_cube') {
    const row = rows.value.find((item) => Number(item.id) > 0)
    return row ? Number(row.id) : 0
  }
  // 聚合缓存按目标立方引用（olap_cube:1）定位立方；优先取生效中的清单。
  const candidates = rows.value.filter((item) => String(item['目标立方'] || '').includes(':'))
  const row = candidates.find((item) => item.status === '生效中') ?? candidates[0]
  const ref = String(row?.['目标立方'] || '')
  const id = Number(ref.split(':').pop())
  return Number.isFinite(id) ? id : 0
})
const coverageGroupPreview = computed(() =>
  (coverage.value.group_records || []).slice(0, 50),
)
const stats = computed(() => [
  { label: meta.metrics[0], value: total.value },
  { label: meta.metrics[1], value: rows.value.filter((row) => row.status === meta.statuses[1]).length },
  {
    label: meta.metrics[2],
    value: rows.value.filter((row) => row.status === meta.statuses[meta.statuses.length - 1]).length,
  },
])
const statusSummary = computed(() =>
  meta.statuses.map((status) => ({
    status,
    count: rows.value.filter((row) => String(row.status) === status).length,
  })),
)

function resetFilters() {
  filters.value = {}
  void reload()
}

async function exportRows() {
  errorMessage.value = ''
  try {
    await downloadEntries(meta.key, filters.value)
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '导出失败'
  }
}

function openCreate() {
  createValues.value = Object.fromEntries(meta.fields.map((field) => [field, '']))
  errorMessage.value = ''
  showCreate.value = true
}

async function submitCreate() {
  creating.value = true
  errorMessage.value = ''
  try {
    const result = await createEntry(meta.key, createValues.value)
    if (!result.ok) throw new Error(result.message)
    showCreate.value = false
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '登记失败'
  } finally {
    creating.value = false
  }
}

async function runAction(action: string, row: EntryRow) {
  errorMessage.value = ''
  try {
    const result = await applyAction(meta.key, Number(row.id), action)
    if (!result.ok) throw new Error(result.message)
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '操作失败'
  }
}

async function reload() {
  errorMessage.value = ''
  try {
    const payload = await listEntries(meta.key, filters.value)
    rows.value = payload.items
    total.value = payload.total
    await loadCoverage()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '业务列表读取失败'
  }
}

async function loadCoverage() {
  coverage.value = {}
  readback.value = null
  if (!coverageCubeId.value) return
  try {
    coverage.value = await loadCoverageRecords(coverageCubeId.value)
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '覆盖核验记录读取失败'
  }
}

async function refreshCoverage() {
  if (!coverageCubeId.value) return
  errorMessage.value = ''
  try {
    await verifyCoverage(coverageCubeId.value)
    await loadCoverage()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '覆盖核验失败'
  }
}

async function runReadPlan() {
  if (!coverageCubeId.value) return
  errorMessage.value = ''
  try {
    readback.value = await readPlan(coverageCubeId.value)
    await loadCoverage()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '覆盖读取裁定失败'
  }
}

watch(
  () => coverageCubeId.value,
  () => {
    if (coverageCubeId.value) void loadCoverage()
  },
)

onMounted(reload)
</script>
