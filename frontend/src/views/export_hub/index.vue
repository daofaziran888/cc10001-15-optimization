<template>
  <section class="page" data-module="export_hub">
    <header class="page-head">
      <div>
        <h2>数据分发管理</h2>
        <p class="page-desc">维护采集结果的多通道分发配置、加密传输与回调通知，支撑下游系统消费。</p>
      </div>
      <div class="page-actions">
        <button class="btn primary" data-testid="open-create" type="button" @click="openCreate">登记分发通道</button>
        <button class="btn" data-testid="export" type="button" @click="exportRows">导出数据分发清单</button>
      </div>
    </header>

    <div class="stat-row">
      <article v-for="item in stats" :key="item.label" class="stat-card">
        <span class="stat-label">{{ item.label }}</span>
        <strong class="stat-value">{{ item.value }}</strong>
      </article>
    </div>

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
          <th v-for="column in columns" :key="column">{{ column }}</th>
          <th>可执行动作</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="row in rows" :key="String(row.id)" :data-testid="`row-${row.id}`" :data-status="String(row.status)">
          <td v-for="column in columns" :key="column">{{ row[column] ?? '—' }}</td>
          <td class="row-actions">
            <button
              v-for="action in actions"
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
          <td :colspan="columns.length + 1" class="empty-state">暂无数据分发数据，可先登记分发通道</td>
        </tr>
      </tbody>
    </table>

    <div v-if="showCreate" class="modal-backdrop" data-testid="create-modal">
      <form class="modal" @submit.prevent="submitCreate">
        <h3>登记分发通道</h3>
        <label v-for="field in columns" :key="field" class="create-field">
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

    <!-- ============ 批次分发与续传工作区 ============ -->
    <section class="dispatch-panel" data-testid="dispatch-panel">
      <header class="page-head">
        <div>
          <h3>批次分发与续传</h3>
          <p class="page-desc">
            读取结果批次、已有分发记录与通道配置，按「批次 × 通道 × 输出格式」去重后分发；
            页面日志区分已完成、重复跳过和待续传通道，每个有效结果只保留一份产物。
          </p>
        </div>
      </header>

      <form class="filter-bar dispatch-form" @submit.prevent="runDispatch">
        <label class="filter-item">
          <span>结果批次</span>
          <select v-model.number="dispatchForm.batch_id" data-testid="dispatch-batch-select" required>
            <option v-for="batch in dispatchBatches" :key="batch.batch_id" :value="batch.batch_id">
              #{{ batch.batch_id }} {{ batch.batch_code ?? '' }}（{{ batch.status }}）
            </option>
          </select>
        </label>
        <label class="filter-item">
          <span>输出格式</span>
          <select v-model="dispatchForm.channel_format" data-testid="dispatch-format-select">
            <option value="json">json</option>
            <option value="csv">csv</option>
          </select>
        </label>
        <label class="filter-item">
          <span>格式冲突策略</span>
          <select v-model="dispatchForm.policy" data-testid="dispatch-policy-select">
            <option value="channel_first">按通道保留首个格式</option>
            <option value="channel_format">按通道+格式分别保留</option>
          </select>
        </label>
        <label class="filter-item check-item">
          <input v-model="dispatchForm.simulate_failure" type="checkbox" data-testid="dispatch-simulate-failure" />
          <span>模拟通道失败</span>
        </label>
        <button class="btn primary" data-testid="dispatch-run" type="submit" :disabled="dispatching">
          {{ dispatching ? '分发中…' : '执行分发/续传' }}
        </button>
        <button class="btn" data-testid="dispatch-all" type="button" :disabled="dispatching" @click="runDispatchAll">
          一键续传所有批次
        </button>
        <button class="btn ghost" data-testid="dispatch-refresh" type="button" @click="reloadDispatch">刷新</button>
      </form>

      <div class="stat-row">
        <article class="stat-card">
          <span class="stat-label">已完成通道</span>
          <strong class="stat-value">{{ dispatchSummary.completed }}</strong>
        </article>
        <article class="stat-card">
          <span class="stat-label">重复跳过</span>
          <strong class="stat-value skip-text">{{ dispatchSummary.skipped }}</strong>
        </article>
        <article class="stat-card">
          <span class="stat-label">待续传通道</span>
          <strong class="stat-value fail-text">{{ dispatchSummary.failed }}</strong>
        </article>
        <article class="stat-card">
          <span class="stat-label">有效产物文件</span>
          <strong class="stat-value">{{ dispatchSummary.files }}</strong>
        </article>
      </div>

      <h4 class="table-title">批次分发状态</h4>
      <table class="data-table" data-testid="batch-status-table">
        <thead>
          <tr>
            <th>批次</th><th>批次编号</th><th>分发状态</th><th>已完成</th>
            <th>重复跳过</th><th>待续传</th><th>未覆盖通道</th><th>产物文件</th><th>操作</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="status in batchStatuses" :key="status.batch_id" :data-testid="`batch-status-${status.batch_id}`">
            <td>#{{ status.batch_id }}</td>
            <td>{{ status.batch_code ?? '—' }}</td>
            <td><span class="badge" :class="batchStatusClass(status.status)">{{ status.status }}</span></td>
            <td>{{ status.completed_channels }}</td>
            <td>{{ status.skipped_channels }}</td>
            <td>{{ status.failed_channels }}</td>
            <td>{{ status.pending_channels.length }}</td>
            <td>{{ status.file_count }}</td>
            <td class="row-actions">
              <button class="link" type="button" data-testid="view-logs" @click="selectBatch(status.batch_id)">
                查看日志
              </button>
              <button
                class="link" type="button" data-testid="package-batch"
                :disabled="status.file_count === 0" @click="downloadPackage(status.batch_id)"
              >
                打包下载
              </button>
            </td>
          </tr>
          <tr v-if="!batchStatuses.length">
            <td colspan="9" class="empty-state">暂无可分发的结果批次</td>
          </tr>
        </tbody>
      </table>

      <h4 class="table-title">分发日志（已完成 / 重复跳过 / 待续传）</h4>
      <table class="data-table" data-testid="dispatch-logs-table">
        <thead>
          <tr>
            <th>日志</th><th>批次</th><th>通道</th><th>输出格式</th><th>目标地址</th>
            <th>状态</th><th>原因</th><th>尝试次数</th><th>更新时间</th><th>操作</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="log in dispatchLogs" :key="String(log.id)" :data-testid="`dispatch-log-${log.id}`" :data-status="String(log.status)">
            <td>{{ log.id }}</td>
            <td>#{{ log.batch_id }}</td>
            <td>{{ log.channel_code }}（#{{ log.channel_id }}）</td>
            <td>{{ log.channel_format }}</td>
            <td class="target-cell">{{ log.target }}</td>
            <td><span class="badge" :class="logStatusClass(log.status)">{{ log.status }}</span></td>
            <td>{{ log.reason ?? '—' }}</td>
            <td>{{ log.attempt }}/{{ log.max_retries }}</td>
            <td>{{ log.updated_at }}</td>
            <td class="row-actions">
              <button
                v-if="log.status === '传输失败'"
                class="link" type="button" :data-testid="`resume-${log.id}`"
                :disabled="dispatching" @click="resumeLog(log.id)"
              >
                通道续传
              </button>
              <button
                v-if="log.file_id"
                class="link" type="button" :data-testid="`download-file-${log.file_id}`"
                @click="downloadFile(log.file_id)"
              >
                下载产物
              </button>
              <span v-if="log.status !== '传输失败' && !log.file_id" class="muted">—</span>
            </td>
          </tr>
          <tr v-if="!dispatchLogs.length">
            <td colspan="10" class="empty-state">当前批次暂无分发记录，先选择批次执行分发</td>
          </tr>
        </tbody>
      </table>

      <div class="page-actions table-actions">
        <button class="btn" data-testid="export-dispatch-logs" type="button" @click="exportDispatchLogs">
          导出分发日志 CSV
        </button>
      </div>
    </section>

    <footer class="page-foot">
      <span>共 {{ total }} 条数据分发记录</span>
      <span v-if="errorMessage" class="error-text">{{ errorMessage }}</span>
    </footer>
  </section>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'

import { request } from '@/api/client'

type Row = Record<string, string | number | null>

type BatchStatus = {
  batch_id: number
  batch_code: string | null
  status: string
  channels_total: number
  completed_channels: number
  skipped_channels: number
  failed_channels: number
  pending_channels: { channel_id: number; channel_code: string }[]
  file_count: number
}

type DispatchLog = {
  id: number
  batch_id: number
  channel_id: number
  channel_code: string
  channel_format: string
  target: string
  status: string
  reason: string | null
  attempt: number
  max_retries: number
  dispatched_at: string
  updated_at: string
  file_id: number | null
}

const ENDPOINT = '/api/export_hub'
const PIPELINE = '/api/pipeline'
const columns = ["通道编号", "输出格式", "目标地址", "加密方式", "压缩选项", "分片大小", "重试上限", "回调地址"]
const actions = ["启用通道", "暂停通道", "停用通道"]
const statuses = ["待配置", "已启用", "传输中", "已停用"]
const metricNames = ["通道总数", "已启用通道", "传输中通道"]

const rows = ref<Row[]>([])
const total = ref(0)
const errorMessage = ref('')
const filters = ref<Record<string, string>>({})
const filterFields = columns.slice(0, 3)
const showCreate = ref(false)
const creating = ref(false)
const createValues = ref<Record<string, string>>({})
const stats = computed(() => [
  { label: metricNames[0], value: total.value },
  { label: metricNames[1], value: rows.value.filter((row) => row.status === statuses[1]).length },
  { label: metricNames[2], value: rows.value.filter((row) => row.status === statuses[statuses.length - 1]).length },
])

// ---------- 批次分发与续传 ----------
const batchStatuses = ref<BatchStatus[]>([])
const dispatchLogs = ref<DispatchLog[]>([])
const selectedBatchId = ref<number | null>(null)
const dispatching = ref(false)
const dispatchForm = ref({
  batch_id: null as number | null,
  channel_format: 'json',
  policy: 'channel_first',
  simulate_failure: false,
})

const dispatchBatches = computed(() => batchStatuses.value)
const dispatchSummary = computed(() => ({
  completed: dispatchLogs.value.filter((log) => log.status === '已完成').length,
  skipped: dispatchLogs.value.filter((log) => log.status === '重复跳过').length,
  failed: dispatchLogs.value.filter((log) => log.status === '传输失败').length,
  files: batchStatuses.value.reduce((sum, item) => sum + item.file_count, 0),
}))

function logStatusClass(status: string): string {
  if (status === '已完成') return 'badge-done'
  if (status === '重复跳过') return 'badge-skip'
  return 'badge-fail'
}

function batchStatusClass(status: string): string {
  if (status === '全部完成') return 'badge-done'
  if (status === '无可用通道') return 'badge-skip'
  return 'badge-fail'
}

async function readError(response: Response, fallback: string): Promise<string> {
  try {
    const payload = await response.json()
    return payload.detail || payload.message || fallback
  } catch {
    return fallback
  }
}

async function triggerDownload(path: string, fallbackName: string) {
  const response = await request(path)
  if (!response.ok) throw new Error(await readError(response, '下载失败，请稍后重试'))
  const url = URL.createObjectURL(await response.blob())
  const link = document.createElement('a')
  link.href = url
  link.download = fallbackName
  link.click()
  URL.revokeObjectURL(url)
}

function selectBatch(batchId: number) {
  selectedBatchId.value = batchId
  dispatchForm.value.batch_id = batchId
  void reloadLogs()
}

async function reloadDispatch() {
  try {
    const [statusResponse, logResponse] = await Promise.all([
      request(`${PIPELINE}/dispatch/batch-status`),
      request(`${PIPELINE}/dispatch/logs${selectedBatchId.value === null ? '' : `?batch_id=${selectedBatchId.value}`}`),
    ])
    if (!statusResponse.ok) throw new Error('批次分发状态读取失败')
    if (!logResponse.ok) throw new Error('分发日志读取失败')
    batchStatuses.value = (await statusResponse.json()) as BatchStatus[]
    dispatchLogs.value = (await logResponse.json()) as DispatchLog[]
    if (dispatchForm.value.batch_id === null && batchStatuses.value.length > 0) {
      dispatchForm.value.batch_id = batchStatuses.value[0].batch_id
    }
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '分发工作区读取失败'
  }
}

async function reloadLogs() {
  const query = selectedBatchId.value === null ? '' : `?batch_id=${selectedBatchId.value}`
  try {
    const response = await request(`${PIPELINE}/dispatch/logs${query}`)
    if (!response.ok) throw new Error('分发日志读取失败')
    dispatchLogs.value = (await response.json()) as DispatchLog[]
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '分发日志读取失败'
  }
}

async function runDispatch() {
  if (dispatchForm.value.batch_id === null) {
    errorMessage.value = '请先选择要分发的结果批次'
    return
  }
  dispatching.value = true
  errorMessage.value = ''
  selectedBatchId.value = dispatchForm.value.batch_id
  try {
    const response = await request(`${PIPELINE}/dispatch/batch`, {
      method: 'POST',
      body: JSON.stringify({
        batch_id: dispatchForm.value.batch_id,
        channel_format: dispatchForm.value.channel_format,
        simulate_failure: dispatchForm.value.simulate_failure,
        policy: dispatchForm.value.policy,
      }),
    })
    if (!response.ok) throw new Error(await readError(response, '分发失败'))
    await reloadDispatch()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '分发失败'
  } finally {
    dispatching.value = false
  }
}

async function runDispatchAll() {
  dispatching.value = true
  errorMessage.value = ''
  try {
    const response = await request(`${PIPELINE}/dispatch/all-ready`, {
      method: 'POST',
      body: JSON.stringify({
        batch_id: dispatchForm.value.batch_id ?? 0,
        channel_format: dispatchForm.value.channel_format,
        simulate_failure: dispatchForm.value.simulate_failure,
        policy: dispatchForm.value.policy,
      }),
    })
    if (!response.ok) throw new Error(await readError(response, '一键续传失败'))
    await reloadDispatch()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '一键续传失败'
  } finally {
    dispatching.value = false
  }
}

async function resumeLog(logId: number) {
  dispatching.value = true
  errorMessage.value = ''
  try {
    const response = await request(`${PIPELINE}/dispatch/resume`, {
      method: 'POST',
      body: JSON.stringify({ log_id: logId, simulate_failure: dispatchForm.value.simulate_failure }),
    })
    if (!response.ok) throw new Error(await readError(response, '通道续传失败'))
    await reloadDispatch()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '通道续传失败'
  } finally {
    dispatching.value = false
  }
}

async function downloadPackage(batchId: number) {
  errorMessage.value = ''
  try {
    await triggerDownload(`${PIPELINE}/dispatch/package/${batchId}`, `batch-${batchId}-dispatch.zip`)
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '打包下载失败'
  }
}

async function downloadFile(fileId: number) {
  errorMessage.value = ''
  try {
    await triggerDownload(`${PIPELINE}/dispatch/files/${fileId}/download`, `dispatch-file-${fileId}`)
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '产物下载失败'
  }
}

async function exportDispatchLogs() {
  errorMessage.value = ''
  const query = selectedBatchId.value === null ? '' : `?batch_id=${selectedBatchId.value}`
  try {
    await triggerDownload(`${PIPELINE}/dispatch/export-logs${query}`, 'dispatch_logs.csv')
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '分发日志导出失败'
  }
}

// ---------- 通道 CRUD（原有能力保持不变） ----------
function resetFilters() {
  filters.value = {}
  void reload()
}

async function exportRows() {
  errorMessage.value = ''
  const query = new URLSearchParams(filters.value).toString()
  try {
    const response = await request(`${ENDPOINT}/export?${query}`)
    if (!response.ok) throw new Error('导出失败，请稍后重试')
    const url = URL.createObjectURL(await response.blob())
    const link = document.createElement('a')
    link.href = url
    link.download = 'export_hub.csv'
    link.click()
    URL.revokeObjectURL(url)
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '导出失败'
  }
}

function openCreate() {
  createValues.value = Object.fromEntries(columns.map((field) => [field, '']))
  errorMessage.value = ''
  showCreate.value = true
}

async function submitCreate() {
  creating.value = true
  errorMessage.value = ''
  try {
    const response = await request(ENDPOINT, {
      method: 'POST', body: JSON.stringify(createValues.value),
    })
    const payload = await response.json()
    if (!response.ok || !payload.ok) throw new Error(payload.message || '登记失败')
    showCreate.value = false
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '登记失败'
  } finally {
    creating.value = false
  }
}

async function runAction(action: string, row: Row) {
  errorMessage.value = ''
  try {
    const response = await request(`${ENDPOINT}/${row.id}/actions`, {
      method: 'POST',
      body: JSON.stringify({ action }),
    })
    if (!response.ok) {
      throw new Error('数据分发动作未生效，请稍后重试')
    }
    await Promise.all([reload(), reloadDispatch()])
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '数据分发操作失败'
  }
}

async function reload() {
  errorMessage.value = ''
  const query = new URLSearchParams(filters.value as Record<string, string>).toString()
  try {
    const response = await request(`${ENDPOINT}?${query}`)
    if (!response.ok) {
      throw new Error('分发通道列表读取失败')
    }
    const payload = await response.json()
    rows.value = payload.items ?? []
    total.value = payload.total ?? rows.value.length
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '数据分发列表读取失败'
  }
}

onMounted(() => {
  void reload()
  void reloadDispatch()
})
</script>

<style scoped>
.dispatch-panel {
  margin-top: 20px;
  background: #f8fafc;
  border: 1px solid var(--border);
  border-radius: 10px;
  padding: 14px 16px;
}
.dispatch-form {
  background: #fff;
  border: 1px solid var(--border);
  border-radius: 8px;
  padding: 12px;
}
.check-item {
  display: flex;
  align-items: center;
  gap: 6px;
}
.check-item input {
  width: 16px;
  height: 16px;
}
.table-title {
  margin: 16px 0 8px;
  font-size: 14px;
}
.table-actions {
  margin-top: 10px;
}
.badge {
  display: inline-block;
  padding: 2px 8px;
  border-radius: 10px;
  font-size: 12px;
  white-space: nowrap;
}
.badge-done {
  background: #dcfce7;
  color: #15803d;
}
.badge-skip {
  background: #f1f5f9;
  color: #64748b;
}
.badge-fail {
  background: #fee2e2;
  color: #b91c1c;
}
.skip-text {
  color: #64748b;
}
.fail-text {
  color: #b91c1c;
}
.target-cell {
  max-width: 240px;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}
.muted {
  color: var(--muted);
  font-size: 12px;
}
button:disabled {
  opacity: 0.5;
  cursor: not-allowed;
}
</style>
