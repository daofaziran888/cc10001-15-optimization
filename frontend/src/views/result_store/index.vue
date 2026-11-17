<template>
  <section class="page" data-module="result_store">
    <header class="page-head">
      <div>
        <h2>采集仓储管理</h2>
        <p class="page-desc">维护采集结果的存储批次、完整性校验与版本追溯，保证原始数据可回溯。</p>
      </div>
      <div class="page-actions">
        <button class="btn primary" data-testid="open-create" type="button" @click="openCreate">登记结果批次</button>
        <button class="btn" data-testid="export" type="button" @click="exportRows">导出采集仓储清单</button>
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
          <td :colspan="columns.length + 1" class="empty-state">暂无采集仓储数据，可先登记结果批次</td>
        </tr>
      </tbody>
    </table>

    <div v-if="showCreate" class="modal-backdrop" data-testid="create-modal">
      <form class="modal" @submit.prevent="submitCreate">
        <h3>登记结果批次</h3>
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

    <!-- 跨模块同步：批次分发状态（数据分发模块回写） -->
    <section class="dispatch-sync" data-testid="dispatch-sync-panel">
      <header class="page-head">
        <div>
          <h3>批次分发状态</h3>
          <p class="page-desc">
            数据分发模块按「批次 × 通道 × 输出格式」去重后的回写结果；
            待续传通道可到「数据分发」页面执行通道续传，已完成通道不会被覆盖。
          </p>
        </div>
        <div class="page-actions">
          <button class="btn ghost" data-testid="dispatch-sync-refresh" type="button" @click="reloadDispatchStatus">刷新分发状态</button>
        </div>
      </header>
      <table class="data-table" data-testid="dispatch-sync-table">
        <thead>
          <tr>
            <th>批次</th><th>批次编号</th><th>分发状态</th><th>已完成通道</th>
            <th>重复跳过</th><th>待续传通道</th><th>未覆盖通道</th><th>有效产物</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="status in dispatchStatuses" :key="status.batch_id" :data-testid="`dispatch-sync-row-${status.batch_id}`">
            <td>#{{ status.batch_id }}</td>
            <td>{{ status.batch_code ?? '—' }}</td>
            <td><span class="sync-badge" :class="batchStatusClass(status.status)">{{ status.status }}</span></td>
            <td>{{ status.completed_channels }}</td>
            <td>{{ status.skipped_channels }}</td>
            <td>{{ status.failed_channels }}</td>
            <td>{{ status.pending_channels.length }}</td>
            <td>{{ status.file_count }}</td>
          </tr>
          <tr v-if="!dispatchStatuses.length">
            <td colspan="8" class="empty-state">暂无批次分发状态，可先到数据分发页面执行分发</td>
          </tr>
        </tbody>
      </table>
    </section>

    <footer class="page-foot">
      <span>共 {{ total }} 条采集仓储记录</span>
      <span v-if="errorMessage" class="error-text">{{ errorMessage }}</span>
    </footer>
  </section>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'

import { request } from '@/api/client'

type Row = Record<string, string | number | null>

type BatchDispatchStatus = {
  batch_id: number
  batch_code: string | null
  status: string
  completed_channels: number
  skipped_channels: number
  failed_channels: number
  pending_channels: unknown[]
  file_count: number
}

const ENDPOINT = '/api/result_store'
const PIPELINE = '/api/pipeline'
const columns = ["批次编号", "关联任务", "原始行数", "有效行数", "存储路径", "压缩格式", "校验摘要", "落地时间"]
const actions = ["触发校验", "标记通过", "标记失败"]
const statuses = ["接收中", "已落盘", "校验通过", "校验失败"]
const metricNames = ["批次总数", "校验通过批次", "校验失败批次"]

const rows = ref<Row[]>([])
const total = ref(0)
const errorMessage = ref('')
const filters = ref<Record<string, string>>({})
const filterFields = columns.slice(0, 3)
const showCreate = ref(false)
const creating = ref(false)
const createValues = ref<Record<string, string>>({})
const dispatchStatuses = ref<BatchDispatchStatus[]>([])
const stats = computed(() => [
  { label: metricNames[0], value: total.value },
  { label: metricNames[1], value: rows.value.filter((row) => row.status === statuses[1]).length },
  { label: metricNames[2], value: rows.value.filter((row) => row.status === statuses[statuses.length - 1]).length },
])

function batchStatusClass(status: string): string {
  if (status === '全部完成') return 'sync-done'
  if (status === '无可用通道') return 'sync-skip'
  return 'sync-pending'
}

async function reloadDispatchStatus() {
  try {
    const response = await request(`${PIPELINE}/dispatch/batch-status`)
    if (!response.ok) throw new Error('批次分发状态读取失败')
    dispatchStatuses.value = (await response.json()) as BatchDispatchStatus[]
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '批次分发状态读取失败'
  }
}

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
    link.download = 'result_store.csv'
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
      throw new Error('采集仓储动作未生效，请稍后重试')
    }
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '采集仓储操作失败'
  }
}

async function reload() {
  errorMessage.value = ''
  const query = new URLSearchParams(filters.value as Record<string, string>).toString()
  try {
    const response = await request(`${ENDPOINT}?${query}`)
    if (!response.ok) {
      throw new Error('结果批次列表读取失败')
    }
    const payload = await response.json()
    rows.value = payload.items ?? []
    total.value = payload.total ?? rows.value.length
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '采集仓储列表读取失败'
  }
}

onMounted(() => {
  void reload()
  void reloadDispatchStatus()
})
</script>

<style scoped>
.dispatch-sync {
  margin-top: 20px;
  background: #f8fafc;
  border: 1px solid var(--border);
  border-radius: 10px;
  padding: 14px 16px;
}
.sync-badge {
  display: inline-block;
  padding: 2px 8px;
  border-radius: 10px;
  font-size: 12px;
  white-space: nowrap;
}
.sync-done {
  background: #dcfce7;
  color: #15803d;
}
.sync-skip {
  background: #f1f5f9;
  color: #64748b;
}
.sync-pending {
  background: #fef3c7;
  color: #b45309;
}
</style>
