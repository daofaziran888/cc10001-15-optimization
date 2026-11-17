"""分发调度器：读取采集仓储批次、已有分发记录与通道配置，做多通道去重分发与失败续传。

数据流：result_store -> export_hub（通道配置） -> dispatch_logs / dispatch_files（联合存储）

核心口径：
- 按「结果批次 × 分发通道 × 输出格式」做批次和通道去重，重试调用不会重复传输；
- 同一批次、通道和输出格式只保留一个有效分发结果（dispatch_files 唯一）；
- 失败通道只能续传自身未完成的传输，续传不会覆盖任何已完成通道；
- 格式边界冲突支持两种策略：
  * channel_first ：按通道只保留首个格式，第二次分发记为重复跳过；
  * channel_format：按通道+格式分别保留文件，但同一格式仍只有一个有效结果。
"""
from __future__ import annotations

import base64
import csv
import hashlib
import io
import json
import time
import zipfile
from typing import Any

from app.store import store

RESULT_MODULE = "result_store"
CHANNEL_MODULE = "export_hub"
LOG_TABLE = "dispatch_logs"
FILE_TABLE = "dispatch_files"

# 分发日志三态：页面据此区分已完成、重复跳过和待续传通道
LOG_DONE = "已完成"
LOG_SKIPPED = "重复跳过"
LOG_FAILED = "传输失败"

# 批次分发状态
BATCH_ALL_DONE = "全部完成"
BATCH_PARTIAL = "部分完成"
BATCH_PENDING = "待分发"
BATCH_NO_CHANNEL = "无可用通道"

ALLOWED_FORMATS = ("json", "csv")
# 格式边界冲突处理策略
POLICY_CHANNEL_FIRST = "channel_first"      # 按通道只保留首个格式
POLICY_CHANNEL_FORMAT = "channel_format"    # 按通道+格式分别保留
POLICIES = (POLICY_CHANNEL_FIRST, POLICY_CHANNEL_FORMAT)

# 可承接分发任务的通道状态
ACTIVE_CHANNEL_STATUSES = ("已启用", "传输中")


class ExportDispatcher:
    def __init__(self) -> None:
        self._migrate_legacy_logs()

    # ------------------------------------------------------------------
    # 基础工具
    # ------------------------------------------------------------------
    def _now(self) -> str:
        return time.strftime("%Y-%m-%d %H:%M:%S")

    def _next_id(self, table: str) -> int:
        rows = store.joint_rows(table)
        return max((int(r.get("id", 0)) for r in rows), default=0) + 1

    def _migrate_legacy_logs(self) -> None:
        """把旧版按调用创建的分发日志补齐批次×通道×格式字段，纳入统一去重口径。"""
        changed = False
        for log in store.joint_rows(LOG_TABLE):
            if "channel_id" in log:
                continue
            entry_id = log.get("export_entry_id")
            channel = store.find(CHANNEL_MODULE, int(entry_id)) if entry_id else None
            fmt = str(log.get("channel_format") or "json").lower()
            log["channel_id"] = int(entry_id) if entry_id else 0
            log["channel_code"] = str(channel.get("通道编号", "")) if channel else f"LEGACY-{entry_id}"
            log["channel_format"] = fmt
            log["target"] = log.get("target") or f"s3://exports/{log.get('batch_id')}.{fmt}"
            log["status"] = LOG_DONE
            log["reason"] = "历史分发记录迁移"
            log["attempt"] = log.get("attempt", 1)
            log["max_retries"] = log.get("max_retries", 3)
            log["policy"] = log.get("policy", POLICY_CHANNEL_FIRST)
            log["simulate_failure"] = False
            log["updated_at"] = log.get("updated_at", log.get("dispatched_at", self._now()))
            log["file_id"] = None
            changed = True
        if changed:
            # 至少存在一条待迁移记录时，借助它的原地更新把整表落盘
            table = store.joint_rows(LOG_TABLE)
            if table:
                store.joint_update(LOG_TABLE, int(table[0]["id"]), {})

    def _batch_logs(self, batch_id: int) -> list[dict[str, Any]]:
        return [r for r in store.joint_rows(LOG_TABLE) if int(r.get("batch_id", 0)) == batch_id]

    def _find_log(
        self, batch_id: int, channel_id: int, channel_format: str,
        statuses: tuple[str, ...] | None = None,
    ) -> dict[str, Any] | None:
        for log in self._batch_logs(batch_id):
            if (
                int(log.get("channel_id", 0)) == channel_id
                and str(log.get("channel_format", "")).lower() == channel_format.lower()
                and (statuses is None or log.get("status") in statuses)
            ):
                return log
        return None

    def _available_channels(self) -> list[dict[str, Any]]:
        """读取通道配置：只有已启用/传输中的通道承接分发任务。"""
        return [
            row for row in store.rows(CHANNEL_MODULE)
            if row.get("status") in ACTIVE_CHANNEL_STATUSES
        ]

    def _effective_format(self, channel: dict[str, Any], requested: str) -> str:
        """通道自身声明的输出格式优先；占位或非受支持格式时回落到请求格式。"""
        declared = str(channel.get("输出格式", "")).strip().lower()
        if declared in ALLOWED_FORMATS:
            return declared
        return requested if requested in ALLOWED_FORMATS else ALLOWED_FORMATS[0]

    def _retry_limit(self, channel: dict[str, Any]) -> int:
        """通道重试上限；种子数据里可能是占位文本，解析失败时回落到默认 3 次。"""
        raw = str(channel.get("重试上限", "")).strip()
        try:
            return max(1, int(raw))
        except ValueError:
            return 3

    def _target_address(self, batch_id: int, channel: dict[str, Any], fmt: str) -> str:
        """分发记录保留目标地址：通道配置了真实地址就沿用，否则给默认对象存储路径。"""
        configured = str(channel.get("目标地址", "")).strip()
        if "://" in configured:
            base = configured.rstrip("/")
            return f"{base}/batch-{batch_id}.{fmt}"
        return f"s3://exports/batch-{batch_id}/channel-{channel['id']}.{fmt}"

    # ------------------------------------------------------------------
    # 产物渲染与单文件落盘
    # ------------------------------------------------------------------
    def _render_artifact(self, batch: dict[str, Any], fmt: str) -> tuple[str, bytes]:
        """按格式渲染该批次的完整产物；同一批次+格式渲染结果稳定，便于核对。"""
        try:
            row_count = int(str(batch.get("有效行数", "0")).replace(",", "") or 0)
        except ValueError:
            row_count = 0
        row_count = max(row_count, 1)
        payload = {
            "batch_id": batch["id"],
            "批次编号": batch.get("批次编号", ""),
            "关联任务": batch.get("关联任务", ""),
            "有效行数": row_count,
            "校验摘要": batch.get("校验摘要", ""),
            "rows": [
                {"row_no": i, "batch_id": batch["id"], "批次编号": batch.get("批次编号", "")}
                for i in range(1, row_count + 1)
            ],
        }
        if fmt == "json":
            content = json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")
            return f"batch-{batch['id']}.json", content

        buffer = io.StringIO()
        buffer.write("\ufeff")
        writer = csv.writer(buffer, lineterminator="\n")
        writer.writerow(["row_no", "batch_id", "批次编号", "关联任务", "校验摘要"])
        for i in range(1, row_count + 1):
            writer.writerow([i, batch["id"], batch.get("批次编号", ""), batch.get("关联任务", ""), batch.get("校验摘要", "")])
        content = buffer.getvalue().encode("utf-8")
        return f"batch-{batch['id']}.csv", content

    def _write_file_once(
        self, batch_id: int, channel: dict[str, Any], fmt: str,
        target: str, batch: dict[str, Any], log_id: int,
    ) -> dict[str, Any]:
        """同一批次×通道×格式只写入一份有效文件；重复传输直接复用既有文件。"""
        existing = store.joint_find_by(
            FILE_TABLE, {"batch_id": batch_id, "channel_id": channel["id"], "channel_format": fmt})
        if existing is not None:
            return existing

        filename, content = self._render_artifact(batch, fmt)
        record = {
            "id": self._next_id(FILE_TABLE),
            "batch_id": batch_id,
            "channel_id": channel["id"],
            "channel_code": str(channel.get("通道编号", "")),
            "channel_format": fmt,
            "target": target,                       # 保留目标地址
            "filename": filename,
            "size_bytes": len(content),
            "checksum": hashlib.sha256(content).hexdigest()[:16],
            "content_b64": base64.b64encode(content).decode("ascii"),
            "produced_by_log": log_id,
            "created_at": self._now(),
        }
        store.joint_append(FILE_TABLE, record)
        return record

    # ------------------------------------------------------------------
    # 单通道分发（去重 + 续传边界都在这一层判定）
    # ------------------------------------------------------------------
    def _dispatch_one(
        self, batch: dict[str, Any], channel: dict[str, Any], requested_format: str,
        policy: str, simulate_failure: bool,
    ) -> dict[str, Any]:
        batch_id = int(batch["id"])
        channel_id = int(channel["id"])
        fmt = self._effective_format(channel, requested_format)
        target = self._target_address(batch_id, channel, fmt)
        now = self._now()

        # 1) 续传边界：该通道同格式存在未完成（传输失败）记录，只能续传，不能新开传输
        unfinished = self._find_log(batch_id, channel_id, fmt, (LOG_FAILED,))
        if unfinished is not None:
            if simulate_failure:
                attempt = int(unfinished.get("attempt", 1)) + 1
                store.joint_update(LOG_TABLE, int(unfinished["id"]), {
                    "attempt": attempt, "updated_at": now,
                    "reason": f"续传再次失败（第{attempt}次）", "simulate_failure": True,
                })
                return {"outcome": LOG_FAILED, "log": unfinished, "file": None}
            file_record = self._write_file_once(batch_id, channel, fmt, target, batch, int(unfinished["id"]))
            store.joint_update(LOG_TABLE, int(unfinished["id"]), {
                "status": LOG_DONE, "reason": "续传完成", "attempt": int(unfinished.get("attempt", 1)) + 1,
                "updated_at": now, "target": target, "file_id": file_record["id"], "simulate_failure": False,
            })
            return {"outcome": LOG_DONE, "log": unfinished, "file": file_record, "resumed": True}

        # 2) 同通道同格式已有有效结果：批次/通道重复，直接跳过，绝不重复传输
        done_same = self._find_log(batch_id, channel_id, fmt, (LOG_DONE,))
        if done_same is not None:
            skip = self._find_log(batch_id, channel_id, fmt, (LOG_SKIPPED,))
            if skip is None:
                skip = self._new_skip_log(batch, channel, fmt, target, policy,
                                          "同批次同通道同格式已有有效分发结果，重复跳过")
            return {"outcome": LOG_SKIPPED, "log": skip, "file": None}

        # 3) 格式边界冲突：该通道已按别的格式完成过
        done_other = next(
            (r for r in self._batch_logs(batch_id)
             if int(r.get("channel_id", 0)) == channel_id
             and r.get("status") == LOG_DONE
             and str(r.get("channel_format", "")).lower() != fmt),
            None,
        )
        if done_other is not None:
            if policy == POLICY_CHANNEL_FIRST:
                # 按通道只保留首个格式：第二次分发跳过，不产生第二个文件
                skip = self._find_log(batch_id, channel_id, fmt, (LOG_SKIPPED,))
                if skip is None:
                    skip = self._new_skip_log(
                        batch, channel, fmt, target, policy,
                        f"通道已按{done_other.get('channel_format')}格式分发，通道级首格式策略下跳过{fmt}分发",
                    )
                return {"outcome": LOG_SKIPPED, "log": skip, "file": None}
            # channel_format：按通道+格式分别保留，落到下面的正常传输流程

        # 4) 全新传输
        log = {
            "id": self._next_id(LOG_TABLE),
            "batch_id": batch_id,
            "channel_id": channel_id,
            "channel_code": str(channel.get("通道编号", "")),
            "channel_format": fmt,
            "target": target,
            "export_entry_id": channel_id,
            "status": LOG_FAILED if simulate_failure else LOG_DONE,
            "reason": "模拟传输失败，等待通道续传" if simulate_failure else "首次分发完成",
            "attempt": 1,
            "max_retries": self._retry_limit(channel),
            "simulate_failure": bool(simulate_failure),
            "policy": policy,
            "dispatched_at": now,
            "updated_at": now,
            "file_id": None,
        }
        store.joint_append(LOG_TABLE, log)
        if simulate_failure:
            return {"outcome": LOG_FAILED, "log": log, "file": None}

        file_record = self._write_file_once(batch_id, channel, fmt, target, batch, int(log["id"]))
        store.joint_update(LOG_TABLE, int(log["id"]), {"file_id": file_record["id"]})
        log["file_id"] = file_record["id"]
        return {"outcome": LOG_DONE, "log": log, "file": file_record}

    def _new_skip_log(
        self, batch: dict[str, Any], channel: dict[str, Any], fmt: str,
        target: str, policy: str, reason: str,
    ) -> dict[str, Any]:
        log = {
            "id": self._next_id(LOG_TABLE),
            "batch_id": int(batch["id"]),
            "channel_id": int(channel["id"]),
            "channel_code": str(channel.get("通道编号", "")),
            "channel_format": fmt,
            "target": target,
            "export_entry_id": int(channel["id"]),
            "status": LOG_SKIPPED,
            "reason": reason,
            "attempt": 0,
            "max_retries": self._retry_limit(channel),
            "simulate_failure": False,
            "policy": policy,
            "dispatched_at": self._now(),
            "updated_at": self._now(),
            "file_id": None,
        }
        store.joint_append(LOG_TABLE, log)
        return log

    # ------------------------------------------------------------------
    # 对外动作
    # ------------------------------------------------------------------
    def dispatch_batch(
        self, batch_id: int, channel_format: str = "json",
        *, simulate_failure: bool = False, policy: str = POLICY_CHANNEL_FIRST,
    ) -> dict[str, Any]:
        """对一个结果批次执行多通道分发：读取批次、通道配置与已有记录后逐通道去重。"""
        batch = store.find(RESULT_MODULE, batch_id)
        if batch is None:
            return {"ok": False, "error": f"结果批次 {batch_id} 不存在"}
        if batch.get("status") not in ("校验通过", "已分发"):
            return {"ok": False, "error": f"批次 {batch_id} 未通过校验，禁止分发"}
        requested = channel_format.lower()
        if requested not in ALLOWED_FORMATS:
            return {"ok": False, "error": f"不支持的输出格式 {channel_format}，仅支持 {ALLOWED_FORMATS}"}
        if policy not in POLICIES:
            return {"ok": False, "error": f"未知格式冲突策略 {policy}"}

        channels = self._available_channels()
        if not channels:
            return {"ok": False, "error": "没有已启用的分发通道，请先在数据分发页面启用通道"}

        outcomes: list[dict[str, Any]] = []
        for channel in channels:
            outcomes.append(self._dispatch_one(batch, channel, requested, policy, simulate_failure))

        completed = [o for o in outcomes if o["outcome"] == LOG_DONE]
        skipped = [o for o in outcomes if o["outcome"] == LOG_SKIPPED]
        failed = [o for o in outcomes if o["outcome"] == LOG_FAILED]

        # 全部通道都拿到有效结果后，批次才标记为已分发
        self._refresh_batch_status(batch)

        summary = self.batch_status(batch_id)
        return {
            "ok": True,
            "batch_id": batch_id,
            "policy": policy,
            "channels_total": len(channels),
            "completed": len(completed),
            "skipped": len(skipped),
            "failed": len(failed),
            "logs": [o["log"] for o in outcomes],
            "batch_status": summary,
        }

    def dispatch_all_ready(self, *, simulate_failure: bool = False,
                           policy: str = POLICY_CHANNEL_FIRST) -> dict[str, Any]:
        """对所有校验通过/已分发但未全部完成的批次自动续传分发。"""
        batches = [b for b in store.rows(RESULT_MODULE) if b.get("status") in ("校验通过", "已分发")]
        results = [
            self.dispatch_batch(int(b["id"]), simulate_failure=simulate_failure, policy=policy)
            for b in batches
        ]
        succeeded = sum(1 for r in results if r.get("ok"))
        return {
            "ok": True, "dispatched": len(results), "succeeded": succeeded,
            "results": results,
        }

    def resume_channel(self, log_id: int, *, simulate_failure: bool = False) -> dict[str, Any]:
        """失败通道独立续传入口：只续传该条未完成记录，不触碰任何已完成通道。"""
        log = store.joint_find(LOG_TABLE, log_id)
        if log is None:
            return {"ok": False, "error": f"分发日志 {log_id} 不存在"}
        if log.get("status") != LOG_FAILED:
            return {"ok": False, "error": f"日志 {log_id} 当前状态为{log.get('status')}，无需续传"}

        batch = store.find(RESULT_MODULE, int(log["batch_id"]))
        if batch is None:
            return {"ok": False, "error": f"结果批次 {log['batch_id']} 不存在，无法续传"}
        channel = store.find(CHANNEL_MODULE, int(log["channel_id"]))
        if channel is None:
            return {"ok": False, "error": f"分发通道 {log['channel_id']} 已停用或归档，无法续传"}

        fmt = str(log.get("channel_format", "json")).lower()
        target = self._target_address(int(batch["id"]), channel, fmt)
        now = self._now()
        if simulate_failure:
            attempt = int(log.get("attempt", 1)) + 1
            store.joint_update(LOG_TABLE, log_id, {
                "attempt": attempt, "updated_at": now, "simulate_failure": True,
                "reason": f"续传再次失败（第{attempt}次）",
            })
            return {"ok": True, "resumed": False, "log": store.joint_find(LOG_TABLE, log_id),
                    "batch_status": self.batch_status(int(batch["id"]))}

        # 续传边界：已完成通道的文件永不覆盖，这里只会写该通道自己缺失的那一份
        file_record = self._write_file_once(
            int(batch["id"]), channel, fmt, target, batch, log_id)
        store.joint_update(LOG_TABLE, log_id, {
            "status": LOG_DONE, "reason": "失败通道续传完成",
            "attempt": int(log.get("attempt", 1)) + 1, "updated_at": now,
            "target": target, "file_id": file_record["id"], "simulate_failure": False,
        })
        self._refresh_batch_status(batch)
        return {
            "ok": True, "resumed": True,
            "log": store.joint_find(LOG_TABLE, log_id),
            "file": file_record,
            "batch_status": self.batch_status(int(batch["id"])),
        }

    def _refresh_batch_status(self, batch: dict[str, Any]) -> None:
        """全部活跃通道都拿到有效结果（无失败、无未覆盖通道）后批次才标记已分发。"""
        logs = self._batch_logs(int(batch["id"]))
        has_done = any(r.get("status") == LOG_DONE for r in logs)
        has_failed = any(r.get("status") == LOG_FAILED for r in logs)
        covered = {int(r.get("channel_id", 0)) for r in logs}
        uncovered = any(int(c["id"]) not in covered for c in self._available_channels())
        if has_done and not has_failed and not uncovered:
            batch["status"] = "已分发"
            batch["pending"] = False

    # ------------------------------------------------------------------
    # 查询：去重后的分发记录与批次分发状态
    # ------------------------------------------------------------------
    def list_logs(self, batch_id: int | None = None) -> list[dict[str, Any]]:
        rows = store.joint_rows(LOG_TABLE)
        if batch_id is not None:
            rows = [r for r in rows if int(r.get("batch_id", 0)) == batch_id]
        return list(rows)

    def get_log(self, log_id: int) -> dict[str, Any] | None:
        return store.joint_find(LOG_TABLE, log_id)

    def list_files(self, batch_id: int | None = None) -> list[dict[str, Any]]:
        rows = store.joint_rows(FILE_TABLE)
        if batch_id is not None:
            rows = [r for r in rows if int(r.get("batch_id", 0)) == batch_id]
        return list(rows)

    def batch_status(self, batch_id: int) -> dict[str, Any]:
        """汇总一个批次的批次分发状态与逐通道去重结果。"""
        batch = store.find(RESULT_MODULE, batch_id)
        logs = self._batch_logs(batch_id)
        done = [r for r in logs if r.get("status") == LOG_DONE]
        skipped = [r for r in logs if r.get("status") == LOG_SKIPPED]
        failed = [r for r in logs if r.get("status") == LOG_FAILED]

        active_channels = self._available_channels()
        covered_channel_ids = {int(r.get("channel_id", 0)) for r in logs}
        pending_channels = [
            {"channel_id": int(c["id"]), "channel_code": c.get("通道编号", "")}
            for c in active_channels if int(c["id"]) not in covered_channel_ids
        ]

        if not active_channels:
            status = BATCH_NO_CHANNEL
        elif failed or pending_channels:
            status = BATCH_PARTIAL if done else BATCH_PENDING
        else:
            status = BATCH_ALL_DONE

        files = self.list_files(batch_id)
        return {
            "batch_id": batch_id,
            "batch_code": batch.get("批次编号") if batch else None,
            "status": status,
            "channels_total": len(active_channels),
            "completed_channels": len({int(r["channel_id"]) for r in done}),
            "skipped_channels": len(skipped),
            "failed_channels": len({int(r["channel_id"]) for r in failed}),
            "pending_channels": pending_channels,
            "file_count": len(files),
            "logs": logs,
            "files": [{k: v for k, v in f.items() if k != "content_b64"} for f in files],
        }

    def list_batch_status(self) -> list[dict[str, Any]]:
        """跨模块页面用：给每个结果批次一份分发状态快照。"""
        result = []
        for batch in store.rows(RESULT_MODULE):
            result.append(self.batch_status(int(batch["id"])))
        return result

    # ------------------------------------------------------------------
    # 打包、下载与导出
    # ------------------------------------------------------------------
    def _file_content(self, file_record: dict[str, Any]) -> bytes:
        stored = file_record.get("content_b64")
        if stored:
            return base64.b64decode(stored)
        batch = store.find(RESULT_MODULE, int(file_record["batch_id"]))
        _, content = self._render_artifact(batch, str(file_record["channel_format"]))
        return content

    def package_batch(self, batch_id: int) -> dict[str, Any]:
        """把批次下每个通道的有效产物（每通道×格式只一份）打成一个 zip 包。"""
        batch = store.find(RESULT_MODULE, batch_id)
        if batch is None:
            return {"ok": False, "error": f"结果批次 {batch_id} 不存在"}
        files = self.list_files(batch_id)
        if not files:
            return {"ok": False, "error": f"批次 {batch_id} 还没有已完成的分发产物，无法打包"}

        buffer = io.BytesIO()
        with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as archive:
            used: set[str] = set()
            for record in files:
                content = self._file_content(record)
                # zip 内文件名带上通道与格式，避免不同通道同名互相覆盖
                name = f"channel-{record['channel_id']}-{record['channel_code']}/{record['filename']}"
                candidate, suffix = name, 1
                while candidate in used:
                    candidate = f"{name[:-4]}-{suffix}{name[-4:]}"
                    suffix += 1
                used.add(candidate)
                archive.writestr(candidate, content)
        package = buffer.getvalue()
        return {
            "ok": True,
            "filename": f"batch-{batch_id}-dispatch.zip",
            "media_type": "application/zip",
            "content": package,
            "file_count": len(files),
        }

    def download_file(self, file_id: int) -> dict[str, Any]:
        """下载单个通道的单份有效产物。"""
        record = store.joint_find(FILE_TABLE, file_id)
        if record is None:
            return {"ok": False, "error": f"分发产物文件 {file_id} 不存在"}
        content = self._file_content(record)
        media = "application/json" if record["channel_format"] == "json" else "text/csv; charset=utf-8"
        return {
            "ok": True, "filename": record["filename"], "media_type": media,
            "content": content, "record": {k: v for k, v in record.items() if k != "content_b64"},
        }

    def export_logs_csv(self, batch_id: int | None = None) -> tuple[str, bytes]:
        """导出去重后的分发日志为 UTF-8 CSV，便于线下核对已完成/跳过/失败通道。"""
        logs = self.list_logs(batch_id)
        header = ["日志编号", "批次编号", "通道编号", "输出格式", "目标地址",
                  "分发状态", "跳过/失败原因", "尝试次数", "重试上限", "首次分发时间", "最近更新时间"]
        buffer = io.StringIO()
        buffer.write("\ufeff")
        writer = csv.writer(buffer, lineterminator="\n")
        writer.writerow(header)
        for log in logs:
            row = [
                log.get("id", ""), log.get("batch_id", ""), log.get("channel_code", ""),
                log.get("channel_format", ""), log.get("target", ""), log.get("status", ""),
                log.get("reason", ""), log.get("attempt", ""), log.get("max_retries", ""),
                log.get("dispatched_at", ""), log.get("updated_at", ""),
            ]
            writer.writerow([
                ("'" + str(v)) if str(v).startswith(("=", "+", "-", "@")) else v for v in row
            ])
        name = f"dispatch_logs-batch-{batch_id}.csv" if batch_id is not None else "dispatch_logs.csv"
        return name, buffer.getvalue().encode("utf-8")


export_dispatcher = ExportDispatcher()
