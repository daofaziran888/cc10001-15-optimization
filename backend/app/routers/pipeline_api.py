"""跨模块数据流引擎 API：流水线执行、调度触发、质量审计、增量同步、数据分发、代理路由、反爬轮换。"""
from __future__ import annotations

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import Response
from pydantic import BaseModel

from app.services.pipeline_executor import pipeline_executor
from app.services.scheduler_engine import scheduler_engine
from app.services.quality_auditor import quality_auditor
from app.services.delta_tracker import delta_tracker
from app.services.export_dispatcher import (
    POLICY_CHANNEL_FIRST,
    export_dispatcher,
)
from app.services.proxy_router import proxy_router
from app.services.anti_block_engine import anti_block_engine

router = APIRouter(prefix="/api/pipeline", tags=["数据流引擎"])


class ExecuteSegmentPayload(BaseModel):
    segment_id: int


class TriggerPlanPayload(BaseModel):
    plan_id: int
    simulate_failure: bool = False


class AuditBatchPayload(BaseModel):
    batch_id: int


class CaptureSyncPayload(BaseModel):
    sync_id: int


class DispatchBatchPayload(BaseModel):
    batch_id: int
    channel_format: str = "json"
    simulate_failure: bool = False
    policy: str = POLICY_CHANNEL_FIRST


class ResumeChannelPayload(BaseModel):
    log_id: int
    simulate_failure: bool = False


class RouteTaskPayload(BaseModel):
    task_id: int


class RotateStrategyPayload(BaseModel):
    strategy_id: int


# ---------- 流水线执行 ----------

@router.post("/pipeline/execute-segment")
def execute_segment(payload: ExecuteSegmentPayload) -> dict:
    res = pipeline_executor.execute_segment(payload.segment_id)
    if not res.get("ok"):
        raise HTTPException(status_code=400, detail=res.get("error"))
    return res


@router.post("/pipeline/execute-dag")
def execute_dag() -> dict:
    return pipeline_executor.execute_dag()


@router.get("/pipeline/states")
def list_pipeline_states() -> list:
    return pipeline_executor.list_states()


@router.get("/pipeline/states/{state_id}")
def get_pipeline_state(state_id: int) -> dict:
    state = pipeline_executor.get_state(state_id)
    if state is None:
        raise HTTPException(status_code=404, detail=f"状态 {state_id} 不存在")
    return state


# ---------- 调度引擎 ----------

@router.post("/scheduler/trigger")
def trigger_plan(payload: TriggerPlanPayload) -> dict:
    res = scheduler_engine.trigger_plan(payload.plan_id, simulate_failure=payload.simulate_failure)
    if not res.get("ok"):
        raise HTTPException(status_code=400, detail=res.get("error"))
    return res


@router.post("/scheduler/tick-all")
def tick_all() -> dict:
    return scheduler_engine.tick_all()


@router.get("/scheduler/triggers")
def list_triggers() -> list:
    return scheduler_engine.list_triggers()


@router.get("/scheduler/triggers/{trigger_id}")
def get_trigger(trigger_id: int) -> dict:
    tg = scheduler_engine.get_trigger(trigger_id)
    if tg is None:
        raise HTTPException(status_code=404, detail=f"触发记录 {trigger_id} 不存在")
    return tg


# ---------- 质量审计 ----------

@router.post("/audit/batch")
def audit_batch(payload: AuditBatchPayload) -> dict:
    res = quality_auditor.audit_batch(payload.batch_id)
    if not res.get("ok"):
        raise HTTPException(status_code=400, detail=res.get("error"))
    return res


@router.post("/audit/all")
def audit_all() -> dict:
    return quality_auditor.audit_all()


@router.get("/audit/records")
def list_audit_records() -> list:
    return quality_auditor.list_records()


@router.get("/audit/records/{record_id}")
def get_audit_record(record_id: int) -> dict:
    rec = quality_auditor.get_record(record_id)
    if rec is None:
        raise HTTPException(status_code=404, detail=f"审计记录 {record_id} 不存在")
    return rec


# ---------- 增量同步 ----------

@router.post("/sync/capture")
def capture_increment(payload: CaptureSyncPayload) -> dict:
    res = delta_tracker.capture_increment(payload.sync_id)
    if not res.get("ok"):
        raise HTTPException(status_code=400, detail=res.get("error"))
    return res


@router.post("/sync/all")
def sync_all() -> dict:
    return delta_tracker.sync_all()


@router.get("/sync/positions")
def list_sync_positions() -> list:
    return delta_tracker.list_positions()


@router.get("/sync/positions/{position_id}")
def get_sync_position(position_id: int) -> dict:
    pos = delta_tracker.get_position(position_id)
    if pos is None:
        raise HTTPException(status_code=404, detail=f"同步位置 {position_id} 不存在")
    return pos


# ---------- 数据分发 ----------

@router.post("/dispatch/batch")
def dispatch_batch(payload: DispatchBatchPayload) -> dict:
    res = export_dispatcher.dispatch_batch(
        payload.batch_id, payload.channel_format,
        simulate_failure=payload.simulate_failure, policy=payload.policy)
    if not res.get("ok"):
        raise HTTPException(status_code=400, detail=res.get("error"))
    return res


@router.post("/dispatch/all-ready")
def dispatch_all_ready(payload: DispatchBatchPayload | None = None) -> dict:
    if payload is None:
        return export_dispatcher.dispatch_all_ready()
    return export_dispatcher.dispatch_all_ready(
        simulate_failure=payload.simulate_failure, policy=payload.policy)


@router.post("/dispatch/resume")
def resume_channel(payload: ResumeChannelPayload) -> dict:
    """失败通道独立续传：只续传该通道未完成的传输，不覆盖已完成通道。"""
    res = export_dispatcher.resume_channel(payload.log_id, simulate_failure=payload.simulate_failure)
    if not res.get("ok"):
        raise HTTPException(status_code=400, detail=res.get("error"))
    return res


@router.get("/dispatch/logs")
def list_dispatch_logs(batch_id: int | None = Query(default=None)) -> list:
    """去重后的分发记录；可按批次过滤，页面据此区分已完成/重复跳过/待续传。"""
    return export_dispatcher.list_logs(batch_id)


@router.get("/dispatch/logs/{log_id}")
def get_dispatch_log(log_id: int) -> dict:
    log = export_dispatcher.get_log(log_id)
    if log is None:
        raise HTTPException(status_code=404, detail=f"分发日志 {log_id} 不存在")
    return log


@router.get("/dispatch/batch-status")
def list_batch_status(batch_id: int | None = Query(default=None)) -> list | dict:
    """批次分发状态：每个结果批次的完成、跳过、失败与待续传通道清单。"""
    if batch_id is not None:
        return export_dispatcher.batch_status(batch_id)
    return export_dispatcher.list_batch_status()


@router.get("/dispatch/files")
def list_dispatch_files(batch_id: int | None = Query(default=None)) -> list:
    """每个批次×通道×格式只保留一份有效产物的文件清单。"""
    return export_dispatcher.list_files(batch_id)


@router.get("/dispatch/files/{file_id}/download")
def download_dispatch_file(file_id: int) -> Response:
    """下载单个通道的单份有效产物。"""
    res = export_dispatcher.download_file(file_id)
    if not res.get("ok"):
        raise HTTPException(status_code=404, detail=res.get("error"))
    return Response(
        content=res["content"], media_type=res["media_type"],
        headers={"Content-Disposition": f'attachment; filename="{res["filename"]}"'})


@router.get("/dispatch/package/{batch_id}")
def package_dispatch(batch_id: int) -> Response:
    """打包下载批次下每个通道的完整产物（每通道×格式一份文件）。"""
    res = export_dispatcher.package_batch(batch_id)
    if not res.get("ok"):
        raise HTTPException(status_code=400, detail=res.get("error"))
    return Response(
        content=res["content"], media_type=res["media_type"],
        headers={"Content-Disposition": f'attachment; filename="{res["filename"]}"'})


@router.get("/dispatch/export-logs")
def export_dispatch_logs(batch_id: int | None = Query(default=None)) -> Response:
    """导出去重后分发日志的 UTF-8 CSV，便于线下核对每个通道的产物。"""
    filename, content = export_dispatcher.export_logs_csv(batch_id)
    return Response(
        content=content, media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'})


# ---------- 代理路由 ----------

@router.post("/proxy/health-check")
def proxy_health_check() -> dict:
    return proxy_router.health_check_all()


@router.post("/proxy/route")
def route_for_task(payload: RouteTaskPayload) -> dict:
    res = proxy_router.route_for_task(payload.task_id)
    if not res.get("ok"):
        raise HTTPException(status_code=400, detail=res.get("error"))
    return res


@router.get("/proxy/assignments")
def list_proxy_assignments() -> list:
    return proxy_router.list_assignments()


@router.get("/proxy/assignments/{assign_id}")
def get_proxy_assignment(assign_id: int) -> dict:
    a = proxy_router.get_assignment(assign_id)
    if a is None:
        raise HTTPException(status_code=404, detail=f"分配记录 {assign_id} 不存在")
    return a


# ---------- 反爬引擎 ----------

@router.post("/anti-block/evaluate")
def evaluate_and_rotate(payload: RotateStrategyPayload) -> dict:
    res = anti_block_engine.evaluate_and_rotate(payload.strategy_id)
    if not res.get("ok"):
        raise HTTPException(status_code=400, detail=res.get("error"))
    return res


@router.post("/anti-block/rotate-all")
def rotate_all() -> dict:
    return anti_block_engine.rotate_all()


@router.get("/anti-block/rotations")
def list_cookie_rotations() -> list:
    return anti_block_engine.list_rotations()


@router.get("/anti-block/rotations/{rotation_id}")
def get_cookie_rotation(rotation_id: int) -> dict:
    r = anti_block_engine.get_rotation(rotation_id)
    if r is None:
        raise HTTPException(status_code=404, detail=f"轮换记录 {rotation_id} 不存在")
    return r


# ---------- 跨模块综合视图 ----------

@router.get("/joint/all")
def joint_all() -> dict:
    """返回所有联合存储表的当前快照。"""
    return store_all()


def store_all() -> dict:
    from app.store import store
    return store.joint_all()