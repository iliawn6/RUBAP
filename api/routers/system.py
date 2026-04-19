import asyncio

from fastapi import APIRouter, Request


router = APIRouter(prefix="/health", tags=["system"])


@router.get("")
async def health() -> dict[str, str]:
    return {"status": "ok"}


@router.get("/dependencies")
async def health_dependencies(request: Request) -> dict[str, str]:
    mongodb_status = "ok"
    clickhouse_status = "ok"

    try:
        await request.app.state.mongo_client.admin.command("ping")
    except Exception:  # noqa: BLE001
        mongodb_status = "error"

    try:
        await asyncio.to_thread(request.app.state.clickhouse_client.command, "SELECT 1")
    except Exception:  # noqa: BLE001
        clickhouse_status = "error"

    return {"mongodb": mongodb_status, "clickhouse": clickhouse_status}
