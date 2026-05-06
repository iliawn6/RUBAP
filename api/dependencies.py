from contextlib import asynccontextmanager

import clickhouse_connect
from fastapi import FastAPI
from motor.motor_asyncio import AsyncIOMotorClient

from api.config import get_settings


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings = get_settings()

    mongo_client = AsyncIOMotorClient(settings.mongo_uri)
    mongo_collection = mongo_client[settings.mongo_db][settings.mongo_collection]

    clickhouse_client = clickhouse_connect.get_client(
        host=settings.clickhouse_host,
        port=settings.clickhouse_port,
        username=settings.clickhouse_user,
        password=settings.clickhouse_password,
        database=settings.clickhouse_database,
    )

    app.state.mongo_client = mongo_client
    app.state.mongo_collection = mongo_collection
    app.state.clickhouse_client = clickhouse_client

    try:
        yield
    finally:
        mongo_client.close()
        clickhouse_client.close()
