from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from api.dependencies import lifespan
from api.routers.analytics import router as analytics_router
from api.routers.realtime import router as realtime_router
from api.routers.system import router as system_router


app = FastAPI(
    title="RUBAP Serving API",
    description="System, realtime, and analytical APIs for user behavior data.",
    version="0.1.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(system_router)
app.include_router(realtime_router)
app.include_router(analytics_router)


@app.get("/", tags=["system"])
async def root() -> dict[str, str]:
    return {"service": "rubap-api", "status": "ok"}
