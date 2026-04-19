from collections import Counter
from datetime import datetime, timedelta, timezone
from typing import Any

from fastapi import APIRouter, Request

from api.schemas.realtime import (
    ActiveSessionsResponse,
    RecentUserResponse,
    RealtimeEvent,
    SessionStateResponse,
    TrendingProduct,
    TrendingProductsResponse,
)


router = APIRouter(prefix="/realtime", tags=["realtime"])

TRENDING_EVENT_TYPES = ["product_view", "add_to_cart", "purchase"]


def _serialize_event(document: dict[str, Any]) -> RealtimeEvent:
    return RealtimeEvent(
        event_id=str(document.get("event_id", "")),
        event_type=str(document.get("event_type", "")),
        timestamp=str(document.get("timestamp", "")),
        user_id=str(document.get("user_id", "")),
        properties=document.get("properties", {}) or {},
    )


@router.get("/active-sessions", response_model=ActiveSessionsResponse)
async def active_sessions(request: Request) -> ActiveSessionsResponse:
    window_minutes = 30
    window_start = datetime.now(tz=timezone.utc) - timedelta(minutes=window_minutes)
    collection = request.app.state.mongo_collection

    session_ids = await collection.distinct("user_id", {"_ingested_at": {"$gte": window_start}})

    return ActiveSessionsResponse(active_sessions=len(session_ids), window_minutes=window_minutes)


@router.get("/session/{session_id}", response_model=SessionStateResponse)
async def realtime_session(request: Request, session_id: str) -> SessionStateResponse:
    collection = request.app.state.mongo_collection
    documents = await collection.find({"user_id": session_id}).sort("timestamp", -1).limit(50).to_list(length=50)

    events = [_serialize_event(document) for document in documents]
    event_counts = dict(Counter(event.event_type for event in events))
    last_activity = events[0].timestamp if events else None

    cart_items: list[dict[str, Any]] = []
    for event in events:
        if event.event_type != "add_to_cart":
            continue
        product_id = event.properties.get("product_id")
        if not product_id:
            continue
        cart_items.append(
            {
                "product_id": product_id,
                "product_name": event.properties.get("product_name"),
                "category": event.properties.get("category"),
                "quantity": event.properties.get("quantity"),
                "price": event.properties.get("price"),
                "total_value": event.properties.get("total_value"),
                "timestamp": event.timestamp,
            }
        )

    return SessionStateResponse(
        user_id=session_id,
        events=events,
        cart_items=cart_items[:10],
        event_counts=event_counts,
        last_activity=last_activity,
    )


@router.get("/trending-products", response_model=TrendingProductsResponse)
async def trending_products(request: Request) -> TrendingProductsResponse:
    window_minutes = 5
    window_start = datetime.now(tz=timezone.utc) - timedelta(minutes=window_minutes)
    collection = request.app.state.mongo_collection

    pipeline = [
        {
            "$match": {
                "_ingested_at": {"$gte": window_start},
                "event_type": {"$in": TRENDING_EVENT_TYPES},
                "properties.product_id": {"$exists": True},
            }
        },
        {
            "$group": {
                "_id": "$properties.product_id",
                "product_name": {"$first": "$properties.product_name"},
                "category": {"$first": "$properties.category"},
                "views": {"$sum": 1},
            }
        },
        {"$sort": {"views": -1}},
        {"$limit": 10},
    ]
    documents = await collection.aggregate(pipeline).to_list(length=10)

    products = [
        TrendingProduct(
            product_id=document.get("_id", ""),
            product_name=document.get("product_name"),
            category=document.get("category"),
            views=int(document.get("views", 0)),
        )
        for document in documents
    ]

    return TrendingProductsResponse(window_minutes=window_minutes, products=products)


@router.get("/recent-user/{user_id}", response_model=RecentUserResponse)
async def recent_user(request: Request, user_id: str) -> RecentUserResponse:
    collection = request.app.state.mongo_collection
    documents = await collection.find({"user_id": user_id}).sort("_ingested_at", -1).limit(20).to_list(length=20)

    events = [_serialize_event(document) for document in documents]
    last_seen = events[0].timestamp if events else None

    return RecentUserResponse(
        user_id=user_id,
        event_count=len(events),
        events=events,
        last_seen=last_seen,
    )
