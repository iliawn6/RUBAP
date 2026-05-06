from typing import Any

from pydantic import BaseModel


class RealtimeEvent(BaseModel):
    event_id: str
    event_type: str
    timestamp: str
    user_id: str
    properties: dict[str, Any]


class ActiveSessionsResponse(BaseModel):
    active_sessions: int
    window_minutes: int


class SessionStateResponse(BaseModel):
    user_id: str
    events: list[RealtimeEvent]
    cart_items: list[dict[str, Any]]
    event_counts: dict[str, int]
    last_activity: str | None


class TrendingProduct(BaseModel):
    product_id: str
    product_name: str | None = None
    category: str | None = None
    views: int


class TrendingProductsResponse(BaseModel):
    window_minutes: int
    products: list[TrendingProduct]


class RecentUserResponse(BaseModel):
    user_id: str
    event_count: int
    events: list[RealtimeEvent]
    last_seen: str | None
