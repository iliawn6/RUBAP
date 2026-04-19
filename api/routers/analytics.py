import asyncio
from datetime import datetime

from fastapi import APIRouter, Query, Request

from api.schemas.analytics import (
    ConversionFunnelResponse,
    EventsTimeseriesPoint,
    EventsTimeseriesResponse,
    TopProductItem,
    TopProductsResponse,
    TopUserItem,
    TopUsersResponse,
    CategoryViewsItem,
    ViewsByCategoryResponse,
)


router = APIRouter(prefix="/analytics", tags=["analytics"])


@router.get("/top-products", response_model=TopProductsResponse)
async def top_products(
    request: Request,
    hours: int = Query(default=24, ge=1, le=24 * 30),
    limit: int = Query(default=10, ge=1, le=100),
) -> TopProductsResponse:
    query = f"""
        SELECT
            JSONExtractString(properties, 'product_id') AS product_id,
            JSONExtractString(properties, 'product_name') AS product_name,
            JSONExtractString(properties, 'category') AS category,
            count() AS event_count
        FROM default.user_events
        WHERE event_type IN ('product_view', 'add_to_cart', 'purchase')
          AND ts >= now() - INTERVAL {hours} HOUR
        GROUP BY product_id, product_name, category
        ORDER BY event_count DESC
        LIMIT {limit}
    """
    result = await asyncio.to_thread(request.app.state.clickhouse_client.query, query)

    products = [
        TopProductItem(
            product_id=row[0],
            product_name=row[1],
            category=row[2],
            event_count=int(row[3]),
        )
        for row in result.result_rows
    ]
    return TopProductsResponse(hours=hours, products=products)


@router.get("/views-by-category", response_model=ViewsByCategoryResponse)
async def views_by_category(
    request: Request,
    hours: int = Query(default=24, ge=1, le=24 * 30),
) -> ViewsByCategoryResponse:
    query = f"""
        SELECT
            JSONExtractString(properties, 'category') AS category,
            count() AS view_count
        FROM default.user_events
        WHERE event_type = 'product_view'
          AND ts >= now() - INTERVAL {hours} HOUR
        GROUP BY category
        ORDER BY view_count DESC
    """
    result = await asyncio.to_thread(request.app.state.clickhouse_client.query, query)

    categories = [
        CategoryViewsItem(category=row[0], view_count=int(row[1]))
        for row in result.result_rows
    ]
    return ViewsByCategoryResponse(hours=hours, categories=categories)


@router.get("/conversion-funnel", response_model=ConversionFunnelResponse)
async def conversion_funnel(
    request: Request,
    hours: int = Query(default=24, ge=1, le=24 * 30),
) -> ConversionFunnelResponse:
    query = f"""
        SELECT
            countIf(event_type = 'product_view') AS views,
            countIf(event_type = 'add_to_cart') AS add_to_cart,
            countIf(event_type = 'purchase') AS purchases
        FROM default.user_events
        WHERE ts >= now() - INTERVAL {hours} HOUR
    """
    result = await asyncio.to_thread(request.app.state.clickhouse_client.query, query)
    row = result.result_rows[0] if result.result_rows else (0, 0, 0)

    return ConversionFunnelResponse(
        hours=hours,
        views=int(row[0]),
        add_to_cart=int(row[1]),
        purchases=int(row[2]),
    )


@router.get("/events-timeseries", response_model=EventsTimeseriesResponse)
async def events_timeseries(
    request: Request,
    hours: int = Query(default=24, ge=1, le=24 * 30),
) -> EventsTimeseriesResponse:
    query = f"""
        SELECT
            toStartOfHour(ts) AS bucket,
            event_type,
            count() AS event_count
        FROM default.user_events
        WHERE ts >= now() - INTERVAL {hours} HOUR
        GROUP BY bucket, event_type
        ORDER BY bucket
    """
    result = await asyncio.to_thread(request.app.state.clickhouse_client.query, query)

    series = []
    for row in result.result_rows:
        bucket = row[0].isoformat() if isinstance(row[0], datetime) else str(row[0])
        series.append(
            EventsTimeseriesPoint(
                bucket=bucket,
                event_type=row[1],
                event_count=int(row[2]),
            )
        )
    return EventsTimeseriesResponse(hours=hours, series=series)


@router.get("/top-users", response_model=TopUsersResponse)
async def top_users(
    request: Request,
    hours: int = Query(default=24, ge=1, le=24 * 30),
    limit: int = Query(default=10, ge=1, le=100),
) -> TopUsersResponse:
    query = f"""
        SELECT
            user_id,
            count() AS event_count,
            uniq(event_type) AS distinct_event_types,
            min(ts) AS first_event,
            max(ts) AS last_event
        FROM default.user_events
        WHERE ts >= now() - INTERVAL {hours} HOUR
        GROUP BY user_id
        ORDER BY event_count DESC
        LIMIT {limit}
    """
    result = await asyncio.to_thread(request.app.state.clickhouse_client.query, query)

    users = []
    for row in result.result_rows:
        first_event = row[3].isoformat() if isinstance(row[3], datetime) else str(row[3])
        last_event = row[4].isoformat() if isinstance(row[4], datetime) else str(row[4])
        users.append(
            TopUserItem(
                user_id=row[0],
                event_count=int(row[1]),
                distinct_event_types=int(row[2]),
                first_event=first_event,
                last_event=last_event,
            )
        )

    return TopUsersResponse(hours=hours, users=users)
