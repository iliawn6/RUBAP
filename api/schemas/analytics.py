from pydantic import BaseModel


class TopProductItem(BaseModel):
    product_id: str | None = None
    product_name: str | None = None
    category: str | None = None
    event_count: int


class TopProductsResponse(BaseModel):
    hours: int
    products: list[TopProductItem]


class CategoryViewsItem(BaseModel):
    category: str | None = None
    view_count: int


class ViewsByCategoryResponse(BaseModel):
    hours: int
    categories: list[CategoryViewsItem]


class ConversionFunnelResponse(BaseModel):
    hours: int
    views: int
    add_to_cart: int
    purchases: int


class EventsTimeseriesPoint(BaseModel):
    bucket: str
    event_type: str
    event_count: int


class EventsTimeseriesResponse(BaseModel):
    hours: int
    series: list[EventsTimeseriesPoint]


class TopUserItem(BaseModel):
    user_id: str
    event_count: int
    distinct_event_types: int
    first_event: str
    last_event: str


class TopUsersResponse(BaseModel):
    hours: int
    users: list[TopUserItem]
