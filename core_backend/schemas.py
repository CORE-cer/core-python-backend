"""Pydantic models matching the frontend's expected API contract."""

from __future__ import annotations

from pydantic import BaseModel


class CreateQueryRequest(BaseModel):
    query: str
    query_name: str
