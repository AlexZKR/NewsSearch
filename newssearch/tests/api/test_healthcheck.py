from http import HTTPStatus

import pytest
from httpx import AsyncClient

pytestmark = [pytest.mark.asyncio]


async def test_healthcheck(client: AsyncClient):
    response = await client.get("/healthcheck")
    assert response.status_code == HTTPStatus.NO_CONTENT
