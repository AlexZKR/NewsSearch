from fastapi import FastAPI

from .healthcheck import router as healthcheck_router
from .public.views import router as public_router


def init_api(app: FastAPI) -> None:
    app.include_router(router=healthcheck_router)
    app.include_router(router=public_router)
