from fastapi import FastAPI

from .healthcheck import router as healthcheck_router


def init_api(app: FastAPI) -> None:
    app.include_router(router=healthcheck_router)
