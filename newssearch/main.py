from fastapi import FastAPI

from newssearch.router import init_api


def create_app() -> FastAPI:
    app = FastAPI(title="NewsSearch", version="0.1.0")
    init_api(app)
    return app


app = create_app()
