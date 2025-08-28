import typer
import uvicorn

from newssearch.config import settings

app = typer.Typer()


@app.command()
def runserver():
    uvicorn.run(**settings.UVICORN_SETTINGS.model_dump())


@app.callback()
def callback():
    pass


if __name__ == "__main__":
    app()
