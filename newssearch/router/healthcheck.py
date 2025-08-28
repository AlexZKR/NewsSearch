from fastapi import APIRouter, Response, status

router = APIRouter()


@router.get("/healthcheck", status_code=status.HTTP_204_NO_CONTENT)
async def healthcheck(response: Response):
    response.status_code = status.HTTP_204_NO_CONTENT
    return response
