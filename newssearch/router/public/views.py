from fastapi import APIRouter

router = APIRouter()


# @router.get("/hello", status_code=status.HTTP_200_OK)
# async def heeloo(request: Request, response: Response):
#     request.app.state.users_events_counter.inc({"path": request.scope["path"]})
#     return "hi!"
