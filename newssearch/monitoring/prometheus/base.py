# def init_prometheus(app: FastAPI) -> None:
#     r = Registry()
#     app.state.users_events_counter = Counter(
#         name="events", doc="Number of events.", registry=r
#     )

#     app.add_middleware(MetricsMiddleware)
#     app.add_route("/metrics", metrics)
