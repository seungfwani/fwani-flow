from api.routers.v1 import dag_crud, udf_router, auth_router, monitor_router

routers = [
    dag_router.router,
    udf_router.router,
    auth_router.router,
    monitor_router.router,
]
