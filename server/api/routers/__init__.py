from api.routers import dag_router, udf_router, auth_router

routers = [
    dag_router.router,
    udf_router.router,
    auth_router.router,
]
