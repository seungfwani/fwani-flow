from api.routers.v2 import dag_router, dag_run_router, common_router

routers = [
    dag_router.router,
    dag_run_router.router,
    common_router.router,
]
