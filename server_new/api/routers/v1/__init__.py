from api.routers.v1 import dag_crud_router, dag_run_router, code_block_router, template_router, system_function_router

routers = [
    dag_crud_router.router,
    dag_run_router.router,
    code_block_router.router,
    template_router.router,
    system_function_router.router,
]
