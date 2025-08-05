from api.routers.v1 import dag_crud, dag_run

routers = [
    dag_crud.router,
    dag_run.router,
]
