FROM python:3.12.9

COPY --from=ghcr.io/astral-sh/uv:0.9 /uv /uvx /bin/

WORKDIR /app

ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PYTHON_DOWNLOADS=never

COPY ./server_new/pyproject.toml ./server_new/uv.lock ./server_new/.python-version ./
RUN uv sync --frozen --no-dev --no-install-project

COPY ./server_new /app
COPY ./builtin_functions /app/builtin_scripts

ENV PATH="/app/.venv/bin:$PATH"

COPY ./docker/workflow.entrypoint.sh /entrypoint.sh
