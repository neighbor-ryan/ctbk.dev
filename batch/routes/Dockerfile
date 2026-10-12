FROM ghcr.io/project-osrm/osrm-backend@sha256:84074e1e66bd7eedc7fe52ece0f4deb2e6865454855ae77675c5f753d281fd58
RUN apt-get update && apt-get install -y --no-install-recommends git ca-certificates curl && rm -rf /var/lib/apt/lists/*
COPY --from=ghcr.io/astral-sh/uv:0.10.0 /uv /usr/local/bin/uv
ARG REF
RUN git clone --filter=blob:none https://github.com/hudcostreets/ctbk.dev.git /app && cd /app && git checkout "$REF"
WORKDIR /app
RUN uv sync --frozen --no-dev --python 3.13 && uv pip install 'osmium==4.3.1'
ENV PATH="/app/.venv/bin:$PATH"
ENTRYPOINT []
CMD ["ctbk", "flow-routes", "nj"]
