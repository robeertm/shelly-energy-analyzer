# Shelly Energy Analyzer — container image
# Serves the Flask web dashboard over plain HTTP on the internal port 8765
# (a reverse proxy in front terminates TLS). Config + database live under /data.
FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1

# Build tools for wheels that may need compiling (numpy/pandas ship wheels, but
# pymodbus/reportlab/pillow occasionally need a compiler on slim images).
RUN apt-get update \
    && apt-get install -y --no-install-recommends build-essential curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY . /app

# Install the package with all runtime dependencies declared in pyproject.toml.
RUN pip install .

# Persistent config + data directory.
RUN mkdir -p /data
VOLUME ["/data"]
WORKDIR /data

EXPOSE 8765

HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 \
    CMD curl -fsS http://127.0.0.1:8765/api/version || exit 1

# --no-ssl: TLS is handled by the reverse proxy; container speaks plain HTTP.
CMD ["python", "-m", "shelly_analyzer", "--config", "/data/config.json", \
     "--host", "0.0.0.0", "--port", "8765", "--no-ssl"]
