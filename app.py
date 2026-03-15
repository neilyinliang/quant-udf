"""Entrypoint for the UDF service."""

from uvicorn import run

from udf_service.server import app


if __name__ == "__main__":
    run("udf_service.server:app", host="0.0.0.0", port=8000, log_level="info")
