"""CLI entry point."""

from __future__ import annotations

import os

import uvicorn

from flex_llm_router.app import create_app


def main() -> None:
    config_path = os.getenv("FLEX_CONFIG", "config/pools.yaml")
    host = os.getenv("FLEX_HOST", "127.0.0.1")
    port = int(os.getenv("FLEX_PORT", "7800"))
    uvicorn.run(create_app(config_path), host=host, port=port)
