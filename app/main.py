from __future__ import annotations

import logging
from pathlib import Path

from fastapi import FastAPI
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from app.api.calls import router as calls_router
from app.config import settings

STATIC_DIR = Path(__file__).parent / "static"
log = logging.getLogger("bt-cc-demo")


app = FastAPI(
    title="BT Next-Gen Contact Centre Demo",
    description="Multi-Agent Operational Customer 360 — spec companion",
    version="0.1.0",
)
app.include_router(calls_router)
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")


@app.on_event("startup")
async def _on_startup() -> None:
    mongo = "mock-mongo" if settings.mock_mode else "atlas"
    llm = "mock-llm" if settings.mock_llm else f"grove:{settings.llm_model}"
    log.warning("=== BT CC demo booting: %s · %s ===", mongo, llm)
    if not settings.mock_llm:
        log.warning("    LLM endpoint: %s", settings.llm_base_url)


@app.get("/", include_in_schema=False)
async def root():
    return FileResponse(STATIC_DIR / "index.html")


@app.get("/healthz")
async def healthz():
    return {"ok": True, "mockMode": settings.mock_mode, "mockLlm": settings.mock_llm}
