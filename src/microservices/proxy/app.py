import os
import random
from typing import Dict

import httpx
from fastapi import FastAPI, Request, Response

app = FastAPI(title="CinemaAbyss Proxy")

# Базовые адреса обоих бэкендов для домена movies
MOVIES_LEGACY_BASE = os.getenv("MONOLITH_URL", "http://monolith:8080")
MOVIES_NEW_BASE = os.getenv("MOVIES_SERVICE_URL", "http://movies-service:8081")

def _percent() -> int:
    """Безопасно читаем процент миграции 0..100."""
    try:
        p = int(os.getenv("MOVIES_MIGRATION_PERCENT", "0"))
    except ValueError:
        p = 0
    return max(0, min(100, p))

def _pick_movies_backend() -> str:
    """Выбор бэкенда по фиче‑флагу: с вероятностью p% идём в новый сервис."""
    p = _percent()
    # 1..100 <= p -> новый сервис, иначе legacy
    return MOVIES_NEW_BASE if random.randint(1, 100) <= p else MOVIES_LEGACY_BASE

@app.get("/healthz")
def healthz():
    return {
        "status": "ok",
        "service": "proxy",
        "movies": {
            "legacy": MOVIES_LEGACY_BASE,
            "new": MOVIES_NEW_BASE,
            "migration_percent": _percent(),
        },
    }

@app.get("/health")
def health():
    return {"status": "ok"}

@app.get("/api/movies")
async def proxy_movies(request: Request):
    """Простейший прокси на GET /api/movies (достаточно для тестов задания)."""
    backend = _pick_movies_backend()
    target = backend.rstrip("/") + "/api/movies"

    # Пробрасываем query‑параметры как есть
    params: Dict[str, str] = dict(request.query_params)

    timeout = httpx.Timeout(10.0)
    async with httpx.AsyncClient(timeout=timeout) as client:
        upstream = await client.get(target, params=params)

    # Отдаём тело и статус, важные заголовки (тип контента)
    headers = {}
    ct = upstream.headers.get("content-type")
    if ct:
        headers["content-type"] = ct
    return Response(content=upstream.content, status_code=upstream.status_code, headers=headers)

@app.get("/api/users")
async def proxy_users(request: Request):
    """Проксирует запросы пользователей всегда в монолит (users не мигрируют)"""
    target = MOVIES_LEGACY_BASE.rstrip("/") + "/api/users"
    
    # Пробрасываем query-параметры как есть
    params: Dict[str, str] = dict(request.query_params)

    timeout = httpx.Timeout(10.0)
    async with httpx.AsyncClient(timeout=timeout) as client:
        upstream = await client.get(target, params=params)

    # Отдаём тело и статус, важные заголовки (тип контента)
    headers = {}
    ct = upstream.headers.get("content-type")
    if ct:
        headers["content-type"] = ct
    return Response(content=upstream.content, status_code=upstream.status_code, headers=headers)
