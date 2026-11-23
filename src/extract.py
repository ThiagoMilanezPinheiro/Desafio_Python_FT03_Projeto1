"""Extração: cliente HTTP com rate limiting e exponential backoff."""
from __future__ import annotations

import logging
import random
import time
from datetime import date, timedelta
from typing import Any

import requests

from src.config import Settings

log = logging.getLogger(__name__)

RETRYABLE_STATUS = {429, 500, 502, 503, 504}


class RateLimitedClient:
    """Garante no máximo N requisições/segundo e repete falhas transitórias."""

    def __init__(self, settings: Settings) -> None:
        self.s = settings
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": "seismic-pipeline/1.0"})
        self._min_interval = 1.0 / max(settings.api_rate_limit_per_sec, 0.1)
        self._last_call = 0.0

    def _throttle(self) -> None:
        wait = self._min_interval - (time.monotonic() - self._last_call)
        if wait > 0:
            time.sleep(wait)
        self._last_call = time.monotonic()

    def get_json(self, url: str, params: dict[str, Any]) -> dict[str, Any]:
        for attempt in range(1, self.s.api_max_retries + 1):
            self._throttle()
            try:
                resp = self.session.get(url, params=params, timeout=self.s.api_timeout)
                if resp.status_code in RETRYABLE_STATUS:
                    raise requests.HTTPError(f"HTTP {resp.status_code}", response=resp)
                resp.raise_for_status()
                return resp.json()
            except (requests.ConnectionError, requests.Timeout, requests.HTTPError) as exc:
                status = getattr(getattr(exc, "response", None), "status_code", None)
                if status is not None and status not in RETRYABLE_STATUS:
                    raise  # erro 4xx definitivo: não adianta repetir
                if attempt == self.s.api_max_retries:
                    raise
                # backoff exponencial + jitter: 2^1, 2^2, 2^3... segundos
                delay = self.s.api_backoff_factor**attempt + random.uniform(0, 1)
                log.warning("Tentativa %s falhou (%s). Nova tentativa em %.1fs", attempt, exc, delay)
                time.sleep(delay)
        raise RuntimeError("unreachable")


def _daily_windows(start: str, end: str):
    """Divide o período em janelas diárias (a API limita 20 mil eventos por chamada)."""
    d0, d1 = date.fromisoformat(start), date.fromisoformat(end)
    while d0 < d1:
        nxt = min(d0 + timedelta(days=1), d1)
        yield d0.isoformat(), nxt.isoformat()
        d0 = nxt


def extract_earthquakes(settings: Settings) -> list[dict[str, Any]]:
    """Retorna a lista de respostas GeoJSON brutas (uma por janela)."""
    client = RateLimitedClient(settings)
    payloads = []
    for start, end in _daily_windows(settings.start_date, settings.end_date):
        params = {
            "format": "geojson",
            "starttime": start,
            "endtime": end,
            "minmagnitude": settings.min_magnitude,
        }
        data = client.get_json(settings.api_base_url, params)
        log.info("Janela %s -> %s: %s eventos", start, end, len(data.get("features", [])))
        payloads.append(data)
    return payloads
