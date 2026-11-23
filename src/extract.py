"""Extração: cliente HTTP com rate limiting, retentativas e exponential backoff.

Fontes (domínios distintos):
  * USGS Earthquake Catalog  -> sismologia (eventos sísmicos em GeoJSON)
  * World Bank Open Data     -> socioeconômico (países, região, renda e população)
"""
from __future__ import annotations

import logging
import random
import time
from collections.abc import Iterator
from dataclasses import dataclass, field
from datetime import date, timedelta
from typing import Any

import requests

from src.config import Settings

log = logging.getLogger(__name__)

RETRYABLE_STATUS = {429, 500, 502, 503, 504}


class ExtractionError(RuntimeError):
    """Falha definitiva de extração (após esgotar as retentativas)."""


@dataclass
class ClientStats:
    requests: int = 0
    retries: int = 0
    failures: list[str] = field(default_factory=list)


class RateLimitedClient:
    """Garante no máximo N requisições/segundo e repete falhas transitórias."""

    def __init__(self, settings: Settings) -> None:
        self.s = settings
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": "seismic-pipeline/1.0"})
        self._min_interval = 1.0 / max(settings.api_rate_limit_per_sec, 0.1)
        self._last_call = 0.0
        self.stats = ClientStats()

    def _throttle(self) -> None:
        wait = self._min_interval - (time.monotonic() - self._last_call)
        if wait > 0:
            time.sleep(wait)
        self._last_call = time.monotonic()

    def _retry_delay(self, attempt: int, resp: requests.Response | None) -> float:
        """Respeita o header Retry-After (429/503); senão backoff exponencial + jitter."""
        retry_after = resp.headers.get("Retry-After") if resp is not None else None
        if retry_after and retry_after.isdigit():
            return float(retry_after)
        return self.s.api_backoff_factor**attempt + random.uniform(0, 1)

    def get_json(self, url: str, params: dict[str, Any]) -> Any:
        for attempt in range(1, self.s.api_max_retries + 1):
            self._throttle()
            self.stats.requests += 1
            resp: requests.Response | None = None
            try:
                resp = self.session.get(url, params=params, timeout=self.s.api_timeout)
                if resp.status_code in RETRYABLE_STATUS:
                    raise requests.HTTPError(f"HTTP {resp.status_code}", response=resp)
                resp.raise_for_status()
                return resp.json()
            except requests.HTTPError as exc:
                if resp is not None and resp.status_code not in RETRYABLE_STATUS:
                    # 4xx definitivo (ex.: 400 parâmetro inválido): repetir não resolve
                    self.stats.failures.append(f"{url} -> {exc}")
                    raise ExtractionError(f"Erro não recuperável em {url}: {exc}") from exc
                last_exc: Exception = exc
            except (requests.ConnectionError, requests.Timeout, ValueError) as exc:
                # ValueError cobre JSON inválido/truncado
                last_exc = exc

            if attempt == self.s.api_max_retries:
                self.stats.failures.append(f"{url} -> {last_exc}")
                raise ExtractionError(
                    f"{url} falhou após {attempt} tentativas: {last_exc}"
                ) from last_exc
            delay = self._retry_delay(attempt, resp)
            self.stats.retries += 1
            log.warning("Tentativa %s falhou (%s). Nova tentativa em %.1fs", attempt, last_exc, delay)
            time.sleep(delay)
        raise ExtractionError("unreachable")


def _windows(start: str, end: str, days: int) -> Iterator[tuple[str, str]]:
    """Divide o período em janelas (a USGS limita 20 mil eventos por chamada)."""
    d0, d1 = date.fromisoformat(start), date.fromisoformat(end)
    while d0 < d1:
        nxt = min(d0 + timedelta(days=days), d1)
        yield d0.isoformat(), nxt.isoformat()
        d0 = nxt


def extract_earthquakes(client: RateLimitedClient, s: Settings) -> list[dict[str, Any]]:
    """USGS: retorna as respostas GeoJSON brutas (uma por janela de tempo)."""
    payloads: list[dict[str, Any]] = []
    for start, end in _windows(s.start_date, s.end_date, s.window_days):
        params = {
            "format": "geojson",
            "starttime": start,
            "endtime": end,
            "minmagnitude": s.min_magnitude,
            "orderby": "time-asc",
        }
        data = client.get_json(s.usgs_api_url, params)
        log.info("USGS %s -> %s: %s eventos", start, end, len(data.get("features", [])))
        payloads.append(data)
    return payloads


def _worldbank_paginated(
    client: RateLimitedClient, url: str, params: dict[str, Any]
) -> list[dict[str, Any]]:
    """A API do Banco Mundial responde [metadados, registros] e é paginada."""
    rows: list[dict[str, Any]] = []
    page, pages = 1, 1
    while page <= pages:
        body = client.get_json(url, {**params, "format": "json", "per_page": 500, "page": page})
        if not isinstance(body, list) or len(body) < 2 or body[1] is None:
            raise ExtractionError(f"Resposta inesperada do World Bank em {url}: {str(body)[:200]}")
        meta, data = body[0], body[1]
        pages = int(meta.get("pages", 1))
        rows.extend(data)
        page += 1
    return rows


def extract_worldbank(client: RateLimitedClient, s: Settings) -> dict[str, list[dict[str, Any]]]:
    """World Bank: cadastro de países (JSON aninhado) e série de população."""
    countries = _worldbank_paginated(client, f"{s.worldbank_api_url}/country", {})
    population = _worldbank_paginated(
        client,
        f"{s.worldbank_api_url}/country/all/indicator/SP.POP.TOTL",
        {"date": s.population_years},
    )
    log.info("World Bank: %s países, %s registros de população", len(countries), len(population))
    return {"countries": countries, "population": population}
