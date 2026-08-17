# Market Data Backend — Design Document

**Status:** As-built. This document describes the market data subsystem exactly as implemented in `backend/app/market/` (verified against source on 2026-08-17). It supersedes the pre-implementation sketches in `planning/archive/` (`MARKET_INTERFACE.md`, `MARKET_SIMULATOR.md`, `MASSIVE_API.md`, `MARKET_DATA_DESIGN.md`) and is the reference for any code that needs to integrate with market data — the FastAPI app entrypoint, portfolio/trade execution, watchlist routes, and the LLM chat tool layer, none of which exist yet.

For a short summary of what was built and its test results, see `planning/MARKET_DATA_SUMMARY.md`.

---

## Table of Contents

1. [Architecture](#1-architecture)
2. [Directory Structure](#2-directory-structure)
3. [Data Model — `models.py`](#3-data-model--modelspy)
4. [Price Cache — `cache.py`](#4-price-cache--cachepy)
5. [Unified Interface — `interface.py`](#5-unified-interface--interfacepy)
6. [Seed Data — `seed_prices.py`](#6-seed-data--seed_pricespy)
7. [Simulator — `simulator.py`](#7-simulator--simulatorpy)
8. [Massive API Client — `massive_client.py`](#8-massive-api-client--massive_clientpy)
9. [Factory — `factory.py`](#9-factory--factorypy)
10. [SSE Streaming Endpoint — `stream.py`](#10-sse-streaming-endpoint--streampy)
11. [Public Package API — `__init__.py`](#11-public-package-api--__init__py)
12. [Integrating With the FastAPI App (not yet built)](#12-integrating-with-the-fastapi-app-not-yet-built)
13. [Watchlist Coordination](#13-watchlist-coordination)
14. [Testing](#14-testing)
15. [Error Handling & Edge Cases](#15-error-handling--edge-cases)
16. [Configuration Reference](#16-configuration-reference)

---

## 1. Architecture

Two interchangeable data sources implement one abstract interface and write into one shared, thread-safe cache. Everything downstream — SSE streaming, portfolio valuation, trade execution — reads from the cache and never talks to a data source directly.

```
                    MarketDataSource (ABC)
                    ┌──────────────┴──────────────┐
                    │                              │
          SimulatorDataSource              MassiveDataSource
          (GBM simulator, default,          (Polygon.io REST poller,
           no external calls)                used when MASSIVE_API_KEY set)
                    │                              │
                    └──────────────┬───────────────┘
                                   ▼
                             PriceCache
                     (thread-safe, in-memory,
                      versioned dict[ticker → PriceUpdate])
                                   │
                ┌──────────────────┼──────────────────┐
                ▼                  ▼                  ▼
      GET /api/stream/prices   Trade execution   Portfolio valuation
         (SSE, ~500ms)        (PriceCache.get_price)  (PriceCache.get_all)
```

**Strategy pattern.** `SimulatorDataSource` and `MassiveDataSource` both implement `MarketDataSource`. Code that starts, stops, or reconfigures market data (watchlist add/remove, app startup/shutdown) is written once against the abstract interface and works identically regardless of which concrete source is active.

**Push, not pull.** A data source's job is to *push* prices into `PriceCache` on its own schedule — every ~500ms for the simulator, every ~15s for Massive. Consumers never ask a data source "what's the price right now?"; they read the last value the source wrote. This decouples producer cadence from consumer cadence: the SSE loop always polls the cache at a fixed 500ms regardless of how fast the underlying source updates.

**One process, no external services.** Both sources run as `asyncio` background tasks inside the FastAPI process. There is no message broker, no Redis, no separate poller process — consistent with the project's single-container design (see `planning/PLAN.md` §3).

---

## 2. Directory Structure

```
backend/
  app/
    market/
      __init__.py         # Public re-exports
      models.py            # PriceUpdate — immutable price snapshot
      cache.py              # PriceCache — thread-safe in-memory store
      interface.py          # MarketDataSource — abstract base class
      seed_prices.py        # SEED_PRICES, TICKER_PARAMS, correlation constants
      simulator.py           # GBMSimulator (math) + SimulatorDataSource (adapter)
      massive_client.py       # MassiveDataSource — Polygon.io REST poller
      factory.py               # create_market_data_source() — env-driven selection
      stream.py                 # create_stream_router() — FastAPI SSE endpoint
  tests/
    market/
      test_models.py
      test_cache.py
      test_simulator.py
      test_simulator_source.py
      test_factory.py
      test_massive.py
  market_data_demo.py     # Rich-terminal live dashboard demo (`uv run market_data_demo.py`)
```

Import surface for the rest of the backend:

```python
from app.market import PriceCache, PriceUpdate, MarketDataSource, create_market_data_source, create_stream_router
```

---

## 3. Data Model — `models.py`

`PriceUpdate` is the *only* type that leaves the market data layer. SSE payloads, trade execution, and portfolio valuation all consume this shape — never a raw float, never a source-specific response object.

```python
from __future__ import annotations

import time
from dataclasses import dataclass, field


@dataclass(frozen=True, slots=True)
class PriceUpdate:
    """Immutable snapshot of a single ticker's price at a point in time."""

    ticker: str
    price: float
    previous_price: float
    timestamp: float = field(default_factory=time.time)  # Unix seconds

    @property
    def change(self) -> float:
        """Absolute price change from previous update."""
        return round(self.price - self.previous_price, 4)

    @property
    def change_percent(self) -> float:
        """Percentage change from previous update."""
        if self.previous_price == 0:
            return 0.0
        return round((self.price - self.previous_price) / self.previous_price * 100, 4)

    @property
    def direction(self) -> str:
        """'up', 'down', or 'flat'."""
        if self.price > self.previous_price:
            return "up"
        elif self.price < self.previous_price:
            return "down"
        return "flat"

    def to_dict(self) -> dict:
        """Serialize for JSON / SSE transmission."""
        return {
            "ticker": self.ticker,
            "price": self.price,
            "previous_price": self.previous_price,
            "timestamp": self.timestamp,
            "change": self.change,
            "change_percent": self.change_percent,
            "direction": self.direction,
        }
```

**Design rationale**

- `frozen=True` — value objects, safe to hand across `asyncio` tasks and threads without defensive copies.
- `slots=True` — memory efficiency; the simulator can create a fresh `PriceUpdate` per ticker every 500ms.
- `change` / `change_percent` / `direction` are **computed properties**, not stored fields. There is no way for a caller to construct an internally-inconsistent update (e.g. `direction="up"` with `price < previous_price`).
- `to_dict()` is the single JSON serialization boundary. The frontend's watchlist grid, sparkline, and flash-animation logic (`planning/PLAN.md` §10) all consume this exact shape over SSE.

Example:

```python
>>> from app.market.models import PriceUpdate
>>> u = PriceUpdate(ticker="AAPL", price=190.55, previous_price=190.30)
>>> u.direction
'up'
>>> u.change
0.25
>>> u.to_dict()
{'ticker': 'AAPL', 'price': 190.55, 'previous_price': 190.3, 'timestamp': 1755417600.0,
 'change': 0.25, 'change_percent': 0.1314, 'direction': 'up'}
```

---

## 4. Price Cache — `cache.py`

The single point of truth. Producers (one data source at a time) write; consumers (SSE, trade execution, portfolio valuation) read.

```python
from __future__ import annotations

import time
from threading import Lock

from .models import PriceUpdate


class PriceCache:
    """Thread-safe in-memory cache of the latest price for each ticker.

    Writers: SimulatorDataSource or MassiveDataSource (one at a time).
    Readers: SSE streaming endpoint, portfolio valuation, trade execution.
    """

    def __init__(self) -> None:
        self._prices: dict[str, PriceUpdate] = {}
        self._lock = Lock()
        self._version: int = 0  # Monotonically increasing; bumped on every update

    def update(self, ticker: str, price: float, timestamp: float | None = None) -> PriceUpdate:
        """Record a new price for a ticker. Returns the created PriceUpdate.

        Automatically computes direction and change from the previous price.
        If this is the first update for the ticker, previous_price == price (direction='flat').
        """
        with self._lock:
            ts = timestamp or time.time()
            prev = self._prices.get(ticker)
            previous_price = prev.price if prev else price

            update = PriceUpdate(
                ticker=ticker,
                price=round(price, 2),
                previous_price=round(previous_price, 2),
                timestamp=ts,
            )
            self._prices[ticker] = update
            self._version += 1
            return update

    def get(self, ticker: str) -> PriceUpdate | None:
        """Get the latest price for a single ticker, or None if unknown."""
        with self._lock:
            return self._prices.get(ticker)

    def get_all(self) -> dict[str, PriceUpdate]:
        """Snapshot of all current prices. Returns a shallow copy."""
        with self._lock:
            return dict(self._prices)

    def get_price(self, ticker: str) -> float | None:
        """Convenience: get just the price float, or None."""
        update = self.get(ticker)
        return update.price if update else None

    def remove(self, ticker: str) -> None:
        """Remove a ticker from the cache (e.g., when removed from watchlist)."""
        with self._lock:
            self._prices.pop(ticker, None)

    @property
    def version(self) -> int:
        """Current version counter. Useful for SSE change detection."""
        return self._version

    def __len__(self) -> int:
        with self._lock:
            return len(self._prices)

    def __contains__(self, ticker: str) -> bool:
        with self._lock:
            return ticker in self._prices
```

**Why `threading.Lock`, not `asyncio.Lock`.** `MassiveDataSource` runs its synchronous REST call inside `asyncio.to_thread(...)`, which executes on a real OS thread outside the event loop. An `asyncio.Lock` only protects coroutines cooperating on the same loop — it does nothing for a thread pool worker. `threading.Lock` is safe from both call sites (event-loop coroutines and thread-pool workers), and the critical sections here are a dict lookup and assignment — sub-microsecond, so contention is never a concern at this scale (10–50 tickers, 2 writes/sec).

**Why a version counter.** SSE polls the cache every 500ms regardless of source. Without a version counter it would re-serialize and re-send every ticker on every poll even when nothing changed — wasteful when the Massive source only actually updates once per 15s. The SSE loop instead does:

```python
last_version = -1
while True:
    if price_cache.version != last_version:
        last_version = price_cache.version
        send(price_cache.get_all())
    await asyncio.sleep(0.5)
```

Example usage:

```python
>>> from app.market.cache import PriceCache
>>> cache = PriceCache()
>>> cache.update("AAPL", 190.50)
PriceUpdate(ticker='AAPL', price=190.5, previous_price=190.5, ...)   # first write: flat
>>> cache.update("AAPL", 191.20)
PriceUpdate(ticker='AAPL', price=191.2, previous_price=190.5, ...)   # direction='up'
>>> cache.get_price("AAPL")
191.2
>>> cache.version
2
```

---

## 5. Unified Interface — `interface.py`

The contract both data sources satisfy. This is the "unified API" — everything above this line (SSE, trade execution, watchlist routes) is written once against `MarketDataSource` and is completely agnostic to whether the simulator or Massive is running underneath.

```python
from __future__ import annotations

from abc import ABC, abstractmethod


class MarketDataSource(ABC):
    """Contract for market data providers.

    Implementations push price updates into a shared PriceCache on their own
    schedule. Downstream code never calls the data source directly for prices —
    it reads from the cache.

    Lifecycle:
        source = create_market_data_source(cache)
        await source.start(["AAPL", "GOOGL", ...])
        # ... app runs ...
        await source.add_ticker("TSLA")
        await source.remove_ticker("GOOGL")
        # ... app shutting down ...
        await source.stop()
    """

    @abstractmethod
    async def start(self, tickers: list[str]) -> None:
        """Begin producing price updates for the given tickers.

        Starts a background task that periodically writes to the PriceCache.
        Must be called exactly once. Calling start() twice is undefined behavior.
        """

    @abstractmethod
    async def stop(self) -> None:
        """Stop the background task and release resources.

        Safe to call multiple times. After stop(), the source will not write
        to the cache again.
        """

    @abstractmethod
    async def add_ticker(self, ticker: str) -> None:
        """Add a ticker to the active set. No-op if already present.

        The next update cycle will include this ticker.
        """

    @abstractmethod
    async def remove_ticker(self, ticker: str) -> None:
        """Remove a ticker from the active set. No-op if not present.

        Also removes the ticker from the PriceCache.
        """

    @abstractmethod
    def get_tickers(self) -> list[str]:
        """Return the current list of actively tracked tickers."""
```

Five methods, no more. `start`/`stop` bracket the process lifetime (FastAPI `lifespan`); `add_ticker`/`remove_ticker` handle live watchlist edits; `get_tickers` supports diagnostics and tests. Neither implementation exposes anything beyond this — e.g. there is no `get_price()` on the interface, because reading prices is the cache's job, not the source's.

---

## 6. Seed Data — `seed_prices.py`

Pure constants — no logic, no imports beyond stdlib types. Used by the simulator for initial prices/volatility and available as a fallback reference for any future feature (e.g. historical charting) that wants realistic starting values.

```python
"""Seed prices and per-ticker parameters for the market simulator."""

# Realistic starting prices for the default watchlist (as of project creation)
SEED_PRICES: dict[str, float] = {
    "AAPL": 190.00,
    "GOOGL": 175.00,
    "MSFT": 420.00,
    "AMZN": 185.00,
    "TSLA": 250.00,
    "NVDA": 800.00,
    "META": 500.00,
    "JPM": 195.00,
    "V": 280.00,
    "NFLX": 600.00,
}

# Per-ticker GBM parameters
# sigma: annualized volatility (higher = more price movement)
# mu: annualized drift / expected return
TICKER_PARAMS: dict[str, dict[str, float]] = {
    "AAPL": {"sigma": 0.22, "mu": 0.05},
    "GOOGL": {"sigma": 0.25, "mu": 0.05},
    "MSFT": {"sigma": 0.20, "mu": 0.05},
    "AMZN": {"sigma": 0.28, "mu": 0.05},
    "TSLA": {"sigma": 0.50, "mu": 0.03},  # High volatility
    "NVDA": {"sigma": 0.40, "mu": 0.08},  # High volatility, strong drift
    "META": {"sigma": 0.30, "mu": 0.05},
    "JPM": {"sigma": 0.18, "mu": 0.04},  # Low volatility (bank)
    "V": {"sigma": 0.17, "mu": 0.04},  # Low volatility (payments)
    "NFLX": {"sigma": 0.35, "mu": 0.05},
}

# Default parameters for tickers not in the list above (dynamically added)
DEFAULT_PARAMS: dict[str, float] = {"sigma": 0.25, "mu": 0.05}

# Correlation groups for the simulator's Cholesky decomposition
# Tickers in the same group have higher intra-group correlation
CORRELATION_GROUPS: dict[str, set[str]] = {
    "tech": {"AAPL", "GOOGL", "MSFT", "AMZN", "META", "NVDA", "NFLX"},
    "finance": {"JPM", "V"},
}

# Correlation coefficients
INTRA_TECH_CORR = 0.6  # Tech stocks move together
INTRA_FINANCE_CORR = 0.5  # Finance stocks move together
CROSS_GROUP_CORR = 0.3  # Between sectors / unknown tickers
TSLA_CORR = 0.3  # TSLA does its own thing
```

A ticker added dynamically that isn't in `SEED_PRICES`/`TICKER_PARAMS` (e.g. the user or the LLM adds `PYPL` to the watchlist) falls back to a random seed price in `$50–$300` and `DEFAULT_PARAMS` (`sigma=0.25, mu=0.05`) — see `GBMSimulator._add_ticker_internal` below.

---

## 7. Simulator — `simulator.py`

Two classes: `GBMSimulator` is a pure, synchronous math engine with no `asyncio` awareness; `SimulatorDataSource` wraps it in an async loop and is the actual `MarketDataSource` implementation.

### 7.1 The Math

Geometric Brownian Motion — the same log-normal model behind Black-Scholes — advances each price by:

```
S(t+dt) = S(t) * exp((mu - sigma²/2)·dt + sigma·√dt·Z)
```

- `S(t)` — current price
- `mu` — annualized drift (expected return)
- `sigma` — annualized volatility
- `dt` — time step, as a fraction of a trading year
- `Z` — a (possibly correlated) draw from the standard normal distribution

At 500ms ticks over a 252-day, 6.5-hour trading year:

```python
TRADING_SECONDS_PER_YEAR = 252 * 6.5 * 3600   # 5,896,800
DEFAULT_DT = 0.5 / TRADING_SECONDS_PER_YEAR    # ≈ 8.48e-8
```

This `dt` is deliberately tiny — each tick is a sub-cent-scale nudge that only becomes a visible move over dozens of ticks, which is what makes the stream look like real intraday trading rather than random-walk noise. Because the update is multiplicative (`exp(...)` is always positive), prices can never go negative regardless of how many steps run.

### 7.2 Correlated Moves via Cholesky Decomposition

Real stocks don't move independently — tech names tend to move together. Each `step()` draws `n` independent standard normals and applies the Cholesky factor `L` of a correlation matrix `C` (`C = L @ Lᵀ`) to produce correlated draws:

```python
z_independent = np.random.standard_normal(n)
z_correlated = cholesky_L @ z_independent   # same marginal distribution, now correlated
```

Correlation is assigned by sector:

| Pair | Correlation |
|---|---|
| Two tech tickers (`AAPL, GOOGL, MSFT, AMZN, META, NVDA, NFLX`) | 0.6 |
| Two finance tickers (`JPM, V`) | 0.5 |
| Either ticker is `TSLA` | 0.3 (TSLA is deliberately excluded from the tech group's correlation — "does its own thing") |
| Cross-sector or unknown ticker | 0.3 |

The matrix is rebuilt (`_rebuild_cholesky`) whenever a ticker is added or removed — O(n²), trivial for the tens of tickers this app supports.

### 7.3 Random Shock Events

Each ticker has a small independent chance per tick (`event_probability`, default `0.001` = 0.1%) of a sudden 2–5% move, for visual drama on the dashboard:

```python
if random.random() < self._event_prob:
    shock_magnitude = random.uniform(0.02, 0.05)
    shock_sign = random.choice([-1, 1])
    self._prices[ticker] *= 1 + shock_magnitude * shock_sign
```

With 10 tickers at 2 ticks/sec, expect a shock somewhere in the watchlist roughly every ~50 seconds.

### 7.4 `GBMSimulator` — Full Implementation

```python
from __future__ import annotations

import asyncio
import logging
import math
import random

import numpy as np

from .cache import PriceCache
from .interface import MarketDataSource
from .seed_prices import (
    CORRELATION_GROUPS,
    CROSS_GROUP_CORR,
    DEFAULT_PARAMS,
    INTRA_FINANCE_CORR,
    INTRA_TECH_CORR,
    SEED_PRICES,
    TICKER_PARAMS,
    TSLA_CORR,
)

logger = logging.getLogger(__name__)


class GBMSimulator:
    """Geometric Brownian Motion simulator for correlated stock prices."""

    TRADING_SECONDS_PER_YEAR = 252 * 6.5 * 3600  # 5,896,800
    DEFAULT_DT = 0.5 / TRADING_SECONDS_PER_YEAR   # ~8.48e-8

    def __init__(
        self,
        tickers: list[str],
        dt: float = DEFAULT_DT,
        event_probability: float = 0.001,
    ) -> None:
        self._dt = dt
        self._event_prob = event_probability
        self._tickers: list[str] = []
        self._prices: dict[str, float] = {}
        self._params: dict[str, dict[str, float]] = {}
        self._cholesky: np.ndarray | None = None

        for ticker in tickers:
            self._add_ticker_internal(ticker)
        self._rebuild_cholesky()

    def step(self) -> dict[str, float]:
        """Advance all tickers by one time step. Returns {ticker: new_price}."""
        n = len(self._tickers)
        if n == 0:
            return {}

        z_independent = np.random.standard_normal(n)
        z_correlated = self._cholesky @ z_independent if self._cholesky is not None else z_independent

        result: dict[str, float] = {}
        for i, ticker in enumerate(self._tickers):
            params = self._params[ticker]
            mu, sigma = params["mu"], params["sigma"]

            drift = (mu - 0.5 * sigma**2) * self._dt
            diffusion = sigma * math.sqrt(self._dt) * z_correlated[i]
            self._prices[ticker] *= math.exp(drift + diffusion)

            if random.random() < self._event_prob:
                shock_magnitude = random.uniform(0.02, 0.05)
                shock_sign = random.choice([-1, 1])
                self._prices[ticker] *= 1 + shock_magnitude * shock_sign

            result[ticker] = round(self._prices[ticker], 2)

        return result

    def add_ticker(self, ticker: str) -> None:
        if ticker in self._prices:
            return
        self._add_ticker_internal(ticker)
        self._rebuild_cholesky()

    def remove_ticker(self, ticker: str) -> None:
        if ticker not in self._prices:
            return
        self._tickers.remove(ticker)
        del self._prices[ticker]
        del self._params[ticker]
        self._rebuild_cholesky()

    def get_price(self, ticker: str) -> float | None:
        return self._prices.get(ticker)

    def get_tickers(self) -> list[str]:
        return list(self._tickers)

    def _add_ticker_internal(self, ticker: str) -> None:
        if ticker in self._prices:
            return
        self._tickers.append(ticker)
        self._prices[ticker] = SEED_PRICES.get(ticker, random.uniform(50.0, 300.0))
        self._params[ticker] = TICKER_PARAMS.get(ticker, dict(DEFAULT_PARAMS))

    def _rebuild_cholesky(self) -> None:
        n = len(self._tickers)
        if n <= 1:
            self._cholesky = None
            return
        corr = np.eye(n)
        for i in range(n):
            for j in range(i + 1, n):
                rho = self._pairwise_correlation(self._tickers[i], self._tickers[j])
                corr[i, j] = corr[j, i] = rho
        self._cholesky = np.linalg.cholesky(corr)

    @staticmethod
    def _pairwise_correlation(t1: str, t2: str) -> float:
        tech = CORRELATION_GROUPS["tech"]
        finance = CORRELATION_GROUPS["finance"]
        if t1 == "TSLA" or t2 == "TSLA":
            return TSLA_CORR
        if t1 in tech and t2 in tech:
            return INTRA_TECH_CORR
        if t1 in finance and t2 in finance:
            return INTRA_FINANCE_CORR
        return CROSS_GROUP_CORR
```

`get_tickers()` is a public method (not private-attribute access) so `SimulatorDataSource.get_tickers()` doesn't reach into `GBMSimulator`'s internals.

### 7.5 `SimulatorDataSource` — Async Adapter

```python
class SimulatorDataSource(MarketDataSource):
    """MarketDataSource backed by the GBM simulator.

    Runs a background asyncio task that calls GBMSimulator.step() every
    `update_interval` seconds and writes results to the PriceCache.
    """

    def __init__(
        self,
        price_cache: PriceCache,
        update_interval: float = 0.5,
        event_probability: float = 0.001,
    ) -> None:
        self._cache = price_cache
        self._interval = update_interval
        self._event_prob = event_probability
        self._sim: GBMSimulator | None = None
        self._task: asyncio.Task | None = None

    async def start(self, tickers: list[str]) -> None:
        self._sim = GBMSimulator(tickers=tickers, event_probability=self._event_prob)
        # Seed the cache with initial prices so SSE has data immediately
        for ticker in tickers:
            price = self._sim.get_price(ticker)
            if price is not None:
                self._cache.update(ticker=ticker, price=price)
        self._task = asyncio.create_task(self._run_loop(), name="simulator-loop")
        logger.info("Simulator started with %d tickers", len(tickers))

    async def stop(self) -> None:
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        self._task = None
        logger.info("Simulator stopped")

    async def add_ticker(self, ticker: str) -> None:
        if self._sim:
            self._sim.add_ticker(ticker)
            price = self._sim.get_price(ticker)
            if price is not None:
                self._cache.update(ticker=ticker, price=price)
            logger.info("Simulator: added ticker %s", ticker)

    async def remove_ticker(self, ticker: str) -> None:
        if self._sim:
            self._sim.remove_ticker(ticker)
        self._cache.remove(ticker)
        logger.info("Simulator: removed ticker %s", ticker)

    def get_tickers(self) -> list[str]:
        return self._sim.get_tickers() if self._sim else []

    async def _run_loop(self) -> None:
        """Core loop: step the simulation, write to cache, sleep."""
        while True:
            try:
                if self._sim:
                    prices = self._sim.step()
                    for ticker, price in prices.items():
                        self._cache.update(ticker=ticker, price=price)
            except Exception:
                logger.exception("Simulator step failed")
            await asyncio.sleep(self._interval)
```

Key behaviors:

- **Immediate seeding.** `start()` writes every ticker's seed price into the cache *before* the background task even runs one iteration — the SSE endpoint has real data on its very first poll, no blank-screen delay.
- **Clean cancellation.** `stop()` cancels the task and awaits it, swallowing `CancelledError` — safe to call from a FastAPI `lifespan` shutdown block, and safe to call twice.
- **Per-tick exception isolation.** `_run_loop` wraps each `step()` call in `try/except` so one bad tick (e.g. a `numpy` edge case) logs and continues rather than killing the whole price feed for the process lifetime.

---

## 8. Massive API Client — `massive_client.py`

Massive (formerly Polygon.io) is used when `MASSIVE_API_KEY` is set. It's accessed via **REST polling**, not a WebSocket — simpler, and works on the free tier. The synchronous `massive` SDK client runs inside `asyncio.to_thread(...)` so it never blocks the event loop.

### 8.1 Endpoint Used

A single call fetches all watched tickers at once — critical for staying under the free-tier rate limit (5 requests/min):

```python
from massive import RESTClient
from massive.rest.models import SnapshotMarketType

client = RESTClient(api_key="...")
snapshots = client.get_snapshot_all(
    market_type=SnapshotMarketType.STOCKS,
    tickers=["AAPL", "GOOGL", "MSFT"],
)
for snap in snapshots:
    print(snap.ticker, snap.last_trade.price, snap.last_trade.timestamp)
```

REST equivalent: `GET /v2/snapshot/locale/us/markets/stocks/tickers?tickers=AAPL,GOOGL,MSFT`. Relevant response fields per ticker:

```json
{
  "ticker": "AAPL",
  "last_trade": { "price": 190.55, "timestamp": 1755417600000 },
  "day": { "previous_close": 189.90, "change_percent": 0.34, "open": 190.10, "high": 191.00, "low": 189.80 }
}
```

`last_trade.timestamp` is Unix **milliseconds**; the client converts to seconds before writing to `PriceCache` (which stores Unix seconds, matching `PriceUpdate.timestamp`).

### 8.2 `MassiveDataSource` — Full Implementation

```python
from __future__ import annotations

import asyncio
import logging

from massive import RESTClient
from massive.rest.models import SnapshotMarketType

from .cache import PriceCache
from .interface import MarketDataSource

logger = logging.getLogger(__name__)


class MassiveDataSource(MarketDataSource):
    """MarketDataSource backed by the Massive (Polygon.io) REST API.

    Polls GET /v2/snapshot/locale/us/markets/stocks/tickers for all watched
    tickers in a single API call, then writes results to the PriceCache.

    Rate limits:
      - Free tier: 5 req/min → poll every 15s (default)
      - Paid tiers: higher limits → poll every 2-5s
    """

    def __init__(
        self,
        api_key: str,
        price_cache: PriceCache,
        poll_interval: float = 15.0,
    ) -> None:
        self._api_key = api_key
        self._cache = price_cache
        self._interval = poll_interval
        self._tickers: list[str] = []
        self._task: asyncio.Task | None = None
        self._client: RESTClient | None = None

    async def start(self, tickers: list[str]) -> None:
        self._client = RESTClient(api_key=self._api_key)
        self._tickers = list(tickers)

        # Do an immediate first poll so the cache has data right away
        await self._poll_once()

        self._task = asyncio.create_task(self._poll_loop(), name="massive-poller")
        logger.info(
            "Massive poller started: %d tickers, %.1fs interval",
            len(tickers), self._interval,
        )

    async def stop(self) -> None:
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        self._task = None
        self._client = None
        logger.info("Massive poller stopped")

    async def add_ticker(self, ticker: str) -> None:
        ticker = ticker.upper().strip()
        if ticker not in self._tickers:
            self._tickers.append(ticker)
            logger.info("Massive: added ticker %s (will appear on next poll)", ticker)

    async def remove_ticker(self, ticker: str) -> None:
        ticker = ticker.upper().strip()
        self._tickers = [t for t in self._tickers if t != ticker]
        self._cache.remove(ticker)
        logger.info("Massive: removed ticker %s", ticker)

    def get_tickers(self) -> list[str]:
        return list(self._tickers)

    async def _poll_loop(self) -> None:
        """Poll on interval. First poll already happened in start()."""
        while True:
            await asyncio.sleep(self._interval)
            await self._poll_once()

    async def _poll_once(self) -> None:
        """Execute one poll cycle: fetch snapshots, update cache."""
        if not self._tickers or not self._client:
            return

        try:
            # The Massive RESTClient is synchronous — run in a thread to
            # avoid blocking the event loop.
            snapshots = await asyncio.to_thread(self._fetch_snapshots)
            processed = 0
            for snap in snapshots:
                try:
                    price = snap.last_trade.price
                    timestamp = snap.last_trade.timestamp / 1000.0  # ms -> s
                    self._cache.update(ticker=snap.ticker, price=price, timestamp=timestamp)
                    processed += 1
                except (AttributeError, TypeError) as e:
                    logger.warning("Skipping snapshot for %s: %s", getattr(snap, "ticker", "???"), e)
            logger.debug("Massive poll: updated %d/%d tickers", processed, len(self._tickers))

        except Exception as e:
            logger.error("Massive poll failed: %s", e)
            # Don't re-raise — the loop retries on the next interval.
            # Common failures: 401 (bad key), 429 (rate limit), network errors.

    def _fetch_snapshots(self) -> list:
        """Synchronous call to the Massive REST API. Runs in a thread."""
        return self._client.get_snapshot_all(
            market_type=SnapshotMarketType.STOCKS,
            tickers=self._tickers,
        )
```

Note: `RESTClient`/`SnapshotMarketType` are imported at **module level**, not lazily inside `start()`. `massive` is a required core dependency in `pyproject.toml` (`massive>=1.0.0`) regardless of whether `MASSIVE_API_KEY` is set, so there is no import-time branching to worry about — the module always loads cleanly, and mocking `massive_client.RESTClient` in tests works with a plain `patch(...)` (no `create=True` needed).

### 8.3 Error Handling Philosophy

| Failure | Behavior |
|---|---|
| **401 Unauthorized** (bad key) | Logged as error; poller keeps retrying every `poll_interval`. Cache keeps last-known prices. |
| **429 Rate limited** | Logged as error; retried on the next scheduled poll. |
| **Network timeout** | Logged as error; retried automatically. |
| **Malformed/partial snapshot** (e.g. missing `last_trade`) | That ticker is skipped with a warning; the rest of the batch still updates. |
| **Entire poll fails** | Cache retains stale prices — SSE keeps streaming last-known values rather than going blank. |

The poller never raises out of `_poll_once()` — every failure mode is caught, logged, and left for the next scheduled attempt.

---

## 9. Factory — `factory.py`

Environment-driven selection between the two sources, per `planning/PLAN.md` §5 ("If `MASSIVE_API_KEY` is set and non-empty → Massive; otherwise → simulator").

```python
from __future__ import annotations

import logging
import os

from .cache import PriceCache
from .interface import MarketDataSource
from .massive_client import MassiveDataSource
from .simulator import SimulatorDataSource

logger = logging.getLogger(__name__)


def create_market_data_source(price_cache: PriceCache) -> MarketDataSource:
    """Create the appropriate market data source based on environment variables.

    - MASSIVE_API_KEY set and non-empty → MassiveDataSource (real market data)
    - Otherwise → SimulatorDataSource (GBM simulation)

    Returns an unstarted source. Caller must await source.start(tickers).
    """
    api_key = os.environ.get("MASSIVE_API_KEY", "").strip()

    if api_key:
        logger.info("Market data source: Massive API (real data)")
        return MassiveDataSource(api_key=api_key, price_cache=price_cache)
    else:
        logger.info("Market data source: GBM Simulator")
        return SimulatorDataSource(price_cache=price_cache)
```

This is the only place in the codebase that branches on `MASSIVE_API_KEY`. Everything else — the SSE endpoint, trade execution, watchlist routes — is written against `MarketDataSource` and doesn't know or care which concrete class it got.

```python
price_cache = PriceCache()
source = create_market_data_source(price_cache)   # unstarted
await source.start(["AAPL", "GOOGL", "MSFT", ...])  # begins writing to price_cache
```

---

## 10. SSE Streaming Endpoint — `stream.py`

The single real-time endpoint the frontend consumes (`planning/PLAN.md` §6, §8): `GET /api/stream/prices`. A long-lived `text/event-stream` connection, polling `PriceCache` on a 500ms cadence and pushing only when the cache's version has advanced.

```python
from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import AsyncGenerator

from fastapi import APIRouter, Request
from fastapi.responses import StreamingResponse

from .cache import PriceCache

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/stream", tags=["streaming"])


def create_stream_router(price_cache: PriceCache) -> APIRouter:
    """Create the SSE streaming router with a reference to the price cache.

    This factory pattern lets us inject the PriceCache without globals.
    """

    @router.get("/prices")
    async def stream_prices(request: Request) -> StreamingResponse:
        """SSE endpoint for live price updates.

        Streams all tracked ticker prices every ~500ms. The client connects
        with EventSource and receives events in the format:

            data: {"AAPL": {"ticker": "AAPL", "price": 190.50, ...}, ...}

        Includes a retry directive so the browser auto-reconnects on
        disconnection (EventSource built-in behavior).
        """
        return StreamingResponse(
            _generate_events(price_cache, request),
            media_type="text/event-stream",
            headers={
                "Cache-Control": "no-cache",
                "Connection": "keep-alive",
                "X-Accel-Buffering": "no",  # Disable nginx buffering if proxied
            },
        )

    return router


async def _generate_events(
    price_cache: PriceCache,
    request: Request,
    interval: float = 0.5,
) -> AsyncGenerator[str, None]:
    """Async generator that yields SSE-formatted price events.

    Sends all prices every `interval` seconds. Stops when the client
    disconnects (detected via request.is_disconnected()).
    """
    yield "retry: 1000\n\n"   # tell the browser to retry after 1s if the connection drops

    last_version = -1
    client_ip = request.client.host if request.client else "unknown"
    logger.info("SSE client connected: %s", client_ip)

    try:
        while True:
            if await request.is_disconnected():
                logger.info("SSE client disconnected: %s", client_ip)
                break

            current_version = price_cache.version
            if current_version != last_version:
                last_version = current_version
                prices = price_cache.get_all()
                if prices:
                    data = {ticker: update.to_dict() for ticker, update in prices.items()}
                    yield f"data: {json.dumps(data)}\n\n"

            await asyncio.sleep(interval)
    except asyncio.CancelledError:
        logger.info("SSE stream cancelled for: %s", client_ip)
```

### Wire format

```
retry: 1000

data: {"AAPL":{"ticker":"AAPL","price":190.55,"previous_price":190.30,"timestamp":1755417600.5,"change":0.25,"change_percent":0.1314,"direction":"up"},"GOOGL":{...}}

```

### Frontend consumption (`planning/PLAN.md` §10)

```javascript
const eventSource = new EventSource('/api/stream/prices');
eventSource.onmessage = (event) => {
  const prices = JSON.parse(event.data);          // { "AAPL": {...}, "GOOGL": {...} }
  for (const [ticker, update] of Object.entries(prices)) {
    applyPriceFlash(ticker, update.direction);     // green/red CSS flash on 'up'/'down'
    appendSparklinePoint(ticker, update.price);     // accumulate for the mini-chart
  }
};
// EventSource reconnects automatically on drop — no manual retry logic needed.
```

**Why poll-and-diff instead of event-driven push from the source directly?** A fixed 500ms cadence gives the frontend evenly-spaced data points, which matters for the sparkline charts (`planning/PLAN.md` §10, "accumulated on the frontend from the SSE stream since page load"). If the SSE loop instead fired immediately whenever a source wrote a value, Massive's 15-second polls would produce irregular, bursty spacing. Reading the cache's `version` field keeps updates efficient without coupling SSE cadence to source cadence.

---

## 11. Public Package API — `__init__.py`

```python
"""Market data subsystem for FinAlly.

Public API:
    PriceUpdate         - Immutable price snapshot dataclass
    PriceCache          - Thread-safe in-memory price store
    MarketDataSource    - Abstract interface for data providers
    create_market_data_source - Factory that selects simulator or Massive
    create_stream_router - FastAPI router factory for SSE endpoint
"""

from .cache import PriceCache
from .factory import create_market_data_source
from .interface import MarketDataSource
from .models import PriceUpdate
from .stream import create_stream_router

__all__ = [
    "PriceUpdate",
    "PriceCache",
    "MarketDataSource",
    "create_market_data_source",
    "create_stream_router",
]
```

Everything else in `app/market/` (`simulator.py`, `massive_client.py`, `seed_prices.py`, concrete class names) is an implementation detail. Downstream code should never `from app.market.simulator import GBMSimulator` — always import from `app.market`.

---

## 12. Integrating With the FastAPI App (not yet built)

`backend/app/` currently contains only `app/market/` — there is no `main.py`, no database layer, no other routers yet (per `planning/PLAN.md`, market data is the one completed component). This section is the integration contract for whoever builds the rest of the backend.

### 12.1 Startup/shutdown via `lifespan`

```python
from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.market import PriceCache, create_market_data_source, create_stream_router


@asynccontextmanager
async def lifespan(app: FastAPI):
    # 1. Shared price cache
    price_cache = PriceCache()
    app.state.price_cache = price_cache

    # 2. Data source (simulator or Massive, chosen by MASSIVE_API_KEY)
    source = create_market_data_source(price_cache)
    app.state.market_source = source

    # 3. Load initial tickers from the watchlist table (seeded with the
    #    10 default tickers per PLAN.md §7 on first run)
    initial_tickers = await load_watchlist_tickers()  # SQLite, not yet implemented
    await source.start(initial_tickers)

    # 4. Mount the SSE router
    app.include_router(create_stream_router(price_cache))

    yield  # app runs

    # --- shutdown ---
    await source.stop()


app = FastAPI(title="FinAlly", lifespan=lifespan)


def get_price_cache() -> PriceCache:
    return app.state.price_cache


def get_market_source() -> MarketDataSource:
    return app.state.market_source
```

### 12.2 Dependency injection into other routes

```python
from fastapi import APIRouter, Depends, HTTPException

router = APIRouter(prefix="/api")


@router.post("/portfolio/trade")
async def execute_trade(trade: TradeRequest, price_cache: PriceCache = Depends(get_price_cache)):
    price = price_cache.get_price(trade.ticker)
    if price is None:
        raise HTTPException(400, f"Price not yet available for {trade.ticker}")
    # ... validate cash/shares, write to positions + trades tables at `price` ...


@router.post("/watchlist")
async def add_to_watchlist(
    payload: WatchlistAdd,
    source: MarketDataSource = Depends(get_market_source),
):
    # ... insert into watchlist table ...
    await source.add_ticker(payload.ticker)


@router.delete("/watchlist/{ticker}")
async def remove_from_watchlist(
    ticker: str,
    source: MarketDataSource = Depends(get_market_source),
):
    # ... delete from watchlist table ...
    # keep tracking if the user still holds a position (see §13 below)
    if not await has_open_position(ticker):
        await source.remove_ticker(ticker)
```

The LLM chat layer (`planning/PLAN.md` §9) reuses the exact same trade-execution and watchlist functions — trades and watchlist changes the model proposes go through identical validation and the same `MarketDataSource` calls as manual UI actions.

---

## 13. Watchlist Coordination

### Adding a ticker

```
POST /api/watchlist {"ticker": "PYPL"}
  → INSERT INTO watchlist (SQLite)
  → await source.add_ticker("PYPL")
        Simulator: GBMSimulator.add_ticker() seeds a price (SEED_PRICES or random
                   $50-300 fallback), rebuilds the Cholesky correlation matrix,
                   and the cache is updated immediately — no waiting for a tick.
        Massive:   appended to the polled ticker list; price appears on the
                   next scheduled poll (up to `poll_interval` seconds later).
  → 200 OK
```

### Removing a ticker

```
DELETE /api/watchlist/PYPL
  → DELETE FROM watchlist (SQLite)
  → check for an open position in `positions` (quantity > 0)
      if none → await source.remove_ticker("PYPL")   # stops tracking, drops from cache
      if held → keep tracking (see edge case below)
  → 200 OK
```

### Edge case: removing a watched ticker the user still holds

If a user drops `PYPL` from the watchlist but still owns shares, the data source must **keep** producing prices for it — otherwise portfolio valuation (`planning/PLAN.md` §7 `positions`, §8 `GET /api/portfolio`) has no price to compute unrealized P&L against. The watchlist route is responsible for this check; `MarketDataSource.remove_ticker` itself unconditionally stops tracking whatever ticker it's given, so the guard belongs in the route, not the market data layer:

```python
@router.delete("/watchlist/{ticker}")
async def remove_from_watchlist(ticker: str, source: MarketDataSource = Depends(get_market_source)):
    await db.delete_watchlist_entry(ticker)
    position = await db.get_position(ticker)
    if position is None or position.quantity == 0:
        await source.remove_ticker(ticker)
    return {"status": "ok"}
```

---

## 14. Testing

**73 tests across 6 modules in `backend/tests/market/`, all passing, 84% overall coverage.**

| Module | Tests | Coverage | What it covers |
|---|---|---|---|
| `test_models.py` | 11 | 100% | `PriceUpdate` properties (`change`, `change_percent`, `direction`), `to_dict()` |
| `test_cache.py` | 13 | 100% | `PriceCache` update/get/get_all/remove, version counter, first-update `direction='flat'` |
| `test_simulator.py` | 17 | 98% | `GBMSimulator`: positive prices, add/remove ticker, Cholesky rebuild, unknown-ticker fallback |
| `test_simulator_source.py` | 10 | — | `SimulatorDataSource` integration: cache seeded on `start()`, prices evolve, clean `stop()` |
| `test_factory.py` | 7 | 100% | `create_market_data_source` branches correctly on `MASSIVE_API_KEY` |
| `test_massive.py` | 13 | 56% (expected — real HTTP calls are mocked) | `_poll_once` cache updates, malformed-snapshot skip, API-error resilience |

Run locally:

```bash
cd backend
uv run --extra dev pytest -v
uv run --extra dev pytest --cov=app
uv run --extra dev ruff check app/ tests/
```

### Representative test patterns

```python
# GBM prices can never go negative — exp() is always positive
def test_prices_are_positive():
    sim = GBMSimulator(tickers=["AAPL"])
    for _ in range(10_000):
        assert sim.step()["AAPL"] > 0

# Cache seeded before the background loop's first tick
async def test_start_populates_cache():
    cache = PriceCache()
    source = SimulatorDataSource(price_cache=cache, update_interval=0.1)
    await source.start(["AAPL", "GOOGL"])
    assert cache.get("AAPL") is not None
    await source.stop()

# Massive poller never lets a malformed snapshot break the whole batch
async def test_malformed_snapshot_skipped():
    cache = PriceCache()
    source = MassiveDataSource(api_key="test", price_cache=cache, poll_interval=60.0)
    source._tickers = ["AAPL", "BAD"]
    good = _make_snapshot("AAPL", 190.50, 1707580800000)
    bad = MagicMock(ticker="BAD", last_trade=None)
    with patch.object(source, "_fetch_snapshots", return_value=[good, bad]):
        await source._poll_once()
    assert cache.get_price("AAPL") == 190.50
    assert cache.get_price("BAD") is None
```

### Live demo

```bash
cd backend
uv run market_data_demo.py
```

A Rich-terminal dashboard: all 10 default tickers, sparklines, color-coded direction arrows, and an event log for notable shock moves. Runs 60 seconds or until Ctrl+C — useful as a manual sanity check independent of the FastAPI app.

---

## 15. Error Handling & Edge Cases

| Scenario | Behavior |
|---|---|
| **Empty watchlist at startup** | `source.start([])` — simulator produces no prices, Massive skips its poll call. SSE sends no data until a ticker is added. |
| **Trade on a ticker with no cached price** | `price_cache.get_price(ticker)` returns `None` → route returns `400` ("Price not yet available for {ticker}. Please wait a moment and try again."). Practically only possible for Massive right after a ticker is added, since the simulator seeds synchronously in `add_ticker()`. |
| **Invalid `MASSIVE_API_KEY`** | First poll gets `401`; logged, poller keeps retrying every `poll_interval`. SSE stays connected but cache never populates. Fix is correcting the key and restarting — there's no auto-fallback to the simulator mid-session. |
| **Massive rate-limited (`429`)** | Logged; retried on the next scheduled poll. No backoff beyond the fixed interval. |
| **`PriceCache` under concurrent load** | `threading.Lock` serializes access; critical sections are O(1) dict ops, so contention is negligible at the tens-of-tickers, ~2 writes/sec scale this app runs at. |
| **Floating-point precision in GBM** | Prices are rounded to 2 decimals on every write (`GBMSimulator.step()` and `PriceCache.update()`); the exponential formulation is numerically stable and always positive. |
| **`start()` called twice** | Undocumented/unsupported — `MarketDataSource.start()` is documented as call-once. A second call would `create_task` a second loop and leak the first. Not guarded against; callers (the `lifespan` block) must not do this. |
| **`stop()` called multiple times** | Explicitly supported and tested — both implementations check `self._task is not None and not self._task.done()` before cancelling. |

---

## 16. Configuration Reference

| Parameter | Location | Default | Notes |
|---|---|---|---|
| `MASSIVE_API_KEY` | Environment variable | `""` (unset) | Non-empty → `MassiveDataSource`; empty/unset → `SimulatorDataSource`. See `planning/PLAN.md` §5. |
| `update_interval` | `SimulatorDataSource.__init__` | `0.5` s | Simulator tick cadence. |
| `event_probability` | `GBMSimulator.__init__` / `SimulatorDataSource.__init__` | `0.001` | Per-ticker, per-tick chance of a 2–5% shock move. |
| `dt` | `GBMSimulator.__init__` | `≈8.48e-8` | GBM time step, fraction of a trading year, derived from `TRADING_SECONDS_PER_YEAR = 252 * 6.5 * 3600`. |
| `poll_interval` | `MassiveDataSource.__init__` | `15.0` s | Free-tier-safe default (5 req/min). Lower for paid Massive tiers. |
| SSE push interval | `stream._generate_events` | `0.5` s | Fixed regardless of underlying source cadence. |
| SSE retry directive | `stream._generate_events` | `1000` ms | Sent as `retry: 1000\n\n`; browser `EventSource` auto-reconnect delay. |
| `numpy` | `pyproject.toml` dependency | `>=2.0.0` | Only used by the simulator (Cholesky decomposition, random normals). |
| `massive` | `pyproject.toml` dependency | `>=1.0.0` | Core dependency (not optional) — imported at module level in `massive_client.py`. |

Default watchlist seeded on first run (`planning/PLAN.md` §7): `AAPL, GOOGL, MSFT, AMZN, TSLA, NVDA, META, JPM, V, NFLX` — matches `SEED_PRICES` / `TICKER_PARAMS` keys exactly.
