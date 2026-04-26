from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

from kiteconnect import KiteConnect

from services.kite_credentials_service import get_kite_credentials


VALID_LIQUIDITY_MODES = {"LIVE_STRICT", "AFTER_HOURS_HISTORICAL"}


def get_kite_client(user_id: str) -> KiteConnect:
    api_key, access_token = get_kite_credentials(user_id)
    kite = KiteConnect(api_key=api_key)
    kite.set_access_token(access_token)
    return kite


class KiteOptionChainAdapter:
    """Kite adapter for building the spec-required option_chain input.

    This adapter only reads instruments, quotes, and historical candles. It never places orders.

    Modes:
    - LIVE_STRICT: uses live quote depth bid/ask from kite.quote().
    - AFTER_HOURS_HISTORICAL: uses last volume-positive historical candle close as both bid and ask.
      This is non-production testing mode. The engine will force execution_ready=false.
    """

    def __init__(
        self,
        kite: KiteConnect,
        exchange: str = "NFO",
        liquidity_mode: str = "LIVE_STRICT",
        historical_lookback_days: int = 10,
        historical_interval: str = "5minute",
        min_historical_volume: int = 1,
    ) -> None:
        if liquidity_mode not in VALID_LIQUIDITY_MODES:
            raise ValueError("liquidity_mode must be LIVE_STRICT or AFTER_HOURS_HISTORICAL")
        self.kite = kite
        self.exchange = exchange
        self.liquidity_mode = liquidity_mode
        self.historical_lookback_days = historical_lookback_days
        self.historical_interval = historical_interval
        self.min_historical_volume = min_historical_volume

    def build_option_chain(self, symbol: str, asof_time: str | None = None) -> list[dict[str, Any]]:
        snapshot_time = asof_time or datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
        symbol_upper = symbol.upper()
        instruments = [
            i for i in self.kite.instruments(self.exchange)
            if i.get("name", "").upper() == symbol_upper and i.get("instrument_type") in {"CE", "PE"}
        ]
        instruments = sorted(
            instruments,
            key=lambda i: (i.get("expiry"), i.get("strike"), i.get("instrument_type"), i.get("tradingsymbol")),
        )

        quote_keys = [f"{self.exchange}:{i['tradingsymbol']}" for i in instruments]
        quotes: dict[str, Any] = {}
        for start in range(0, len(quote_keys), 200):
            quotes.update(self.kite.quote(quote_keys[start:start + 200]))

        historical_prices: dict[int, dict[str, Any]] = {}
        if self.liquidity_mode == "AFTER_HOURS_HISTORICAL":
            historical_prices = self._build_historical_price_map(instruments)

        chain = []
        for inst in instruments:
            key = f"{self.exchange}:{inst['tradingsymbol']}"
            q = quotes.get(key) or {}
            depth = q.get("depth") or {}
            buys = depth.get("buy") or []
            sells = depth.get("sell") or []
            bid = float(buys[0]["price"]) if buys and buys[0].get("price") is not None else 0.0
            ask = float(sells[0]["price"]) if sells and sells[0].get("price") is not None else 0.0
            last_price = float(q.get("last_price") or 0.0)
            open_interest = int(q.get("oi") or 0)
            volume = int(q.get("volume") or q.get("last_quantity") or 0)
            data_timestamp = snapshot_time

            if self.liquidity_mode == "AFTER_HOURS_HISTORICAL":
                hist = historical_prices.get(int(inst["instrument_token"]))
                if hist is not None:
                    close = float(hist["close"])
                    bid = close
                    ask = close
                    last_price = close
                    volume = int(hist["volume"])
                    open_interest = int(hist.get("oi") or open_interest or 0)
                    data_timestamp = hist["date"].isoformat()
                else:
                    bid = 0.0
                    ask = 0.0
                    volume = 0

            chain.append({
                "tradingsymbol": inst["tradingsymbol"],
                "instrument_token": inst["instrument_token"],
                "expiry": str(inst["expiry"]),
                "strike": float(inst["strike"]),
                "option_type": inst["instrument_type"],
                "bid_price": bid,
                "ask_price": ask,
                "last_price": last_price,
                "open_interest": open_interest,
                "volume": volume,
                "delta": None,
                "data_timestamp": data_timestamp,
                "underlying": inst.get("name"),
                "lot_size": int(inst.get("lot_size") or 0) or None,
                "delta_source": None,
                "delta_timestamp": None,
                "delta_source_verified": False,
                "pricing_source": self.liquidity_mode,
            })
        return chain

    def _build_historical_price_map(self, instruments: list[dict[str, Any]]) -> dict[int, dict[str, Any]]:
        """Return last volume-positive historical candle per option instrument.

        This can be API-heavy for large chains. It is intended for after-hours testing,
        not live trading.
        """
        to_dt = datetime.now()
        from_dt = to_dt - timedelta(days=self.historical_lookback_days)
        out: dict[int, dict[str, Any]] = {}

        for inst in instruments:
            token = int(inst["instrument_token"])
            try:
                candles = self.kite.historical_data(
                    instrument_token=token,
                    from_date=from_dt,
                    to_date=to_dt,
                    interval=self.historical_interval,
                    continuous=False,
                    oi=True,
                )
            except Exception:
                continue

            valid = [
                c for c in candles
                if int(c.get("volume") or 0) >= self.min_historical_volume
                and float(c.get("close") or 0.0) > 0.0
            ]
            if not valid:
                continue
            out[token] = valid[-1]
        return out
