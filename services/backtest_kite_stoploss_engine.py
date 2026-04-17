# File name: backtest_kite_stoploss_engine.py

import sys
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

from kiteconnect import KiteConnect

from kite_credentials_service import get_kite_credentials
from kite_stoploss_service_v2 import calculate_noise_filtered_stop


VALID_INTERVALS = {
    "day",
    "minute",
    "3minute",
    "5minute",
    "10minute",
    "15minute",
    "30minute",
    "60minute",
}

LOOKAHEAD_DAYS = 30


@dataclass
class BacktestConfig:
    user_id: str
    exchange: str
    tradingsymbol: str
    side: str
    entry_date: str
    from_date: str
    to_date: str
    interval: str = "day"
    entry_price: Optional[float] = None
    tick_size: float = 0.05
    atr_period: int = 14
    atr_multiple: float = 2.5
    swing_lookback: int = 5
    quantity: int = 1


def get_kite_client(user_id: str) -> KiteConnect:
    api_key, access_token = get_kite_credentials(user_id)
    kite = KiteConnect(api_key=api_key)
    kite.set_access_token(access_token)
    return kite


def normalize_date(value: str) -> datetime:
    return datetime.strptime(value, "%Y-%m-%d")


def candles_to_dicts(raw_candles: List[Dict]) -> List[Dict]:
    out = []
    for c in raw_candles:
        out.append(
            {
                "date": c["date"],
                "open": float(c["open"]),
                "high": float(c["high"]),
                "low": float(c["low"]),
                "close": float(c["close"]),
                "volume": c.get("volume"),
                "oi": c.get("oi"),
            }
        )
    return out


def resolve_instrument(
    kite: KiteConnect,
    exchange: str,
    tradingsymbol: str,
) -> Dict:
    instruments = kite.instruments(exchange)
    matches = [i for i in instruments if i.get("tradingsymbol") == tradingsymbol]

    if not matches:
        raise ValueError(f"Instrument not found for {exchange}:{tradingsymbol}")

    exact = matches[0]
    return {
        "instrument_token": int(exact["instrument_token"]),
        "tradingsymbol": exact["tradingsymbol"],
        "exchange": exact["exchange"],
        "tick_size": float(exact.get("tick_size") or 0.05),
        "instrument_type": exact.get("instrument_type"),
        "segment": exact.get("segment"),
        "name": exact.get("name"),
    }


def fetch_historical_candles(
    kite: KiteConnect,
    instrument_token: int,
    from_date: str,
    to_date: str,
    interval: str,
) -> List[Dict]:
    if interval not in VALID_INTERVALS:
        raise ValueError(f"interval must be one of: {', '.join(sorted(VALID_INTERVALS))}")

    from_dt = normalize_date(from_date)
    to_dt = normalize_date(to_date) + timedelta(days=1)

    raw = kite.historical_data(
        instrument_token=instrument_token,
        from_date=from_dt,
        to_date=to_dt,
        interval=interval,
        continuous=False,
        oi=True,
    )
    candles = candles_to_dicts(raw)

    if not candles:
        raise ValueError("No historical candles returned")

    return candles


def find_entry_index(candles: List[Dict], entry_date: str) -> int:
    target = normalize_date(entry_date).date()

    for i, candle in enumerate(candles):
        candle_dt = candle["date"]
        candle_date = candle_dt.date() if hasattr(candle_dt, "date") else candle_dt
        if candle_date >= target:
            return i

    raise ValueError(f"No candle found on or after entry_date={entry_date}")


def simulate_exit_price(
    side: str,
    candle: Dict,
    trigger_price: float,
    limit_price: float,
) -> Optional[Tuple[float, str]]:
    low = float(candle["low"])
    high = float(candle["high"])
    open_price = float(candle["open"])

    if side == "LONG":
        if open_price <= trigger_price:
            return open_price, "GAP_DOWN_OPEN_EXIT"
        if low <= trigger_price:
            if low <= limit_price:
                return limit_price, "LIMIT_EXIT"
            return trigger_price, "TRIGGER_EXIT"
        return None

    if side == "SHORT":
        if open_price >= trigger_price:
            return open_price, "GAP_UP_OPEN_EXIT"
        if high >= trigger_price:
            if high >= limit_price:
                return limit_price, "LIMIT_EXIT"
            return trigger_price, "TRIGGER_EXIT"
        return None

    raise ValueError("side must be LONG or SHORT")


def compute_pnl(side: str, entry_price: float, exit_price: float, quantity: int) -> Tuple[float, float]:
    if side == "LONG":
        per_unit = exit_price - entry_price
    else:
        per_unit = entry_price - exit_price
    total = per_unit * quantity
    return per_unit, total


def backtest_stop_engine(config: BacktestConfig) -> Dict:
    if config.side not in {"LONG", "SHORT"}:
        raise ValueError("side must be LONG or SHORT")

    kite = get_kite_client(config.user_id)
    instrument = resolve_instrument(
        kite=kite,
        exchange=config.exchange,
        tradingsymbol=config.tradingsymbol,
    )

    tick_size = config.tick_size or instrument["tick_size"]
    candles = fetch_historical_candles(
        kite=kite,
        instrument_token=instrument["instrument_token"],
        from_date=config.from_date,
        to_date=config.to_date,
        interval=config.interval,
    )

    entry_index = find_entry_index(candles, config.entry_date)

    min_history = max(config.atr_period + 1, config.swing_lookback + 1)
    if entry_index < min_history:
        raise ValueError(
            f"Not enough candle history before entry_date. Need at least {min_history} completed candles before entry."
        )

    entry_candle = candles[entry_index]
    entry_price = config.entry_price if config.entry_price is not None else float(entry_candle["close"])

    daily_log = []
    exit_info = None

    for i in range(entry_index + 1, len(candles)):
        history = candles[:i]
        current_candle = candles[i]

        stop = calculate_noise_filtered_stop(
            candles=history,
            side=config.side,
            tick_size=tick_size,
            entry_price=entry_price,
            atr_period=config.atr_period,
            atr_multiple=config.atr_multiple,
            swing_lookback=config.swing_lookback,
        )

        trigger_price = float(stop["trigger_price"])
        limit_price = float(stop["limit_price"])

        log_row = {
            "date": current_candle["date"],
            "open": float(current_candle["open"]),
            "high": float(current_candle["high"]),
            "low": float(current_candle["low"]),
            "close": float(current_candle["close"]),
            "atr": float(stop["atr"]),
            "raw_stop": float(stop["raw_stop"]),
            "trigger_price": trigger_price,
            "limit_price": limit_price,
        }

        simulated = simulate_exit_price(
            side=config.side,
            candle=current_candle,
            trigger_price=trigger_price,
            limit_price=limit_price,
        )

        if simulated is not None:
            exit_price, exit_reason = simulated
            per_unit_pnl, total_pnl = compute_pnl(
                side=config.side,
                entry_price=entry_price,
                exit_price=exit_price,
                quantity=config.quantity,
            )

            exit_index = i

            lookahead_slice = candles[exit_index: exit_index + LOOKAHEAD_DAYS]

            future_high = max(c["high"] for c in lookahead_slice) if lookahead_slice else None
            future_low = min(c["low"] for c in lookahead_slice) if lookahead_slice else None

            noise_flag = False

            if config.side == "LONG":
                prev_high = max(c["high"] for c in candles[:exit_index])
                if future_high is not None and future_high > prev_high:
                    noise_flag = True

            elif config.side == "SHORT":
                prev_low = min(c["low"] for c in candles[:exit_index])
                if future_low is not None and future_low < prev_low:
                    noise_flag = True

            exit_info = {
                "exit_date": current_candle["date"],
                "exit_price": round(exit_price, 4),
                "exit_reason": exit_reason,
                "per_unit_pnl": round(per_unit_pnl, 4),
                "total_pnl": round(total_pnl, 4),
                "future_high": round(future_high, 2) if future_high else None,
                "future_low": round(future_low, 2) if future_low else None,
                "noise_exit": noise_flag,
            }
            log_row["exit_triggered"] = True
            log_row["exit_reason"] = exit_reason
            log_row["exit_price"] = round(exit_price, 4)
            daily_log.append(log_row)
            break

        log_row["exit_triggered"] = False
        daily_log.append(log_row)

    if exit_info is None:
        final_candle = candles[-1]
        final_price = float(final_candle["close"])
        per_unit_pnl, total_pnl = compute_pnl(
            side=config.side,
            entry_price=entry_price,
            exit_price=final_price,
            quantity=config.quantity,
        )
        exit_info = {
            "exit_date": final_candle["date"],
            "exit_price": round(final_price, 4),
            "exit_reason": "NO_STOP_HIT_MARK_TO_MARKET",
            "per_unit_pnl": round(per_unit_pnl, 4),
            "total_pnl": round(total_pnl, 4),
        }

    return {
        "instrument": instrument,
        "config": {
            "user_id": config.user_id,
            "exchange": config.exchange,
            "tradingsymbol": config.tradingsymbol,
            "side": config.side,
            "entry_date": config.entry_date,
            "from_date": config.from_date,
            "to_date": config.to_date,
            "interval": config.interval,
            "entry_price": round(entry_price, 4),
            "tick_size": tick_size,
            "atr_period": config.atr_period,
            "atr_multiple": config.atr_multiple,
            "swing_lookback": config.swing_lookback,
            "quantity": config.quantity,
        },
        "entry": {
            "entry_date": entry_candle["date"],
            "entry_price": round(entry_price, 4),
        },
        "exit": exit_info,
        "daily_log": daily_log,
    }


def print_summary(result: Dict) -> None:
    instrument = result["instrument"]
    config = result["config"]
    entry = result["entry"]
    exit_info = result["exit"]

    print("BACKTEST SUMMARY")
    print(f"Symbol       : {instrument['exchange']}:{instrument['tradingsymbol']}")
    print(f"Side         : {config['side']}")
    print(f"Interval     : {config['interval']}")
    print(f"Entry Date   : {entry['entry_date']}")
    print(f"Entry Price  : {entry['entry_price']:.2f}")
    print(f"Exit Date    : {exit_info['exit_date']}")
    print(f"Exit Price   : {exit_info['exit_price']:.2f}")
    print(f"Exit Reason  : {exit_info['exit_reason']}")
    print(f"PnL / Unit   : {exit_info['per_unit_pnl']:.2f}")
    print(f"Total PnL    : {exit_info['total_pnl']:.2f}")
    print(f"Future High  : {exit_info.get('future_high')}")
    print(f"Future Low   : {exit_info.get('future_low')}")
    print(f"Noise Exit   : {exit_info.get('noise_exit')}")
    print()

    print("DAILY TRACE")
    headers = [
        "Date",
        "Open",
        "High",
        "Low",
        "Close",
        "ATR",
        "RawStop",
        "Trigger",
        "Limit",
        "Exit?",
        "Reason",
    ]

    rows = []
    for row in result["daily_log"]:
        rows.append([
            str(row["date"]),
            f"{row['open']:.2f}",
            f"{row['high']:.2f}",
            f"{row['low']:.2f}",
            f"{row['close']:.2f}",
            f"{row['atr']:.2f}",
            f"{row['raw_stop']:.2f}",
            f"{row['trigger_price']:.2f}",
            f"{row['limit_price']:.2f}",
            "YES" if row["exit_triggered"] else "NO",
            row.get("exit_reason", ""),
        ])

    print_table(headers, rows)


def print_table(headers: List[str], rows: List[List[str]]) -> None:
    widths = [len(str(h)) for h in headers]
    for row in rows:
        for i, cell in enumerate(row):
            widths[i] = max(widths[i], len(str(cell)))

    def format_row(values: List[str]) -> str:
        return " | ".join(str(v).ljust(widths[i]) for i, v in enumerate(values))

    separator = "-+-".join("-" * w for w in widths)

    print(format_row(headers))
    print(separator)
    for row in rows:
        print(format_row(row))


def print_help() -> None:
    print("Usage:")
    print("  python backtest_kite_stoploss_engine.py <USER_ID> <EXCHANGE> <TRADINGSYMBOL> <SIDE> <ENTRY_DATE> <FROM_DATE> <TO_DATE> [INTERVAL] [ENTRY_PRICE] [QTY]")
    print("")
    print("Arguments:")
    print("  <USER_ID>       Zerodha user id, e.g. XJ1877")
    print("  <EXCHANGE>      NSE | NFO | BSE")
    print("  <TRADINGSYMBOL> e.g. INFY, TCS, RELIANCE, HDFCBANK26APRFUT")
    print("  <SIDE>          LONG | SHORT")
    print("  <ENTRY_DATE>    YYYY-MM-DD")
    print("  <FROM_DATE>     YYYY-MM-DD")
    print("  <TO_DATE>       YYYY-MM-DD")
    print("  [INTERVAL]      day | minute | 5minute | 15minute ... (default: day)")
    print("  [ENTRY_PRICE]   optional explicit entry price")
    print("  [QTY]           optional quantity (default: 1)")
    print("")
    print("Examples:")
    print("  python backtest_kite_stoploss_engine.py XJ1877 NSE INFY LONG 2025-01-10 2024-10-01 2025-03-31")
    print("  python backtest_kite_stoploss_engine.py XJ1877 NSE TCS LONG 2025-02-03 2024-11-01 2025-05-31 day 4125.50 125")
    print("  python backtest_kite_stoploss_engine.py XJ1877 NFO HDFCBANK26APRFUT LONG 2026-03-15 2025-12-01 2026-04-30 day 765.85 550")


if __name__ == "__main__":
    if len(sys.argv) < 8:
        print_help()
        sys.exit(1)

    user_id = sys.argv[1]
    exchange = sys.argv[2].upper()
    tradingsymbol = sys.argv[3].upper()
    side = sys.argv[4].upper()
    entry_date = sys.argv[5]
    from_date = sys.argv[6]
    to_date = sys.argv[7]
    interval = sys.argv[8] if len(sys.argv) >= 9 else "day"
    entry_price = float(sys.argv[9]) if len(sys.argv) >= 10 else None
    quantity = int(sys.argv[10]) if len(sys.argv) >= 11 else 1

    try:
        cfg = BacktestConfig(
            user_id=user_id,
            exchange=exchange,
            tradingsymbol=tradingsymbol,
            side=side,
            entry_date=entry_date,
            from_date=from_date,
            to_date=to_date,
            interval=interval,
            entry_price=entry_price,
            quantity=quantity,
        )
        result = backtest_stop_engine(cfg)
        print_summary(result)
    except Exception as exc:
        print(f"ERROR: {exc}")
        sys.exit(1)