import csv
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

from kiteconnect import KiteConnect

from kite_credentials_service import get_kite_credentials
from kite_stoploss_service_production import StopLossConfig, calculate_deterministic_stop


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


@dataclass(frozen=True)
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
    tick_size: Optional[float] = None
    quantity: int = 1
    lookahead_bars: int = 30
    stop_config: StopLossConfig = StopLossConfig()


def get_kite_client(user_id: str) -> KiteConnect:
    api_key, access_token = get_kite_credentials(user_id)
    kite = KiteConnect(api_key=api_key)
    kite.set_access_token(access_token)
    return kite


def normalize_date(value: str) -> datetime:
    return datetime.strptime(value, "%Y-%m-%d")


def candles_to_dicts(raw_candles: List[Dict]) -> List[Dict]:
    return [
        {
            "date": c["date"],
            "open": float(c["open"]),
            "high": float(c["high"]),
            "low": float(c["low"]),
            "close": float(c["close"]),
            "volume": c.get("volume"),
            "oi": c.get("oi"),
        }
        for c in raw_candles
    ]


def resolve_instrument(kite: KiteConnect, exchange: str, tradingsymbol: str) -> Dict:
    instruments = kite.instruments(exchange)
    matches = [i for i in instruments if i.get("tradingsymbol") == tradingsymbol]
    if not matches:
        raise ValueError(f"Instrument not found for {exchange}:{tradingsymbol}")

    inst = matches[0]
    return {
        "instrument_token": int(inst["instrument_token"]),
        "tradingsymbol": inst["tradingsymbol"],
        "exchange": inst["exchange"],
        "tick_size": float(inst.get("tick_size") or 0.05),
        "segment": inst.get("segment"),
        "instrument_type": inst.get("instrument_type"),
        "name": inst.get("name"),
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
        candle_date = candle["date"].date() if hasattr(candle["date"], "date") else candle["date"]
        if candle_date >= target:
            return i
    raise ValueError(f"No candle found on or after entry_date={entry_date}")


def compute_quote_like_snapshot(candle: Dict, tick_size: float) -> Tuple[float, float, float]:
    current_price = float(candle["close"])
    bid = max(tick_size, current_price - tick_size)
    ask = current_price + tick_size
    return current_price, bid, ask


def simulate_exit_price(
    side: str,
    candle: Dict,
    trigger_price: float,
    limit_price: float,
) -> Optional[Tuple[float, str]]:
    open_price = float(candle["open"])
    high = float(candle["high"])
    low = float(candle["low"])

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
    return per_unit, per_unit * quantity


def compute_noise_and_capture(
    side: str,
    candles: List[Dict],
    entry_index: int,
    exit_index: int,
    exit_price: float,
    entry_price: float,
    lookahead_bars: int,
) -> Dict:
    future_slice = candles[exit_index + 1 : exit_index + 1 + lookahead_bars]
    pre_exit_slice = candles[entry_index:exit_index] if exit_index > entry_index else [candles[entry_index]]

    future_high = max((float(c["high"]) for c in future_slice), default=None)
    future_low = min((float(c["low"]) for c in future_slice), default=None)
    max_favorable = max(float(c["high"]) for c in candles[entry_index:exit_index + 1]) if side == "LONG" else min(float(c["low"]) for c in candles[entry_index:exit_index + 1])

    noise_exit = False
    if side == "LONG":
        prev_high = max(float(c["high"]) for c in pre_exit_slice)
        if future_high is not None and future_high > prev_high:
            noise_exit = True
        denom = max_favorable - entry_price
        capture_ratio = ((exit_price - entry_price) / denom) if denom > 0 else 0.0
    else:
        prev_low = min(float(c["low"]) for c in pre_exit_slice)
        if future_low is not None and future_low < prev_low:
            noise_exit = True
        denom = entry_price - max_favorable
        capture_ratio = ((entry_price - exit_price) / denom) if denom > 0 else 0.0

    return {
        "future_high": round(future_high, 4) if future_high is not None else None,
        "future_low": round(future_low, 4) if future_low is not None else None,
        "noise_exit": noise_exit,
        "capture_ratio": round(capture_ratio, 4),
    }


def backtest_stop_engine(config: BacktestConfig) -> Dict:
    if config.side not in {"LONG", "SHORT"}:
        raise ValueError("side must be LONG or SHORT")

    kite = get_kite_client(config.user_id)
    instrument = resolve_instrument(kite, config.exchange, config.tradingsymbol)

    tick_size = config.tick_size or float(instrument["tick_size"])
    candles = fetch_historical_candles(
        kite=kite,
        instrument_token=instrument["instrument_token"],
        from_date=config.from_date,
        to_date=config.to_date,
        interval=config.interval,
    )

    entry_index = find_entry_index(candles, config.entry_date)
    min_history = max(
        config.stop_config.atr_period + config.stop_config.atr_average_window,
        config.stop_config.swing_lookback + 1,
    )
    if entry_index < min_history:
        raise ValueError(
            f"Not enough candle history before entry_date. Need at least {min_history} completed candles before entry."
        )

    entry_candle = candles[entry_index]
    entry_price = float(config.entry_price) if config.entry_price is not None else float(entry_candle["close"])
    previous_trigger_price: Optional[float] = None

    daily_log: List[Dict] = []
    exit_info: Optional[Dict] = None

    for i in range(entry_index + 1, len(candles)):
        history = candles[:i]
        current_candle = candles[i]
        current_price, bid, ask = compute_quote_like_snapshot(current_candle, tick_size)

        stop = calculate_deterministic_stop(
            candles=history,
            side=config.side,
            tick_size=tick_size,
            entry_price=entry_price,
            current_price=current_price,
            bid=bid,
            ask=ask,
            config=config.stop_config,
            previous_trigger_price=previous_trigger_price,
        )

        trigger_price = float(stop["trigger_price"])
        limit_price = float(stop["limit_price"])
        previous_trigger_price = trigger_price

        log_row = {
            "date": current_candle["date"],
            "open": float(current_candle["open"]),
            "high": float(current_candle["high"]),
            "low": float(current_candle["low"]),
            "close": float(current_candle["close"]),
            "atr": float(stop["atr"]),
            "atr_average": float(stop["atr_average"]),
            "multiplier": float(stop["multiplier"]),
            "raw_stop": float(stop["raw_stop"]),
            "trigger_price": trigger_price,
            "limit_price": limit_price,
            "exit_triggered": False,
            "exit_reason": "",
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
            post_exit = compute_noise_and_capture(
                side=config.side,
                candles=candles,
                entry_index=entry_index,
                exit_index=i,
                exit_price=exit_price,
                entry_price=entry_price,
                lookahead_bars=config.lookahead_bars,
            )
            exit_info = {
                "exit_date": current_candle["date"],
                "exit_price": round(exit_price, 4),
                "exit_reason": exit_reason,
                "per_unit_pnl": round(per_unit_pnl, 4),
                "total_pnl": round(total_pnl, 4),
                **post_exit,
            }
            log_row["exit_triggered"] = True
            log_row["exit_reason"] = exit_reason
            log_row["exit_price"] = round(exit_price, 4)
            daily_log.append(log_row)
            break

        daily_log.append(log_row)

    if exit_info is None:
        final_candle = candles[-1]
        exit_price = float(final_candle["close"])
        per_unit_pnl, total_pnl = compute_pnl(
            side=config.side,
            entry_price=entry_price,
            exit_price=exit_price,
            quantity=config.quantity,
        )
        exit_info = {
            "exit_date": final_candle["date"],
            "exit_price": round(exit_price, 4),
            "exit_reason": "NO_STOP_HIT_MARK_TO_MARKET",
            "per_unit_pnl": round(per_unit_pnl, 4),
            "total_pnl": round(total_pnl, 4),
            "future_high": None,
            "future_low": None,
            "noise_exit": None,
            "capture_ratio": None,
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
            "quantity": config.quantity,
            "lookahead_bars": config.lookahead_bars,
        },
        "entry": {
            "entry_date": entry_candle["date"],
            "entry_price": round(entry_price, 4),
        },
        "exit": exit_info,
        "daily_log": daily_log,
    }


def export_daily_log_csv(path: str, daily_log: List[Dict]) -> None:
    if not daily_log:
        return
    fieldnames = list(daily_log[0].keys())
    with open(path, "w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(daily_log)


def print_table(headers: List[str], rows: List[List[str]]) -> None:
    widths = [len(str(h)) for h in headers]
    for row in rows:
        for i, cell in enumerate(row):
            widths[i] = max(widths[i], len(str(cell)))

    def fmt(values: List[str]) -> str:
        return " | ".join(str(v).ljust(widths[i]) for i, v in enumerate(values))

    separator = "-+-".join("-" * w for w in widths)
    print(fmt(headers))
    print(separator)
    for row in rows:
        print(fmt(row))


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
    print(f"Capture Ratio: {exit_info.get('capture_ratio')}")
    print()

    headers = [
        "Date",
        "Open",
        "High",
        "Low",
        "Close",
        "ATR",
        "ATRAvg",
        "Mult",
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
            f"{row['atr_average']:.2f}",
            f"{row['multiplier']:.2f}",
            f"{row['raw_stop']:.2f}",
            f"{row['trigger_price']:.2f}",
            f"{row['limit_price']:.2f}",
            "YES" if row["exit_triggered"] else "NO",
            row.get("exit_reason", ""),
        ])
    print("DAILY TRACE")
    print_table(headers, rows)


def print_help() -> None:
    print("Usage:")
    print("  python backtest_kite_stoploss_engine_production.py <USER_ID> <EXCHANGE> <TRADINGSYMBOL> <SIDE> <ENTRY_DATE> <FROM_DATE> <TO_DATE> [INTERVAL] [ENTRY_PRICE] [QTY] [LOOKAHEAD_BARS] [CSV_PATH]")
    print("")
    print("Examples:")
    print("  python backtest_kite_stoploss_engine_production.py XJ1877 NSE INFY LONG 2025-01-10 2024-10-01 2025-03-31")
    print("  python backtest_kite_stoploss_engine_production.py XJ1877 NSE BSE LONG 2025-06-01 2025-04-01 2026-03-31 day 2693.30 1 30 bse_backtest.csv")
    print("  python backtest_kite_stoploss_engine_production.py XJ1877 NFO HDFCBANK26APRFUT LONG 2026-03-15 2025-12-01 2026-04-30 day 765.85 550")


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
    lookahead_bars = int(sys.argv[11]) if len(sys.argv) >= 12 else 30
    csv_path = sys.argv[12] if len(sys.argv) >= 13 else ""

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
            lookahead_bars=lookahead_bars,
        )
        result = backtest_stop_engine(cfg)
        print_summary(result)
        if csv_path:
            export_daily_log_csv(csv_path, result["daily_log"])
            print(f"\nDaily log exported to: {csv_path}")
    except Exception as exc:
        print(f"ERROR: {exc}")
        sys.exit(1)
