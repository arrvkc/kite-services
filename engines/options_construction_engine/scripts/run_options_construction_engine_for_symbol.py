from __future__ import annotations

import argparse
import csv
import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

from engines.options_construction_engine.engine import OptionsConstructionEngine
from engines.options_construction_engine.kite_adapter import KiteOptionChainAdapter, get_kite_client
from engines.options_construction_engine.models import OptionsConstructionConfig


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Construct deterministic option legs from either a Strategy Engine JSON payload, "
            "a direct symbol, a symbol list, or the F&O universe."
        )
    )
    parser.add_argument("user_id")
    parser.add_argument(
        "strategy_payload_json_or_symbol",
        nargs="?",
        help=(
            "Path to Strategy Engine public payload JSON/wrapper JSON, symbol when --from-symbol is used, "
            "or ignored when --fo-universe / --symbols-file is used."
        ),
    )
    parser.add_argument(
        "--from-symbol",
        action="store_true",
        help="Treat the second positional argument as a symbol and build Strategy Engine payload internally.",
    )
    parser.add_argument(
        "--fo-universe",
        action="store_true",
        help="Run for all symbols having NFO option contracts. Use --limit for a controlled trial.",
    )
    parser.add_argument(
        "--symbols",
        default=None,
        help="Comma-separated symbols to run in bulk, e.g. NIFTY,RELIANCE,IRFC.",
    )
    parser.add_argument(
        "--symbols-file",
        default=None,
        help="Text file containing one symbol per line for bulk mode.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Limit number of symbols in bulk mode.",
    )
    parser.add_argument(
        "--audit-log-path",
        default=None,
        help="Audit path for single-symbol/JSON mode. In bulk mode, per-symbol timestamped audit paths are auto-created.",
    )
    parser.add_argument("--reference-fixture-compatibility", action="store_true")
    parser.add_argument(
        "--liquidity-mode",
        choices=["LIVE_STRICT", "AFTER_HOURS_HISTORICAL"],
        default="LIVE_STRICT",
        help=(
            "LIVE_STRICT uses live Kite bid/ask and may be execution-ready. "
            "AFTER_HOURS_HISTORICAL uses Kite historical last volume-positive candle prices "
            "and always returns execution_ready=false."
        ),
    )
    parser.add_argument(
        "--save-strategy-json",
        default=None,
        help="Optional path to save internally generated Strategy Engine JSON in single --from-symbol mode.",
    )
    parser.add_argument(
        "--output",
        choices=["json", "table"],
        default="table",
        help="table prints aligned comparison rows; json prints full wrapper output in single mode only.",
    )
    parser.add_argument(
        "--no-summary-csv",
        action="store_true",
        help="Disable automatic append to daily CSV summary.",
    )
    parser.add_argument(
        "--no-header",
        action="store_true",
        help="Do not print table header.",
    )
    parser.add_argument(
        "--show-candidates",
        action="store_true",
        help="Show all scored candidates in the same full table format as the selected result.",
    )
    return parser


def _price_source_for_mode(liquidity_mode: str) -> str:
    if liquidity_mode == "AFTER_HOURS_HISTORICAL":
        return "HISTORICAL_LAST_VOLUME_POSITIVE_CANDLE_CLOSE"
    return "LIVE_BID_ASK"


def _execution_note_for_mode(liquidity_mode: str) -> str:
    if liquidity_mode == "AFTER_HOURS_HISTORICAL":
        return (
            "Non-production historical test mode. Prices are historical candle-close proxies; "
            "execution_ready is always false."
        )
    return "Live strict mode. Uses current Kite bid/ask liquidity checks."


def _safe_symbol(symbol: str) -> str:
    return symbol.upper().replace("/", "_").replace(" ", "_")


def _default_audit_path(symbol: str, liquidity_mode: str) -> str:
    now = datetime.now()
    day = now.strftime("%Y-%m-%d")
    stamp = now.strftime("%Y%m%d_%H%M%S")
    return str(
        Path("services/audit_logs/options_construction")
        / day
        / f"{_safe_symbol(symbol)}_{liquidity_mode}_{stamp}.json"
    )


def _summary_csv_path() -> Path:
    day = datetime.now().strftime("%Y-%m-%d")
    return Path("services/audit_logs/options_construction") / f"summary_{day}.csv"


def _bulk_csv_path() -> Path:
    day = datetime.now().strftime("%Y-%m-%d")
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return Path("services/audit_logs/options_construction") / f"bulk_{day}_{stamp}.csv"


def _resolve_spot_symbol(symbol: str) -> str:
    symbol_upper = symbol.upper()
    if symbol_upper == "NIFTY":
        return "NSE:NIFTY 50"
    if symbol_upper == "BANKNIFTY":
        return "NSE:NIFTY BANK"
    if symbol_upper == "FINNIFTY":
        return "NSE:NIFTY FIN SERVICE"
    if symbol_upper == "MIDCPNIFTY":
        return "NSE:NIFTY MID SELECT"
    return f"NSE:{symbol_upper}"


def _get_fo_option_universe(kite: Any) -> list[str]:
    """Return sorted symbols with listed CE/PE instruments in NFO.

    This fetches instruments once for universe discovery.
    """
    instruments = kite.instruments("NFO")
    symbols = {
        str(inst.get("name", "")).upper()
        for inst in instruments
        if inst.get("name")
        and inst.get("instrument_type") in {"CE", "PE"}
    }
    return sorted(symbols)


def _read_symbols_from_file(path: str) -> list[str]:
    symbols: list[str] = []
    for line in Path(path).read_text(encoding="utf-8").splitlines():
        text = line.strip()
        if not text or text.startswith("#"):
            continue
        symbols.append(text.upper())
    return symbols


def _symbols_from_args(args: argparse.Namespace, kite: Any) -> list[str]:
    symbols: list[str] = []

    if args.fo_universe:
        symbols.extend(_get_fo_option_universe(kite))

    if args.symbols:
        symbols.extend([s.strip().upper() for s in args.symbols.split(",") if s.strip()])

    if args.symbols_file:
        symbols.extend(_read_symbols_from_file(args.symbols_file))

    # deterministic de-dup preserving sorted order for universe-like comparison
    symbols = sorted(set(symbols))

    if args.limit is not None:
        symbols = symbols[: max(args.limit, 0)]

    return symbols


def _build_strategy_result_from_symbol(kite: Any, symbol: str) -> dict[str, Any]:
    from engines.strategy_deterministic_engine.adapters.trend_identifier_adapter import TrendIdentifierKiteAdapter
    from engines.strategy_deterministic_engine.engine import evaluate_strategy_engine

    symbol_upper = symbol.upper()

    adapter = TrendIdentifierKiteAdapter(kite=kite)
    strategy_input = adapter.build_strategy_input_for_symbol(symbol_upper)

    strategy_payload = {
        "instrument": strategy_input.instrument,
        "asof_time": strategy_input.latest_payload.asof_time,
        "label": strategy_input.latest_payload.label,
        "confidence": strategy_input.latest_payload.confidence,
        "aggregate_score": strategy_input.latest_payload.aggregate_score,
        "internal_state": strategy_input.latest_payload.internal_state,
        "trend_history_w5": [
            {
                "label": row.label,
                "confidence": row.confidence,
                "aggregate_score": row.aggregate_score,
            }
            for row in strategy_input.trend_history_w5
        ],
        "dte_near_month": strategy_input.dte_near_month,
        "next_month_available": strategy_input.next_month_available,
        "dte_next_month": strategy_input.dte_next_month,
        "in_universe": strategy_input.in_universe,
        "prior_committed_state": None,
    }

    result = evaluate_strategy_engine(strategy_payload)
    payload = result["payload"]

    nfo = kite.instruments("NFO")
    opts = [
        inst
        for inst in nfo
        if str(inst.get("name", "")).upper() == symbol_upper
        and inst.get("instrument_type") in {"CE", "PE"}
    ]
    if not opts:
        raise RuntimeError(f"No NFO option instruments found for symbol: {symbol_upper}")

    strikes = sorted({float(inst["strike"]) for inst in opts if float(inst.get("strike") or 0) > 0})
    if len(strikes) < 2:
        raise RuntimeError(f"Not enough listed strikes to infer strike_step for symbol: {symbol_upper}")

    strike_steps = [b - a for a, b in zip(strikes, strikes[1:]) if b > a]
    strike_step = min(strike_steps)
    lot_size = int(opts[0]["lot_size"])

    spot_key = _resolve_spot_symbol(symbol_upper)
    spot_quote = kite.ltp([spot_key])
    spot = spot_quote[spot_key]["last_price"]

    payload["underlying_spot_price"] = float(spot)
    payload["lot_size"] = lot_size
    payload["strike_step"] = float(strike_step)

    return result


def _load_strategy_result(path: str) -> dict[str, Any]:
    with open(path, "r", encoding="utf-8") as fh:
        return json.load(fh)


def _money(value: Any, decimals: int = 2) -> str:
    if value is None:
        return "-"
    try:
        return f"{float(value):.{decimals}f}"
    except (TypeError, ValueError):
        return str(value)


def _pct(value: Any) -> str:
    if value is None:
        return "-"
    try:
        return f"{100 * float(value):.2f}%"
    except (TypeError, ValueError):
        return str(value)


def _display_strike(value: Any) -> str:
    strike = float(value)
    return str(int(strike)) if strike.is_integer() else str(strike)


def _spread_and_width(result: dict[str, Any]) -> tuple[str, float | None]:
    legs = result.get("legs") or []
    if len(legs) < 2:
        return "-", None

    # Iron Condor: show both PE side and CE side.
    role_map = {leg.get("role"): leg for leg in legs}
    if {"PUT_LONG_WING", "PUT_SHORT", "CALL_SHORT", "CALL_LONG_WING"}.issubset(role_map):
        pl = role_map["PUT_LONG_WING"]
        ps = role_map["PUT_SHORT"]
        cs = role_map["CALL_SHORT"]
        cl = role_map["CALL_LONG_WING"]

        put_width = abs(float(ps["strike"]) - float(pl["strike"]))
        call_width = abs(float(cl["strike"]) - float(cs["strike"]))
        width = max(put_width, call_width)

        spread = (
            f"B {_display_strike(pl['strike'])}PE/S {_display_strike(ps['strike'])}PE "
            f"+ S {_display_strike(cs['strike'])}CE/B {_display_strike(cl['strike'])}CE"
        )
        return spread, width

    short_leg = next((leg for leg in legs if leg.get("role") == "SHORT_LEG"), legs[0])
    long_leg = next((leg for leg in legs if leg.get("role") == "LONG_LEG"), legs[1])

    short_strike = float(short_leg["strike"])
    long_strike = float(long_leg["strike"])
    width = abs(long_strike - short_strike)

    spread = (
        f"S {_display_strike(short_strike)}{short_leg['option_type']} "
        f"/ B {_display_strike(long_strike)}{long_leg['option_type']}"
    )
    return spread, width


def _derived_metrics(result: dict[str, Any]) -> dict[str, Any]:
    max_profit = result.get("max_profit_per_lot")
    max_loss = result.get("max_loss_per_lot")
    net_premium = result.get("net_premium")
    spread, width = _spread_and_width(result)

    rr = None
    roi = None
    credit_pct_width = None

    if max_profit is not None and max_loss not in (None, 0):
        rr = float(max_profit) / float(max_loss)
        roi = rr

    if net_premium is not None and width not in (None, 0):
        credit_pct_width = float(net_premium) / float(width)

    return {
        "spread": spread,
        "width": width,
        "rr": rr,
        "roi": roi,
        "credit_pct_width": credit_pct_width,
    }


def _table_header() -> str:
    return (
        f"{'SYMBOL':<12} "
        f"{'STRATEGY':<20} "
        f"{'STATUS':<12} "
        f"{'EXPIRY':<12} "
        f"{'SPREAD':<26} "
        f"{'WIDTH':>8} "
        f"{'NET':>8} "
        f"{'CR/W':>8} "
        f"{'PROFIT':>11} "
        f"{'LOSS':>11} "
        f"{'ROI':>9} "
        f"{'RR':>7} "
        f"{'SCORE':>6} "
        f"{'EXEC':>6} "
        f"{'MODE':<24} "
        f"{'ERROR':<24}"
    )


def _table_row(wrapped_output: dict[str, Any]) -> str:
    ctx = wrapped_output["run_context"]
    result = wrapped_output["construction_result"]
    metrics = _derived_metrics(result)
    error_text = ";".join(e.get("code", "") for e in result.get("errors", [])[:2])

    return (
        f"{str(ctx['symbol']):<12} "
        f"{str(result.get('strategy_family', '-')):<20} "
        f"{str(result.get('construction_status', '-')):<12} "
        f"{str(result.get('expiry', '-')):<12} "
        f"{metrics['spread']:<26} "
        f"{_money(metrics['width']):>8} "
        f"{_money(result.get('net_premium')):>8} "
        f"{_pct(metrics['credit_pct_width']):>8} "
        f"{_money(result.get('max_profit_per_lot')):>11} "
        f"{_money(result.get('max_loss_per_lot')):>11} "
        f"{_pct(metrics['roi']):>9} "
        f"{_money(metrics['rr']):>7} "
        f"{str(result.get('construction_score', '-')):>6} "
        f"{str(result.get('execution_ready', '-')):>6} "
        f"{str(ctx['liquidity_mode']):<24} "
        f"{error_text:<24}"
    )


def _candidate_table_row(
    wrapped_output: dict[str, Any],
    candidate: dict[str, Any],
    selected_id: str | None,
) -> str:
    ctx = wrapped_output["run_context"]
    result = wrapped_output["construction_result"]
    legs = candidate.get("legs") or []

    temp_result = {"legs": legs}
    spread, width = _spread_and_width(temp_result)

    econ = candidate.get("economics") or {}
    net_premium = econ.get("net_premium")
    max_profit = econ.get("max_profit_per_lot")
    max_loss = econ.get("max_loss_per_lot")
    rr = econ.get("reward_risk_ratio")
    roi = rr
    credit_pct_width = None
    if net_premium is not None and width not in (None, 0):
        credit_pct_width = float(net_premium) / float(width)

    status = "SELECTED" if candidate.get("candidate_id") == selected_id else "SCORED"

    return (
        f"{str(ctx['symbol']):<12} "
        f"{str(result.get('strategy_family', '-')):<20} "
        f"{status:<12} "
        f"{str(result.get('expiry', '-')):<12} "
        f"{spread:<26} "
        f"{_money(width):>8} "
        f"{_money(net_premium):>8} "
        f"{_pct(credit_pct_width):>8} "
        f"{_money(max_profit):>11} "
        f"{_money(max_loss):>11} "
        f"{_pct(roi):>9} "
        f"{_money(rr):>7} "
        f"{str(candidate.get('construction_score', '-')):>6} "
        f"{str(result.get('execution_ready', '-')):>6} "
        f"{str(ctx['liquidity_mode']):<24} "
        f"{'':<24}"
    )


def _candidate_rows_from_audit(wrapped_output: dict[str, Any]) -> list[str]:
    audit_path = wrapped_output["run_context"].get("audit_log_path")
    if not audit_path:
        return []

    path = Path(audit_path)
    if not path.exists():
        return []

    audit = json.loads(path.read_text(encoding="utf-8"))
    scored = audit.get("candidate_lists", {}).get("scored", [])
    selected_id = (audit.get("final_selection") or {}).get("candidate_id")

    return [
        _candidate_table_row(wrapped_output, candidate, selected_id)
        for candidate in sorted(
            scored,
            key=lambda c: (
                c.get("candidate_id") != selected_id,
                -int(c.get("construction_score") or 0),
                c.get("candidate_id") or "",
            ),
        )
    ]



def _csv_row(wrapped_output: dict[str, Any]) -> list[Any]:
    ctx = wrapped_output["run_context"]
    strategy = wrapped_output["strategy_result"].get("payload", {})
    result = wrapped_output["construction_result"]
    metrics = _derived_metrics(result)

    return [
        datetime.now().isoformat(timespec="seconds"),
        ctx["symbol"],
        result.get("strategy_family"),
        strategy.get("final_strategy_strength"),
        result.get("construction_status"),
        result.get("expiry"),
        metrics["spread"],
        metrics["width"],
        result.get("net_premium"),
        metrics["credit_pct_width"],
        result.get("max_profit_per_lot"),
        result.get("max_loss_per_lot"),
        metrics["roi"],
        metrics["rr"],
        result.get("construction_score"),
        result.get("execution_ready"),
        ctx["liquidity_mode"],
        ctx["price_source"],
        ctx["audit_log_path"],
        ",".join(result.get("reason_codes", [])),
        ";".join(e.get("code", "") for e in result.get("errors", [])),
    ]


def _csv_header() -> list[str]:
    return [
        "run_timestamp",
        "symbol",
        "strategy_family",
        "strategy_strength",
        "construction_status",
        "expiry",
        "spread",
        "width",
        "net_premium",
        "credit_pct_width",
        "max_profit_per_lot",
        "max_loss_per_lot",
        "roi_on_risk",
        "reward_risk_ratio",
        "construction_score",
        "execution_ready",
        "liquidity_mode",
        "price_source",
        "audit_log_path",
        "reason_codes",
        "error_codes",
    ]


def _append_summary_csv(wrapped_output: dict[str, Any]) -> Path:
    path = _summary_csv_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    file_exists = path.exists()

    with open(path, "a", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        if not file_exists:
            writer.writerow(_csv_header())
        writer.writerow(_csv_row(wrapped_output))

    return path


def _write_bulk_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        writer.writerow(_csv_header())
        for row in rows:
            writer.writerow(_csv_row(row))


def _error_wrapped_output(args: argparse.Namespace, symbol: str, exc: Exception) -> dict[str, Any]:
    message = str(exc)
    return {
        "run_context": {
            "user_id": args.user_id,
            "input_type": "SYMBOL",
            "symbol": symbol.upper(),
            "liquidity_mode": args.liquidity_mode,
            "price_source": _price_source_for_mode(args.liquidity_mode),
            "execution_note": _execution_note_for_mode(args.liquidity_mode),
            "audit_log_path": None,
            "saved_strategy_json": None,
        },
        "strategy_result": {"payload": {"instrument": symbol.upper(), "final_strategy_strength": None}},
        "construction_result": {
            "instrument": symbol.upper(),
            "strategy_family": "-",
            "construction_status": "ERROR",
            "expiry": None,
            "legs": [],
            "net_premium": None,
            "max_profit_per_lot": None,
            "max_loss_per_lot": None,
            "construction_score": None,
            "execution_ready": False,
            "reason_codes": [],
            "errors": [{"code": "SCRIPT_ERROR", "message": message}],
        },
    }


def _run_symbol(
    args: argparse.Namespace,
    kite: Any,
    symbol: str,
    *,
    save_strategy_json: str | None = None,
) -> dict[str, Any]:
    strategy_result = _build_strategy_result_from_symbol(kite, symbol)
    payload = strategy_result.get("payload", strategy_result)

    if save_strategy_json:
        save_path = Path(save_strategy_json)
        save_path.parent.mkdir(parents=True, exist_ok=True)
        save_path.write_text(json.dumps(strategy_result, indent=2, sort_keys=True), encoding="utf-8")

    audit_log_path = args.audit_log_path or _default_audit_path(payload["instrument"], args.liquidity_mode)

    chain = KiteOptionChainAdapter(
        kite,
        liquidity_mode=args.liquidity_mode,
    ).build_option_chain(payload["instrument"], asof_time=payload["asof_time"])

    engine = OptionsConstructionEngine(OptionsConstructionConfig(
        audit_log_path=audit_log_path,
        reference_fixture_compatibility=args.reference_fixture_compatibility,
        liquidity_mode=args.liquidity_mode,
    ))
    construction_result = engine.construct(strategy_result, chain)

    return {
        "run_context": {
            "user_id": args.user_id,
            "input_type": "SYMBOL",
            "symbol": payload["instrument"],
            "liquidity_mode": args.liquidity_mode,
            "price_source": _price_source_for_mode(args.liquidity_mode),
            "execution_note": _execution_note_for_mode(args.liquidity_mode),
            "audit_log_path": audit_log_path,
            "saved_strategy_json": save_strategy_json,
        },
        "strategy_result": strategy_result,
        "construction_result": construction_result,
    }


def _run_json(args: argparse.Namespace, kite: Any, path: str) -> dict[str, Any]:
    strategy_result = _load_strategy_result(path)
    payload = strategy_result.get("payload", strategy_result)

    audit_log_path = args.audit_log_path or _default_audit_path(payload["instrument"], args.liquidity_mode)

    chain = KiteOptionChainAdapter(
        kite,
        liquidity_mode=args.liquidity_mode,
    ).build_option_chain(payload["instrument"], asof_time=payload["asof_time"])

    engine = OptionsConstructionEngine(OptionsConstructionConfig(
        audit_log_path=audit_log_path,
        reference_fixture_compatibility=args.reference_fixture_compatibility,
        liquidity_mode=args.liquidity_mode,
    ))
    construction_result = engine.construct(strategy_result, chain)

    return {
        "run_context": {
            "user_id": args.user_id,
            "input_type": "JSON",
            "symbol": payload["instrument"],
            "liquidity_mode": args.liquidity_mode,
            "price_source": _price_source_for_mode(args.liquidity_mode),
            "execution_note": _execution_note_for_mode(args.liquidity_mode),
            "audit_log_path": audit_log_path,
            "saved_strategy_json": None,
        },
        "strategy_result": strategy_result,
        "construction_result": construction_result,
    }


def _run_bulk(args: argparse.Namespace, kite: Any) -> int:
    symbols = _symbols_from_args(args, kite)
    if not symbols:
        raise RuntimeError("No symbols found. Use --fo-universe, --symbols, or --symbols-file.")

    rows: list[dict[str, Any]] = []
    if args.output == "table" and not args.no_header:
        print(_table_header())

    for index, symbol in enumerate(symbols, start=1):
        try:
            wrapped_output = _run_symbol(args, kite, symbol)
        except Exception as exc:
            wrapped_output = _error_wrapped_output(args, symbol, exc)

        rows.append(wrapped_output)

        if not args.no_summary_csv:
            _append_summary_csv(wrapped_output)

        if args.output == "table":
            if args.show_candidates:
                candidate_rows = _candidate_rows_from_audit(wrapped_output)
                if candidate_rows:
                    for row in candidate_rows:
                        print(row)
                else:
                    print(_table_row(wrapped_output))
            else:
                print(_table_row(wrapped_output))
        else:
            print(json.dumps(wrapped_output, indent=2, sort_keys=True))

        print(f"# {index}/{len(symbols)} {symbol}", file=sys.stderr)

    bulk_csv = _bulk_csv_path()
    _write_bulk_csv(bulk_csv, rows)
    print(f"\nBULK_CSV: {bulk_csv}")
    return 0


def main() -> None:
    args = build_parser().parse_args()
    kite = get_kite_client(args.user_id)

    bulk_requested = args.fo_universe or args.symbols or args.symbols_file
    if bulk_requested:
        raise SystemExit(_run_bulk(args, kite))

    if args.from_symbol:
        if not args.strategy_payload_json_or_symbol:
            raise SystemExit("symbol is required when --from-symbol is used")
        wrapped_output = _run_symbol(
            args,
            kite,
            args.strategy_payload_json_or_symbol.upper(),
            save_strategy_json=args.save_strategy_json,
        )
    else:
        if not args.strategy_payload_json_or_symbol:
            raise SystemExit("strategy payload JSON path is required unless --from-symbol/--fo-universe/--symbols is used")
        wrapped_output = _run_json(args, kite, args.strategy_payload_json_or_symbol)

    summary_path = None
    if not args.no_summary_csv:
        summary_path = _append_summary_csv(wrapped_output)

    if args.output == "json":
        if summary_path is not None:
            wrapped_output["run_context"]["summary_csv_path"] = str(summary_path)
        print(json.dumps(wrapped_output, indent=2, sort_keys=True))
    else:
        if not args.no_header:
            print(_table_header())
        if args.show_candidates:
            candidate_rows = _candidate_rows_from_audit(wrapped_output)
            if candidate_rows:
                for row in candidate_rows:
                    print(row)
            else:
                print(_table_row(wrapped_output))
        else:
            print(_table_row(wrapped_output))
        if summary_path is not None:
            print(f"\nCSV: {summary_path}")


if __name__ == "__main__":
    main()

