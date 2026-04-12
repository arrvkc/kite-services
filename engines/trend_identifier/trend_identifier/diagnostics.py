"""Diagnostic packaging."""

from __future__ import annotations

from .enums import ReasonCode
from .types import ConflictFlags, DiagnosticsBundle, FeatureBundle, VetoFlags


def package_diagnostics(features: FeatureBundle, vetoes: VetoFlags, conflicts: ConflictFlags, transition_state: str, extra_reason_codes: list[str] | None = None) -> dict:
    # SPEC TRACE: Section 12 - package_diagnostics must be lossless for load-bearing fields
    reason_codes = list(features.reason_codes)
    extra_reason_codes = extra_reason_codes or []
    reason_codes.extend(extra_reason_codes)

    if conflicts.major:
        reason_codes.append(ReasonCode.CONFLICT_MAJOR.value)
    if conflicts.score:
        reason_codes.append(ReasonCode.CONFLICT_SCORE.value)
    if conflicts.quality:
        reason_codes.append(ReasonCode.CONFLICT_QUALITY.value)
    if vetoes.shock:
        reason_codes.append(ReasonCode.VETO_SHOCK.value)
    if vetoes.volatility:
        reason_codes.append(ReasonCode.VETO_VOLATILITY.value)
    if vetoes.anomaly:
        reason_codes.append(ReasonCode.VETO_ANOMALY.value)
    if vetoes.liquidity:
        reason_codes.append(ReasonCode.VETO_LIQUIDITY.value)
    if vetoes.roll:
        reason_codes.append(ReasonCode.VETO_ROLL.value)

    return {
        "acceptance_partial": bool(features.weekly.acceptance_partial or features.daily.acceptance_partial or features.hourly.acceptance_partial),
        "missing_bars_adjusted": bool(features.diagnostics.get("missing_bars_adjusted", False)),
        "pivot_tie": bool(features.weekly.pivot_tie or features.daily.pivot_tie or features.hourly.pivot_tie),
        "hourly_deterioration": bool(features.hourly.breakout_sign == 0 and abs(features.hourly.gap or 0.0) > 0),
        "conflicts": {
            "major": conflicts.major,
            "score": conflicts.score,
            "quality": conflicts.quality,
        },
        "reason_codes": list(dict.fromkeys(reason_codes)),
    }
