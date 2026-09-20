"""Read and validate official NSE UDiFF final-close evidence.

The provider deliberately has no broker or database dependency.  It downloads
the official final capital-market and derivatives bhavcopy archives once for a
session, validates their identity, and exposes exact instrument matches to the
governed historical acquisition pipeline.
"""

from __future__ import annotations

import csv
from dataclasses import dataclass
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation
import hashlib
import io
import zipfile

import requests


NSE_ARCHIVE_ROOT = "https://nsearchives.nseindia.com/content"
_ARCHIVE_TEMPLATE = "BhavCopy_NSE_{segment}_0_0_0_{session}_F_0000.csv.zip"
_MAX_ARCHIVE_BYTES = 25 * 1024 * 1024
_REQUIRED_COLUMNS = {
    "TradDt",
    "BizDt",
    "Sgmt",
    "Src",
    "FinInstrmTp",
    "TckrSymb",
    "SctySrs",
    "FinInstrmNm",
    "ClsPric",
}


class NseFinalCloseUnavailable(RuntimeError):
    """Official final evidence is not available or cannot be validated."""


class NseFinalCloseNotPublished(NseFinalCloseUnavailable):
    """The exact official archive does not exist yet for the requested date."""


class NseFinalCloseIdentityError(RuntimeError):
    """Official evidence has a missing or ambiguous instrument identity."""


@dataclass(frozen=True)
class OfficialFinalCloseEvidence:
    session_date: date
    exchange: str
    tradingsymbol: str
    instrument_kind: str
    close: Decimal
    source_name: str
    source_sha256: str
    retrieved_at: datetime


def _decimal(value: object, *, field: str) -> Decimal:
    try:
        result = Decimal(str(value).strip())
    except (InvalidOperation, ValueError) as exc:
        raise NseFinalCloseUnavailable(
            f"Official NSE final evidence contains an invalid {field}."
        ) from exc
    if not result.is_finite() or result <= 0:
        raise NseFinalCloseUnavailable(
            f"Official NSE final evidence contains an invalid {field}."
        )
    return result


def closes_match(zerodha_close: object, official_close: Decimal) -> bool:
    """Compare rupee closes at their published two-decimal price precision."""
    candidate = _decimal(zerodha_close, field="Zerodha close")
    quantum = Decimal("0.01")
    return candidate.quantize(quantum) == official_close.quantize(quantum)


class NseFinalCloseCatalog:
    def __init__(
        self,
        *,
        session_date: date,
        rows_by_segment: dict[str, list[dict[str, str]]],
        archive_hashes: dict[str, str],
        archive_names: dict[str, str],
        retrieved_at: datetime,
    ) -> None:
        self.session_date = session_date
        self._rows_by_segment = rows_by_segment
        self._archive_hashes = archive_hashes
        self._archive_names = archive_names
        self.retrieved_at = retrieved_at

    @property
    def source_name(self) -> str:
        return "+".join(
            self._archive_names[segment] for segment in sorted(self._archive_names)
        )

    @property
    def source_sha256(self) -> str:
        material = "|".join(
            f"{segment}:{self._archive_hashes[segment]}"
            for segment in sorted(self._archive_hashes)
        )
        return hashlib.sha256(material.encode("ascii")).hexdigest()

    def resolve(self, instrument: dict, target_date: date) -> OfficialFinalCloseEvidence:
        if target_date != self.session_date:
            raise NseFinalCloseIdentityError(
                "Official NSE final evidence does not match the target session."
            )
        exchange = str(instrument.get("resolved_exchange") or "").strip().upper()
        tradingsymbol = str(
            instrument.get("resolved_tradingsymbol") or ""
        ).strip().upper()
        instrument_type = str(
            instrument.get("kite_instrument_type") or ""
        ).strip().upper()
        if exchange == "NSE" and instrument_type == "EQ":
            segment = "CM"
            matches = [
                row
                for row in self._rows_by_segment[segment]
                if row["TckrSymb"].strip().upper() == tradingsymbol
                and row["SctySrs"].strip().upper() == "EQ"
                and row["FinInstrmTp"].strip().upper() == "STK"
            ]
            kind = "NSE_CM_EQ_FINAL"
        elif exchange == "NFO" and instrument_type == "FUT":
            segment = "FO"
            matches = [
                row
                for row in self._rows_by_segment[segment]
                if row["FinInstrmNm"].strip().upper() == tradingsymbol
                and row["FinInstrmTp"].strip().upper() in {"IDF", "STF"}
            ]
            kind = "NSE_FO_FUT_FINAL"
        else:
            raise NseFinalCloseIdentityError(
                "The selected instrument has no governed NSE final-close contract."
            )

        if len(matches) != 1:
            raise NseFinalCloseIdentityError(
                "Official NSE final evidence has a missing or ambiguous instrument row."
            )
        row = matches[0]
        return OfficialFinalCloseEvidence(
            session_date=target_date,
            exchange=exchange,
            tradingsymbol=tradingsymbol,
            instrument_kind=kind,
            close=_decimal(row["ClsPric"], field="close"),
            source_name=self._archive_names[segment],
            source_sha256=self._archive_hashes[segment],
            retrieved_at=self.retrieved_at,
        )


class NseFinalCloseProvider:
    """Acquire official final evidence without using time as a finality proxy."""

    def __init__(self, session: requests.Session | None = None, timeout: int = 30) -> None:
        self._session = session or requests.Session()
        self._timeout = timeout

    @staticmethod
    def archive_name(segment: str, session_date: date) -> str:
        return _ARCHIVE_TEMPLATE.format(
            segment=segment,
            session=session_date.strftime("%Y%m%d"),
        )

    def _fetch_segment(self, segment: str, session_date: date) -> tuple[list[dict[str, str]], str, str]:
        archive_name = self.archive_name(segment, session_date)
        url = f"{NSE_ARCHIVE_ROOT}/{segment.lower()}/{archive_name}"
        try:
            response = self._session.get(
                url,
                headers={
                    "User-Agent": "EAJEE governed historical finality/1.0",
                    "Accept": "application/zip,application/octet-stream",
                },
                timeout=self._timeout,
            )
            if response.status_code == 404:
                raise NseFinalCloseNotPublished(
                    f"Official NSE {segment} final evidence is not published."
                )
            response.raise_for_status()
        except NseFinalCloseNotPublished:
            raise
        except requests.RequestException as exc:
            raise NseFinalCloseUnavailable(
                f"Official NSE {segment} final evidence is unavailable."
            ) from exc
        content = response.content
        if not content or len(content) > _MAX_ARCHIVE_BYTES:
            raise NseFinalCloseUnavailable(
                f"Official NSE {segment} final evidence has an invalid size."
            )
        expected_csv = archive_name.removesuffix(".zip")
        try:
            with zipfile.ZipFile(io.BytesIO(content)) as archive:
                members = [name for name in archive.namelist() if not name.endswith("/")]
                if members != [expected_csv]:
                    raise NseFinalCloseUnavailable(
                        f"Official NSE {segment} final archive identity is invalid."
                    )
                csv_bytes = archive.read(expected_csv)
        except (zipfile.BadZipFile, KeyError) as exc:
            raise NseFinalCloseUnavailable(
                f"Official NSE {segment} final archive is invalid."
            ) from exc
        try:
            text = csv_bytes.decode("utf-8-sig")
            reader = csv.DictReader(io.StringIO(text))
            if reader.fieldnames is None or not _REQUIRED_COLUMNS.issubset(reader.fieldnames):
                raise NseFinalCloseUnavailable(
                    f"Official NSE {segment} final schema is incomplete."
                )
            rows = list(reader)
        except UnicodeDecodeError as exc:
            raise NseFinalCloseUnavailable(
                f"Official NSE {segment} final archive encoding is invalid."
            ) from exc
        if not rows:
            raise NseFinalCloseUnavailable(
                f"Official NSE {segment} final evidence is empty."
            )
        expected_date = session_date.isoformat()
        for row in rows:
            if (
                row["TradDt"].strip() != expected_date
                or row["BizDt"].strip() != expected_date
                or row["Sgmt"].strip().upper() != segment
                or row["Src"].strip().upper() != "NSE"
            ):
                raise NseFinalCloseUnavailable(
                    f"Official NSE {segment} final archive contains mismatched rows."
                )
        return rows, hashlib.sha256(content).hexdigest(), archive_name

    def fetch_segments(
        self, session_date: date, segments: tuple[str, ...]
    ) -> NseFinalCloseCatalog:
        rows_by_segment: dict[str, list[dict[str, str]]] = {}
        archive_hashes: dict[str, str] = {}
        archive_names: dict[str, str] = {}
        for segment in segments:
            rows, archive_hash, archive_name = self._fetch_segment(
                segment, session_date
            )
            rows_by_segment[segment] = rows
            archive_hashes[segment] = archive_hash
            archive_names[segment] = archive_name
        return NseFinalCloseCatalog(
            session_date=session_date,
            rows_by_segment=rows_by_segment,
            archive_hashes=archive_hashes,
            archive_names=archive_names,
            retrieved_at=datetime.now(timezone.utc),
        )

    def fetch_cash(self, session_date: date) -> NseFinalCloseCatalog:
        """Fetch only the official cash-market final archive."""
        return self.fetch_segments(session_date, ("CM",))

    def fetch(self, session_date: date) -> NseFinalCloseCatalog:
        return self.fetch_segments(session_date, ("CM", "FO"))
