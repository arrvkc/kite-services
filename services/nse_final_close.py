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
import re
import socket
import sys
import zipfile

import requests
from requests.adapters import HTTPAdapter
from urllib3.connection import HTTPSConnection
from urllib3.connectionpool import HTTPSConnectionPool
from urllib3.exceptions import (
    ConnectTimeoutError,
    NameResolutionError,
    NewConnectionError,
)


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


def official_symbol_for_session(symbol: str, session_date: date) -> str:
    """Resolve only exchange-documented renames at their effective date.

    NSE/CML/76967 and NSE/FAOP/77013: ICICIPRULI -> ICICILIFE,
    effective 2026-10-23. No fuzzy or fallback matching is permitted.
    https://nsearchives.nseindia.com/content/circulars/CML76967.pdf
    https://nsearchives.nseindia.com/content/circulars/FAOP77013.pdf
    """
    match = re.fullmatch(r"(ICICIPRULI|ICICILIFE)([0-9]{2}[A-Z]{3}FUT)?", symbol)
    if match is None:
        return symbol
    underlying = "ICICIPRULI" if session_date < date(2026, 10, 23) else "ICICILIFE"
    return underlying + (match.group(2) or "")


def _create_ipv4_connection(
    address: tuple[str, int],
    timeout: float | None,
    source_address: tuple[str, int] | None,
    socket_options: list[tuple[int, int, int]] | None,
) -> socket.socket:
    """Connect through IPv4 without changing process-wide DNS behavior."""
    host, port = address
    if host.startswith("["):
        host = host.strip("[]")
    error: OSError | None = None
    for family, socktype, proto, _canonname, sockaddr in socket.getaddrinfo(
        host, port, socket.AF_INET, socket.SOCK_STREAM
    ):
        sock = None
        try:
            sock = socket.socket(family, socktype, proto)
            for option in socket_options or ():
                sock.setsockopt(*option)
            sock.settimeout(timeout)
            if source_address:
                sock.bind(source_address)
            sock.connect(sockaddr)
            return sock
        except OSError as exc:
            error = exc
            if sock is not None:
                sock.close()
    if error is not None:
        raise error
    raise OSError("IPv4 DNS resolution returned no addresses")


class _NseIPv4HTTPSConnection(HTTPSConnection):
    """Keep the official archive request off NSE's rejecting IPv6 edge."""

    def _new_conn(self) -> socket.socket:
        try:
            sock = _create_ipv4_connection(
                (self._dns_host, self.port),
                self.timeout,
                self.source_address,
                self.socket_options,
            )
        except socket.gaierror as exc:
            raise NameResolutionError(self.host, self, exc) from exc
        except TimeoutError as exc:
            raise ConnectTimeoutError(
                self,
                f"Connection to {self.host} timed out. "
                f"(connect timeout={self.timeout})",
            ) from exc
        except OSError as exc:
            raise NewConnectionError(
                self, f"Failed to establish a new connection: {exc}"
            ) from exc
        sys.audit("http.client.connect", self, self.host, self.port)
        return sock


class _NseIPv4HTTPSConnectionPool(HTTPSConnectionPool):
    ConnectionCls = _NseIPv4HTTPSConnection


class _NseIPv4HTTPAdapter(HTTPAdapter):
    """Scope IPv4 selection to the official NSE archive URL prefix."""

    def init_poolmanager(self, connections, maxsize, block=False, **pool_kwargs):
        super().init_poolmanager(
            connections, maxsize, block=block, **pool_kwargs
        )
        self.poolmanager.pool_classes_by_scheme = (
            self.poolmanager.pool_classes_by_scheme.copy()
        )
        self.poolmanager.pool_classes_by_scheme["https"] = (
            _NseIPv4HTTPSConnectionPool
        )


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

    def filter_current_universe(self, current_symbols: list[str]) -> list[str]:
        """Use archive membership, requiring a current broker identity for each member."""
        historical = {row["TckrSymb"].strip().upper() for row in self._rows_by_segment["FO"]
                      if row["FinInstrmTp"].strip().upper() in {"IDF", "STF"}}
        current = {official_symbol_for_session(symbol, self.session_date): symbol for symbol in current_symbols}
        if len(current) != len(current_symbols) or not historical or historical - current.keys():
            raise NseFinalCloseIdentityError("Historical futures membership cannot be mapped exactly to broker identities.")
        return sorted(current[symbol] for symbol in historical)

    def nearest_index_future(self, symbol: str) -> tuple[date, str] | None:
        historical_symbol = official_symbol_for_session(symbol, self.session_date)
        rows = [row for row in self._rows_by_segment["FO"]
                if row["TckrSymb"].strip().upper() == historical_symbol
                and row["FinInstrmTp"].strip().upper() == "IDF"]
        if not rows:
            return None
        expiry = self.monthly_futures_expiries(symbol)[0]
        month = "JAN FEB MAR APR MAY JUN JUL AUG SEP OCT NOV DEC".split()[expiry.month - 1]
        return expiry, f"{historical_symbol}{expiry.year % 100:02d}{month}FUT"

    def monthly_futures_expiries(self, symbol: str) -> list[date]:
        """Recover contracts listed in the exact session, including expired ones."""
        archive_symbol = official_symbol_for_session(symbol.strip().upper(), self.session_date)
        rows = [row for row in self._rows_by_segment["FO"]
                if row["TckrSymb"].strip().upper() == archive_symbol
                and row["FinInstrmTp"].strip().upper() in {"IDF", "STF"}]
        expiries = []
        for row in rows:
            try:
                expiry = date.fromisoformat(row["XpryDt"].strip())
            except (KeyError, ValueError) as exc:
                raise NseFinalCloseIdentityError("Official historical futures expiry is invalid.") from exc
            month = "JAN FEB MAR APR MAY JUN JUL AUG SEP OCT NOV DEC".split()[expiry.month - 1]
            expected_name = f"{archive_symbol}{expiry.year % 100:02d}{month}FUT"
            if row["FinInstrmNm"].strip().upper() != expected_name:
                raise NseFinalCloseIdentityError("Official historical monthly contract identity is ambiguous.")
            if expiry >= self.session_date:
                expiries.append(expiry)
        if not expiries or len(expiries) != len(set(expiries)):
            raise NseFinalCloseIdentityError("Official historical monthly contracts are missing or ambiguous.")
        return sorted(expiries)

    def resolve(self, instrument: dict, target_date: date) -> OfficialFinalCloseEvidence:
        if target_date != self.session_date:
            raise NseFinalCloseIdentityError(
                "Official NSE final evidence does not match the target session."
            )
        exchange = str(instrument.get("resolved_exchange") or "").strip().upper()
        tradingsymbol = str(
            instrument.get("resolved_tradingsymbol") or ""
        ).strip().upper()
        archive_symbol = official_symbol_for_session(tradingsymbol, target_date)
        instrument_type = str(
            instrument.get("kite_instrument_type") or ""
        ).strip().upper()
        if exchange == "NSE" and instrument_type == "EQ":
            segment = "CM"
            matches = [
                row
                for row in self._rows_by_segment[segment]
                if row["TckrSymb"].strip().upper() == archive_symbol
                and row["SctySrs"].strip().upper() == "EQ"
                and row["FinInstrmTp"].strip().upper() == "STK"
            ]
            kind = "NSE_CM_EQ_FINAL"
        elif exchange == "NFO" and instrument_type == "FUT":
            segment = "FO"
            matches = [
                row
                for row in self._rows_by_segment[segment]
                if row["FinInstrmNm"].strip().upper() == archive_symbol
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
            tradingsymbol=archive_symbol,
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
        if isinstance(self._session, requests.Session):
            self._session.mount(
                f"{NSE_ARCHIVE_ROOT}/", _NseIPv4HTTPAdapter()
            )
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
                    f"Official NSE {segment} final evidence is not published. "
                    "http_status=404"
                )
            response.raise_for_status()
        except NseFinalCloseNotPublished:
            raise
        except requests.RequestException as exc:
            response = getattr(exc, "response", None)
            status = getattr(response, "status_code", None)
            safe_status = str(status) if isinstance(status, int) else "NONE"
            raise NseFinalCloseUnavailable(
                f"Official NSE {segment} final evidence is unavailable. "
                f"cause={type(exc).__name__} http_status={safe_status}"
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
