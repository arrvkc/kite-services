from datetime import date
from decimal import Decimal
import io
import socket
import unittest
from unittest.mock import MagicMock, patch
import zipfile

import requests

from services.nse_final_close import (
    NSE_ARCHIVE_ROOT,
    NseFinalCloseIdentityError,
    NseFinalCloseNotPublished,
    NseFinalCloseProvider,
    NseFinalCloseUnavailable,
    _NseIPv4HTTPSConnectionPool,
    _create_ipv4_connection,
    closes_match,
)


HEADER = (
    "TradDt,BizDt,Sgmt,Src,FinInstrmTp,FinInstrmId,ISIN,TckrSymb,"
    "SctySrs,XpryDt,FininstrmActlXpryDt,StrkPric,OptnTp,FinInstrmNm,"
    "OpnPric,HghPric,LwPric,ClsPric,LastPric,PrvsClsgPric,UndrlygPric,"
    "SttlmPric,OpnIntrst,ChngInOpnIntrst,TtlTradgVol,TtlTrfVal,"
    "TtlNbOfTxsExctd,SsnId,NewBrdLotQty,Rmks,Rsvd1,Rsvd2,Rsvd3,Rsvd4\n"
)


def archive(segment: str, rows: list[str]) -> bytes:
    session = "20260917"
    filename = f"BhavCopy_NSE_{segment}_0_0_0_{session}_F_0000.csv"
    output = io.BytesIO()
    with zipfile.ZipFile(output, "w") as zipped:
        zipped.writestr(filename, HEADER + "".join(rows))
    return output.getvalue()


def cm_row(symbol: str = "LICI", close: str = "398.10") -> str:
    return (
        "2026-09-17,2026-09-17,CM,NSE,STK,1,INE0,"
        f"{symbol},EQ,,,,,{symbol},{close},{close},{close},{close},"
        f"{close},{close},,{close},,,100,1000,10,F1,1,,,,,\n"
    )


def fo_row(name: str = "NIFTY26SEPFUT", close: str = "23333.40") -> str:
    return (
        "2026-09-17,2026-09-17,FO,NSE,IDF,2,,NIFTY,,2026-09-29,"
        f"2026-09-29,,,{name},{close},{close},{close},{close},{close},"
        f"{close},23270.60,{close},100,10,10,1000,10,F1,65,,,,,\n"
    )


class NseFinalCloseTests(unittest.TestCase):
    def catalog(self, cm_rows=None, fo_rows=None):
        session = MagicMock()
        responses = []
        for body in (
            archive("CM", cm_rows or [cm_row()]),
            archive("FO", fo_rows or [fo_row()]),
        ):
            response = MagicMock()
            response.content = body
            response.raise_for_status.return_value = None
            responses.append(response)
        session.get.side_effect = responses
        return NseFinalCloseProvider(session=session).fetch(date(2026, 9, 17))

    def test_exact_cash_equity_and_index_future_identity(self):
        catalog = self.catalog()
        equity = catalog.resolve(
            {
                "resolved_exchange": "NSE",
                "resolved_tradingsymbol": "LICI",
                "kite_instrument_type": "EQ",
            },
            date(2026, 9, 17),
        )
        future = catalog.resolve(
            {
                "resolved_exchange": "NFO",
                "resolved_tradingsymbol": "NIFTY26SEPFUT",
                "kite_instrument_type": "FUT",
            },
            date(2026, 9, 17),
        )
        self.assertEqual(equity.close, Decimal("398.10"))
        self.assertEqual(equity.instrument_kind, "NSE_CM_EQ_FINAL")
        self.assertEqual(future.close, Decimal("23333.40"))
        self.assertEqual(future.instrument_kind, "NSE_FO_FUT_FINAL")

    def test_provider_requests_only_exact_official_final_archives(self):
        session = MagicMock()
        responses = []
        for body in (archive("CM", [cm_row()]), archive("FO", [fo_row()])):
            response = MagicMock()
            response.content = body
            response.raise_for_status.return_value = None
            responses.append(response)
        session.get.side_effect = responses

        NseFinalCloseProvider(session=session).fetch(date(2026, 9, 17))

        self.assertEqual(
            [call.args[0] for call in session.get.call_args_list],
            [
                "https://nsearchives.nseindia.com/content/cm/"
                "BhavCopy_NSE_CM_0_0_0_20260917_F_0000.csv.zip",
                "https://nsearchives.nseindia.com/content/fo/"
                "BhavCopy_NSE_FO_0_0_0_20260917_F_0000.csv.zip",
            ],
        )

    def test_default_provider_scopes_ipv4_transport_to_nse_archives(self):
        provider = NseFinalCloseProvider()

        archive_adapter = provider._session.get_adapter(
            f"{NSE_ARCHIVE_ROOT}/cm/archive.zip"
        )
        ordinary_adapter = provider._session.get_adapter("https://example.com/")

        self.assertIs(
            archive_adapter.poolmanager.pool_classes_by_scheme["https"],
            _NseIPv4HTTPSConnectionPool,
        )
        self.assertIsNot(archive_adapter, ordinary_adapter)

    def test_shared_session_keeps_other_nse_requests_on_existing_transport(self):
        session = requests.Session()
        original_adapter = session.get_adapter("https://www.nseindia.com/")
        NseFinalCloseProvider(session=session)

        self.assertIs(session.get_adapter("https://www.nseindia.com/"), original_adapter)
        self.assertIsNot(
            session.get_adapter(f"{NSE_ARCHIVE_ROOT}/cm/archive.zip"),
            original_adapter,
        )

    def test_temporary_http_failure_can_recover_on_next_governed_attempt(self):
        session = MagicMock()
        failed = requests.Response()
        failed.status_code = 403
        successful = requests.Response()
        successful.status_code = 200
        successful._content = archive("CM", [cm_row()])
        session.get.side_effect = [failed, successful]
        provider = NseFinalCloseProvider(session=session)

        with self.assertRaisesRegex(NseFinalCloseUnavailable, "http_status=403"):
            provider.fetch_cash(date(2026, 9, 17))
        catalog = provider.fetch_cash(date(2026, 9, 17))
        evidence = catalog.resolve(
            {
                "resolved_exchange": "NSE",
                "resolved_tradingsymbol": "LICI",
                "kite_instrument_type": "EQ",
            },
            date(2026, 9, 17),
        )
        self.assertEqual(evidence.close, Decimal("398.10"))
        self.assertEqual(session.get.call_count, 2)

    @patch("services.nse_final_close.socket.socket")
    @patch("services.nse_final_close.socket.getaddrinfo")
    def test_archive_transport_resolves_only_ipv4(self, getaddrinfo, socket_factory):
        sock = socket_factory.return_value
        getaddrinfo.return_value = [
            (socket.AF_INET, socket.SOCK_STREAM, 6, "", ("203.0.113.8", 443))
        ]

        result = _create_ipv4_connection(
            ("nsearchives.nseindia.com", 443), 30, None, None
        )

        self.assertIs(result, sock)
        getaddrinfo.assert_called_once_with(
            "nsearchives.nseindia.com",
            443,
            socket.AF_INET,
            socket.SOCK_STREAM,
        )
        sock.connect.assert_called_once_with(("203.0.113.8", 443))

    def test_http_failure_retains_safe_status_and_cause(self):
        session = MagicMock()
        response = MagicMock()
        response.status_code = 403
        response.raise_for_status.side_effect = requests.HTTPError(
            "synthetic secret must not be retained", response=response
        )
        session.get.return_value = response

        with self.assertRaisesRegex(
            NseFinalCloseUnavailable,
            r"cause=HTTPError http_status=403$",
        ) as raised:
            NseFinalCloseProvider(session=session).fetch_cash(date(2026, 9, 17))

        self.assertNotIn("synthetic secret", str(raised.exception))

    def test_transport_failure_retains_safe_cause_without_raw_detail(self):
        session = MagicMock()
        session.get.side_effect = requests.ConnectTimeout(
            "synthetic account marker must not be retained"
        )

        with self.assertRaisesRegex(
            NseFinalCloseUnavailable,
            r"cause=ConnectTimeout http_status=NONE$",
        ) as raised:
            NseFinalCloseProvider(session=session).fetch_cash(date(2026, 9, 17))

        self.assertNotIn("synthetic account marker", str(raised.exception))

    def test_unpublished_archive_remains_distinct_from_transport_failure(self):
        session = MagicMock()
        response = requests.Response()
        response.status_code = 404
        session.get.return_value = response

        with self.assertRaisesRegex(NseFinalCloseNotPublished, "http_status=404"):
            NseFinalCloseProvider(session=session).fetch_cash(date(2026, 9, 17))

    @patch("services.nse_final_close.socket.socket")
    @patch("services.nse_final_close.socket.getaddrinfo")
    def test_failed_ipv4_address_is_closed_before_trying_next(self, getaddrinfo, socket_factory):
        getaddrinfo.return_value = [
            (socket.AF_INET, socket.SOCK_STREAM, 6, "", ("203.0.113.8", 443)),
            (socket.AF_INET, socket.SOCK_STREAM, 6, "", ("203.0.113.9", 443)),
        ]
        failed, successful = MagicMock(), MagicMock()
        failed.connect.side_effect = TimeoutError("synthetic transport failure")
        socket_factory.side_effect = [failed, successful]

        result = _create_ipv4_connection(
            ("nsearchives.nseindia.com", 443), 30, None, None
        )

        self.assertIs(result, successful)
        failed.close.assert_called_once_with()
        successful.connect.assert_called_once_with(("203.0.113.9", 443))
        successful.settimeout.assert_called_once_with(30)

    def test_missing_wrong_or_duplicate_identity_fails_closed(self):
        catalog = self.catalog(cm_rows=[cm_row(), cm_row()])
        with self.assertRaisesRegex(NseFinalCloseIdentityError, "ambiguous"):
            catalog.resolve(
                {
                    "resolved_exchange": "NSE",
                    "resolved_tradingsymbol": "LICI",
                    "kite_instrument_type": "EQ",
                },
                date(2026, 9, 17),
            )
        with self.assertRaises(NseFinalCloseIdentityError):
            catalog.resolve(
                {
                    "resolved_exchange": "NSE",
                    "resolved_tradingsymbol": "MISSING",
                    "kite_instrument_type": "EQ",
                },
                date(2026, 9, 17),
            )
        with self.assertRaises(NseFinalCloseIdentityError):
            catalog.resolve(
                {
                    "resolved_exchange": "NSE",
                    "resolved_tradingsymbol": "NIFTY 50",
                    "kite_instrument_type": "INDEX",
                },
                date(2026, 9, 17),
            )

    def test_close_comparison_uses_published_decimal_precision(self):
        self.assertTrue(closes_match(398.1, Decimal("398.10")))
        self.assertFalse(closes_match(398.15, Decimal("398.10")))


if __name__ == "__main__":
    unittest.main()
