from datetime import date, datetime, timezone
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
    NseFinalCloseCatalog,
    official_symbol_for_session,
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


class RenamedInstrumentFinalCloseTests(unittest.TestCase):
    def catalog(self, target, symbols, *, segment="CM"):
        return NseFinalCloseCatalog(
            session_date=target,
            rows_by_segment={segment: [
                {"TckrSymb": symbol, "SctySrs": "EQ", "FinInstrmTp": "STK" if segment == "CM" else "STF",
                 "FinInstrmNm": symbol, "ClsPric": "600.10"}
                for symbol in symbols
            ]},
            archive_hashes={segment: "a" * 64},
            archive_names={segment: "official-final.zip"},
            retrieved_at=datetime(2026, 10, 29, tzinfo=timezone.utc),
        )

    def test_effective_date_and_both_symbol_directions(self):
        for target, expected in [(date(2026, 10, 22), "ICICIPRULI"), (date(2026, 10, 23), "ICICILIFE")]:
            for requested in ["ICICILIFE", "ICICIPRULI"]:
                with self.subTest(target=target, requested=requested):
                    evidence = self.catalog(target, [expected]).resolve(
                        {"resolved_exchange": "NSE", "resolved_tradingsymbol": requested, "kite_instrument_type": "EQ"}, target
                    )
                    self.assertEqual(evidence.tradingsymbol, expected)
                    self.assertEqual(evidence.close, Decimal("600.10"))
                    self.assertEqual(evidence.source_sha256, "a" * 64)

    def test_derivative_contract_suffix_is_preserved(self):
        target = date(2026, 10, 22)
        evidence = self.catalog(target, ["ICICIPRULI26OCTFUT"], segment="FO").resolve(
            {"resolved_exchange": "NFO", "resolved_tradingsymbol": "ICICILIFE26OCTFUT", "kite_instrument_type": "FUT"}, target
        )
        self.assertEqual(evidence.tradingsymbol, "ICICIPRULI26OCTFUT")

    def test_wrong_date_name_and_duplicate_remain_rejected(self):
        target = date(2026, 10, 22)
        instrument = {"resolved_exchange": "NSE", "resolved_tradingsymbol": "ICICILIFE", "kite_instrument_type": "EQ"}
        for symbols in [["ICICILIFE"], ["ICICIPRULI", "ICICIPRULI"], ["UNRELATED"]]:
            with self.subTest(symbols=symbols), self.assertRaises(NseFinalCloseIdentityError):
                self.catalog(target, symbols).resolve(instrument, target)

    def test_no_prefix_or_fuzzy_matching(self):
        for symbol in ["ICICILIFEOTHER", "ICICILIFE26OCT600CE", "ICICIBANK"]:
            self.assertEqual(official_symbol_for_session(symbol, date(2026, 10, 22)), symbol)


class HistoricalExpiryCatalogTests(unittest.TestCase):
    catalog = RenamedInstrumentFinalCloseTests.catalog

    def test_expired_contracts_are_retained_for_the_historical_session(self):
        target = date(2026, 10, 22)
        catalog = self.catalog(target, ["ICICIPRULI26OCTFUT", "ICICIPRULI26NOVFUT"], segment="FO")
        for row, expiry in zip(catalog._rows_by_segment["FO"], ["2026-10-27", "2026-11-24"]):
            row["TckrSymb"] = "ICICIPRULI"
            row["XpryDt"] = expiry
        self.assertEqual(catalog.monthly_futures_expiries("ICICILIFE"), [date(2026, 10, 27), date(2026, 11, 24)])
        catalog._rows_by_segment["FO"].append(catalog._rows_by_segment["FO"][0].copy())
        with self.assertRaises(NseFinalCloseIdentityError):
            catalog.monthly_futures_expiries("ICICILIFE")

    def test_mismatched_monthly_name_does_not_invent_an_expiry(self):
        target = date(2026, 10, 22)
        catalog = self.catalog(target, ["ICICIPRULI26NOVFUT"], segment="FO")
        row = catalog._rows_by_segment["FO"][0]
        row.update(TckrSymb="ICICIPRULI", XpryDt="2026-10-27")
        with self.assertRaises(NseFinalCloseIdentityError):
            catalog.monthly_futures_expiries("ICICILIFE")


class HistoricalMembershipTests(unittest.TestCase):
    def test_later_listings_are_removed_and_historical_members_must_map(self):
        catalog = RenamedInstrumentFinalCloseTests().catalog(date(2026, 10, 22), ["ICICIPRULI26OCTFUT"], segment="FO")
        catalog._rows_by_segment["FO"][0].update(TckrSymb="ICICIPRULI", XpryDt="2026-10-27")
        self.assertEqual(catalog.filter_current_universe(["ICICILIFE", "NEWCO"]), ["ICICILIFE"])
        self.assertIsNone(catalog.nearest_index_future("ICICILIFE"))
        self.assertEqual(catalog.filter_current_universe(["NEWCO"], {"ICICILIFE"}), ["ICICILIFE"])
        for symbols in [["NEWCO"], ["ICICILIFE", "ICICIPRULI"]]:
            with self.assertRaises(NseFinalCloseIdentityError):
                catalog.filter_current_universe(symbols)

    def test_index_near_contract_comes_from_the_target_archive(self):
        catalog = RenamedInstrumentFinalCloseTests().catalog(date(2026, 10, 22), ["NIFTY26OCTFUT"], segment="FO")
        catalog._rows_by_segment["FO"][0].update(TckrSymb="NIFTY", FinInstrmTp="IDF", XpryDt="2026-10-27")
        self.assertEqual(catalog.nearest_index_future("NIFTY"), (date(2026, 10, 27), "NIFTY26OCTFUT"))


if __name__ == "__main__":
    unittest.main()
