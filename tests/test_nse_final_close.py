from datetime import date
from decimal import Decimal
import io
import unittest
from unittest.mock import MagicMock
import zipfile

from services.nse_final_close import (
    NseFinalCloseIdentityError,
    NseFinalCloseProvider,
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
