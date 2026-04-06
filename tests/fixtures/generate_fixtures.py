"""
Generate XLSX fixture files for integration testing.

Run once: python tests/fixtures/generate_fixtures.py
Creates mexc_deposits.xlsx and mexc_withdrawals.xlsx with real export format headers.
"""
import os
from openpyxl import Workbook

FIXTURES_DIR = os.path.dirname(os.path.abspath(__file__))


def generate_mexc_deposits():
    wb = Workbook()
    ws = wb.active
    ws.title = "Sheet1"
    headers = ["UID", "Status", "Time", "Crypto", "Network",
               "Deposit Amount", "TxID", "Progress"]
    ws.append(headers)
    ws.append([96214533, "Credited Successfully", "2026-03-11 02:03:21",
               "BTC", "Bitcoin(BTC)", 0.00137478,
               "b90565f25a57968e52e36d6654474a635d138cd0f8e5ed7d0315c713ec4004d9:0",
               "(3/3)"])
    ws.append([96214533, "Credited Successfully", "2026-03-09 19:28:12",
               "BTC", "Bitcoin(BTC)", 0.00022277,
               "1353e028767e8a587c8431fd80fece415a93cce34e71dc23cf1e900aea15a21c:0",
               "(3/3)"])
    ws.append([96214533, "Credited Successfully", "2026-02-17 03:32:51",
               "USDT", "Solana(SOL)", 105,
               "4KPCmhEvKqySKbMMtWxyLThwJUTTCUZSJqci6C89h1heseR6LFQwr2NePA7gPkpJfYcautiK6Kvfi6J3rpRh6dVN:4",
               "(126/100)"])
    path = os.path.join(FIXTURES_DIR, "mexc_deposits.xlsx")
    wb.save(path)
    print(f"Created {path}")


def generate_mexc_withdrawals():
    wb = Workbook()
    ws = wb.active
    ws.title = "Sheet1"
    headers = ["UID", "Status", "Time", "Crypto", "Network",
               "Request Amount", "Withdrawal Address", "memo", "TxID",
               "Trading Fee", "Settlement Amount", "Withdrawal Descriptions"]
    ws.append(headers)
    ws.append([96214533, "Withdrawal Successful", "2026-02-18 20:43:55",
               "SAL", "SALVIUM1", 2090.61,
               "SC11aHNaiaVQzopqEDwGVhVeHcEz4mNB9NfBBwMtH9iN5iKYggAUM8366se8TWnrXdYn7QRJG2YpTMKeAUEPzefy32D67NewhG",
               "--",
               "151466150d6cf14cf940e18bd6936f10c747d40f2f31ad31e37d3441cba3ade8",
               4, 2086.61, "test"])
    path = os.path.join(FIXTURES_DIR, "mexc_withdrawals.xlsx")
    wb.save(path)
    print(f"Created {path}")


if __name__ == "__main__":
    generate_mexc_deposits()
    generate_mexc_withdrawals()
    print("Done!")
