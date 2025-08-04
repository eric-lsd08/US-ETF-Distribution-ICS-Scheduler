#!/usr/bin/env python3

"""
Downloads Vanguard’s 2025 dividend schedule PDF, extracts dates for a configured
ETF ticker, normalizes all date formats, writes to CSV, and generates separate
all-day ICS files with an 8 PM local alarm (UTC+8) using a relative trigger.

Ticker and ICS output types are now configurable via command-line arguments.
"""

import csv
import re
import requests
import uuid
import pdfplumber
import pandas as pd

from io import BytesIO
from datetime import datetime, timedelta
from pandas.tseries.offsets import BDay
import argparse
import os

# Matches M/D/YY, M/D/YYYY, or YYYY/MM/DD
DATE_PATTERN = re.compile(r"^(?P<m>\d{1,2})[/-](?P<d>\d{1,2})[/-](?P<y>\d{2}(?:\d{2})?)$")
RELATIVE_TRIGGER = "PT20H"  # 8 PM local alarm

def download_pdf(url: str) -> BytesIO:
    resp = requests.get(url)
    resp.raise_for_status()
    return BytesIO(resp.content)

def normalize_date(dt_str: str) -> str:
    m = DATE_PATTERN.match(dt_str)
    if m:
        month, day, year = m.group("m"), m.group("d"), m.group("y")
    else:
        parts = re.split(r"[/-]", dt_str)
        if len(parts) == 3 and len(parts[0]) == 4:
            year, month, day = parts
        else:
            raise ValueError(f"Unrecognized date format: {dt_str}")
    yy = year[-2:]
    return f"{int(month)}/{int(day)}/{yy}"

def parse_row(rec: str, exd: str, pay: str) -> dict:
    rec_norm = normalize_date(rec)
    ex_norm = normalize_date(exd)
    pay_norm = normalize_date(pay)
    ex_dt = datetime.strptime(ex_norm, "%m/%d/%y")
    prev = ex_dt - BDay(1)
    ex_prev = f"{prev.month}/{prev.day}/{str(prev.year)[2:]}"
    quarter = (
        "Q1" if ex_dt.month <= 3 else
        "Q2" if ex_dt.month <= 6 else
        "Q3" if ex_dt.month <= 9 else
        "Q4"
    )
    return {
        "Quarter": quarter,
        "Record Date": rec_norm,
        "Ex-Dividend Date-1": ex_prev,
        "Ex-Dividend Date": ex_norm,
        "Payable Date": pay_norm,
    }

def extract_schedule_for_ticker(pdf_stream: BytesIO, ticker: str) -> list[dict]:
    ticker = ticker.upper()
    lines = []
    with pdfplumber.open(pdf_stream) as pdf:
        for pg in pdf.pages:
            txt = pg.extract_text()
            if txt:
                lines.extend(txt.split("\n"))
    for i, line in enumerate(lines):
        parts = line.split()
        uppers = [p.upper() for p in parts]
        if ticker in uppers:
            pos = uppers.index(ticker)
            if len(parts) >= pos + 4:
                rec, exd, pay = parts[pos+1], parts[pos+2], parts[pos+3]
                try:
                    normalize_date(rec); normalize_date(exd); normalize_date(pay)
                except ValueError:
                    continue
                schedule = [parse_row(rec, exd, pay)]
                for extra in lines[i+1:]:
                    tok = extra.split()
                    if len(tok) >= 3:
                        try:
                            normalize_date(tok[0])
                        except ValueError:
                            break
                        schedule.append(parse_row(tok[0], tok[1], tok[2]))
                    else:
                        break
                return schedule
    return []

def save_to_csv(data: list[dict], filename: str):
    headers = ["Quarter", "Record Date", "Ex-Dividend Date-1", "Ex-Dividend Date", "Payable Date"]
    with open(filename, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        writer.writerows(data)

def csv_to_separate_ics(csv_file: str, ticker: str, enabled_types: set[str], out_dir: str):
    df = pd.read_csv(csv_file, dtype=str)
    os.makedirs(out_dir, exist_ok=True)
    for dt_type in ["Record Date", "Ex-Dividend Date-1", "Ex-Dividend Date", "Payable Date"]:
        if dt_type not in enabled_types:
            continue
        fname = os.path.join(out_dir, f"{ticker.lower()}_{dt_type.replace(' ', '_')}.ics")
        lines = [
            "BEGIN:VCALENDAR", "VERSION:2.0",
            f"PRODID:-//{ticker} Dividend Schedule//example.com//",
            "CALSCALE:GREGORIAN", "METHOD:PUBLISH",
        ]
        count = 0
        for _, row in df.iterrows():
            d = datetime.strptime(row[dt_type], "%m/%d/%y").date()
            dtstart = d.strftime("%Y%m%d")
            dtend = (d + timedelta(days=1)).strftime("%Y%m%d")
            uid = uuid.uuid4()
            summary = f"{ticker} {row['Quarter']} – {dt_type}"
            lines.extend([
                "BEGIN:VEVENT",
                f"UID:{uid}", f"SUMMARY:{summary}",
                f"DTSTART;VALUE=DATE:{dtstart}", f"DTEND;VALUE=DATE:{dtend}",
                "BEGIN:VALARM", "ACTION:DISPLAY", "DESCRIPTION:Reminder",
                f"TRIGGER;RELATED=START:{RELATIVE_TRIGGER}",
                "END:VALARM", "END:VEVENT",
            ])
            count += 1
        lines.append("END:VCALENDAR")
        with open(fname, "w", encoding="utf-8") as f:
            f.write("\r\n".join(lines) + "\r\n")
        print(f"Wrote {fname} ({count} events)")

def main():
    parser = argparse.ArgumentParser(
        description="Fetch Vanguard dividend dates and generate CSV + ICS files."
    )
    parser.add_argument(
        "--ticker", "-t",
        default="VOO",
        help="ETF ticker symbol (default: VOO)"
    )
    parser.add_argument(
        "--enable-ics", "-e",
        nargs="+",
        choices=["Record Date", "Ex-Dividend Date-1", "Ex-Dividend Date", "Payable Date"],
        default=["Ex-Dividend Date-1"],
        help="Date types to generate ICS for"
    )
    parser.add_argument(
        "--csv-file", "-c",
        default=None,
        help="Output CSV filename (default: '<ticker>_dividend_schedule.csv')"
    )
    parser.add_argument(
        "--ics-dir", "-d",
        default=".",
        help="Directory to write ICS files (default: current directory)"
    )
    args = parser.parse_args()

    ticker = args.ticker.upper()
    pdf_url = (
        "https://investor.vanguard.com/content/dam/retail/publicsite/en/documents/"
        "taxes/DIVDAT_012025.pdf"
    )
    csv_file = args.csv_file or f"{ticker.lower()}_dividend_schedule.csv"

    print(f"Ticker: {ticker}")
    pdf = download_pdf(pdf_url)
    print("Extracting schedule…")
    schedule = extract_schedule_for_ticker(pdf, ticker)
    if not schedule:
        print(f"No records found for ticker {ticker}.")
        return

    print(f"Found {len(schedule)} entries. Saving CSV…")
    save_to_csv(schedule, csv_file)

    print("Generating ICS files…")
    csv_to_separate_ics(csv_file, ticker, set(args.enable_ics), args.ics_dir)

if __name__ == "__main__":
    main()
