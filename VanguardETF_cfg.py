#!/usr/bin/env python3

"""
Downloads Vanguard’s 2025 dividend schedule PDF, extracts dates for configured
ETF tickers, normalizes all date formats, writes to CSV, and generates separate
all-day ICS files with an 8 PM local alarm (UTC+8) using a relative trigger.

Configuration allows enabling/disabling each ICS output.
"""

import csv
import re
import requests
import uuid
import pdfplumber
import pandas as pd
from io import BytesIO
from datetime import datetime, timedelta
from pandas.tseries.holiday import USFederalHolidayCalendar
from pandas.tseries.offsets import CustomBusinessDay

# === Configuration ===
TICKERS = ["VOO", "VPLS"]  # List of tickers, e.g. ["VOO", "VPLS"]
PDF_URL = (
    "https://investor.vanguard.com/content/dam/retail/publicsite/en/documents/"
    "taxes/DIVDAT_012025.pdf"
)

# Only “Ex-Dividend Date-1” alarms are enabled here; toggle as needed
ENABLE_ICS = {
    "Record Date": False,
    "Ex-Dividend Date-1": True,
    "Ex-Dividend Date": False,
    "Payable Date": False,
}

# Matches M/D/YY, M/D/YYYY, or YYYY/MM/DD
DATE_PATTERN = re.compile(r"^(?P<m>\d{1,2})[/-](?P<d>\d{1,2})[/-](?P<y>\d{2}(?:\d{2})?)$")
# For all-day events, local 20:00 is 20 hours after midnight → PT20H
RELATIVE_TRIGGER = "PT20H"

# Define a US business day offset (excludes weekends and US federal holidays)
US_BUSINESS_DAY = CustomBusinessDay(calendar=USFederalHolidayCalendar())

def download_pdf(url: str) -> BytesIO:
    resp = requests.get(url)
    resp.raise_for_status()
    return BytesIO(resp.content)


def normalize_date(dt_str: str) -> str:
    """
    Convert M/D/YY, M/D/YYYY, or YYYY/MM/DD into M/D/YY.
    """
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
    # Subtract one US business day
    prev = ex_dt - US_BUSINESS_DAY
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
                    normalize_date(rec)
                    normalize_date(exd)
                    normalize_date(pay)
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
    headers = [
        "Quarter",
        "Record Date",
        "Ex-Dividend Date-1",
        "Ex-Dividend Date",
        "Payable Date",
    ]
    with open(filename, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=headers)
        w.writeheader()
        w.writerows(data)


def csv_to_separate_ics(csv_file: str, ticker: str):
    df = pd.read_csv(csv_file, dtype=str)
    ics_files = {
        "Record Date": f"{ticker.lower()}_record_dates.ics",
        "Ex-Dividend Date-1": f"{ticker.lower()}_ex_minus_1_dates.ics",
        "Ex-Dividend Date": f"{ticker.lower()}_ex_dates.ics",
        "Payable Date": f"{ticker.lower()}_payable_dates.ics",
    }
    for dt_type, enabled in ENABLE_ICS.items():
        if not enabled:
            continue
        fname = ics_files[dt_type]
        lines = [
            "BEGIN:VCALENDAR",
            "VERSION:2.0",
            f"PRODID:-//{ticker} Dividend Schedule//example.com//",
            "CALSCALE:GREGORIAN",
            "METHOD:PUBLISH",
        ]
        count = 0
        for _, row in df.iterrows():
            d = datetime.strptime(row[dt_type], "%m/%d/%y").date()
            dtstart = d.strftime("%Y%m%d")
            dtend = (d + timedelta(days=1)).strftime("%Y%m%d")
            uid = str(uuid.uuid4())
            summary = f"{ticker} {row['Quarter']} – {dt_type}"
            lines.extend([
                "BEGIN:VEVENT",
                f"UID:{uid}",
                f"SUMMARY:{summary}",
                f"DTSTART;VALUE=DATE:{dtstart}",
                f"DTEND;VALUE=DATE:{dtend}",
                "BEGIN:VALARM",
                "ACTION:DISPLAY",
                "DESCRIPTION:Reminder",
                f"TRIGGER;RELATED=START:{RELATIVE_TRIGGER}",
                "END:VALARM",
                "END:VEVENT",
            ])
            count += 1
        lines.append("END:VCALENDAR")
        with open(fname, "w", encoding="utf-8") as f:
            f.write("\r\n".join(lines) + "\r\n")
        print(f"Wrote {fname} ({count} events)")


def main():
    print(f"Tickers: {', '.join(TICKERS)}")
    pdf = download_pdf(PDF_URL)

    for ticker in TICKERS:
        print(f"Extracting schedule for {ticker}…")
        sched = extract_schedule_for_ticker(pdf, ticker)
        if not sched:
            print(f"No records found for ticker {ticker}.")
            continue
        output_csv = f"{ticker.lower()}_dividend_schedule.csv"
        print(f"Found {len(sched)} entries for {ticker}. Saving CSV…")
        save_to_csv(sched, output_csv)
        print(f"Generating ICS files for {ticker}…")
        csv_to_separate_ics(output_csv, ticker)


if __name__ == "__main__":
    main()
