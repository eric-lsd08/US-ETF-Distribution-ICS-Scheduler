#!/usr/bin/env python3

"""
extract_spdr_dates_with_footer_verbose.py

Extracts monthly and “Potential Excise Distribution” rows from SPDR PDF into CSV.
Splits per-ticker columns and independently generates ICS events from rows.

"""

import re, csv, requests, pdfplumber, os, sys, uuid
from datetime import datetime, timedelta, timezone
from pandas.tseries.holiday import USFederalHolidayCalendar
from pandas.tseries.offsets import CustomBusinessDay

# Configuration flags
GENERATE_FULL_CSV = True                        # True => write OUTPUT_CSV
GENERATE_SPLIT_EX_DATE = False                  # True => generate per-ticker Ex Date CSV
GENERATE_SPLIT_EX_DATE_MINUS_1 = False           # True => generate per-ticker Ex Date -1 CSV
GENERATE_SPLIT_RECORD_DATE = False              # True => generate per-ticker Record Date CSV
GENERATE_SPLIT_PAYABLE_DATE = True              # True => generate per-ticker Payable Date CSV

GENERATE_ICS_EX_DATE = False                    # True => generate ICS for Ex Date
GENERATE_ICS_EX_DATE_MINUS_1 = True             # True => generate ICS for Ex Date -1
GENERATE_ICS_RECORD_DATE = False                # True => generate ICS for Record Date
GENERATE_ICS_PAYABLE_DATE = False               # True => generate ICS for Payable Date

# Per-ticker override of columns to extract and process.
# If not specified here, the script uses the global GENERATE_SPLIT_* flags to determine columns.
# Example:
#   TICKER_COLUMNS_CONFIG = {
#       "GAL": ["Ex Date -1"],        # For GAL, only extract and process the 'Ex Date -1' column.
#       "SPY": ["Record Date","Payable Date"]  # For SPY, only extract 'Record Date' and 'Payable Date'.
#   }
TICKER_COLUMNS_CONFIG = {"GAL": ["Ex Date -1"], "SPY": ["Record Date"]}

PDF_URL = "https://www.ssga.com/library-content/products/fund-data/etfs/us/distribution/SPDR_Dividend_Distribution_Schedule.pdf"
PDF_PATH = "SPDR_Dividend_Distribution_Schedule.pdf"
OUTPUT_CSV = "spdr_dates_with_footer_verbose.csv"
TICKERS = "BIL,SPY"

MONTH_NAMES = ["January","February","March","April","May","June","July","August","September","October","November","December"]
MONTH_RE = re.compile(
    r"^(" + "|".join(MONTH_NAMES) + r")\s+(\d{1,2}/\d{1,2}/\d{4})\s+"
    r"(\d{1,2}/\d{1,2}/\d{4})\s+(\d{1,2}/\d{1,2}/\d{4})"
)
FOOTER_RE = re.compile(
    r"^Potential Excise Distribution\s+(\d{1,2}/\d{1,2}/\d{4})\s+"
    r"(\d{1,2}/\d{1,2}/\d{4})\s+(\d{1,2}/\d{1,2}/\d{4})"
)

us_bd = CustomBusinessDay(calendar=USFederalHolidayCalendar())

def get_business_day_before(date_str: str) -> str:
    d = datetime.strptime(date_str, "%m/%d/%Y")
    return (d - us_bd).strftime("%m/%d/%Y")

def download_pdf():
    if os.path.exists(PDF_PATH):
        print(f"PDF already downloaded: {PDF_PATH}")
        return
    print(f"Downloading PDF from {PDF_URL} ...")
    r = requests.get(PDF_URL); r.raise_for_status()
    with open(PDF_PATH, "wb") as f: f.write(r.content)
    print(f"Downloaded PDF saved to {PDF_PATH}")

def extract_rows():
    print(f"Extracting dates from PDF: {PDF_PATH}")
    rows = []
    with pdfplumber.open(PDF_PATH) as pdf:
        for page in pdf.pages:
            text = page.extract_text()
            if not text: continue
            for ln in text.splitlines():
                m = MONTH_RE.match(ln.strip())
                if m:
                    month, exd, recd, payd = m.groups()
                    rows.append([month, exd, get_business_day_before(exd), recd, payd])
                    continue
                f = FOOTER_RE.match(ln.strip())
                if f:
                    exd, recd, payd = f.groups()
                    rows.append(["Potential Excise Distribution", exd, get_business_day_before(exd), recd, payd])
    print(f"Extracted {len(rows)} rows")
    return rows

def write_csv(rows, path):
    print(f"Writing {len(rows)} rows to CSV file: {path}")
    with open(path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["Month","Ex Date","Ex Date -1","Record Date","Payable Date"])
        w.writerows(rows)
    print("CSV writing complete.")

def write_ticker_splits(rows, base, cols):
    idx = {"Ex Date":1, "Ex Date -1":2, "Record Date":3, "Payable Date":4}
    for c in cols:
        if (c=="Ex Date" and not GENERATE_SPLIT_EX_DATE) or \
           (c=="Ex Date -1" and not GENERATE_SPLIT_EX_DATE_MINUS_1) or \
           (c=="Record Date" and not GENERATE_SPLIT_RECORD_DATE) or \
           (c=="Payable Date" and not GENERATE_SPLIT_PAYABLE_DATE):
            continue
        fn = f"{base}_{c.replace(' ','_')}.csv"
        print(f"Writing split file for column '{c}': {fn}")
        with open(fn, "w", newline="") as f:
            w = csv.writer(f); w.writerow(["Month", c])
            for r in rows:
                if len(r) > idx[c]:
                    w.writerow([r[0], r[idx[c]]])

def generate_ics_from_rows(rows, base, cols, ticker):
    idx = {"Ex Date":1, "Ex Date -1":2, "Record Date":3, "Payable Date":4}
    flags = {
        "Ex Date":GENERATE_ICS_EX_DATE,
        "Ex Date -1":GENERATE_ICS_EX_DATE_MINUS_1,
        "Record Date":GENERATE_ICS_RECORD_DATE,
        "Payable Date":GENERATE_ICS_PAYABLE_DATE
    }
    for c in cols:
        if not flags[c]: continue
        ics = f"{base}_{c.replace(' ','_')}.ics"
        print(f"Generating ICS file: {ics}")
        cal = ["BEGIN:VCALENDAR","VERSION:2.0","PRODID:-//Extract SPDR//EN","CALSCALE:GREGORIAN","METHOD:PUBLISH"]
        for r in rows:
            if len(r) <= idx[c]: continue
            date = r[idx[c]]
            try: dt = datetime.strptime(date, "%m/%d/%Y")
            except: continue
            uid = str(uuid.uuid4()); ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
            d1 = dt.strftime("%Y%m%d"); d2 = (dt + timedelta(days=1)).strftime("%Y%m%d")
            alarm = dt.replace(hour=12, minute=0).strftime("%Y%m%dT%H%M%SZ")
            summary = f"{ticker} Dividend {c}"
            cal += [
                "BEGIN:VEVENT",
                f"UID:{uid}",
                f"DTSTAMP:{ts}",
                f"SUMMARY:{summary}",
                f"DTSTART;VALUE=DATE:{d1}",
                f"DTEND;VALUE=DATE:{d2}",
                f"RELATED-TO:{uid}",
                "BEGIN:VALARM",
                f"TRIGGER;VALUE=DATE-TIME:{alarm}",
                "DESCRIPTION:Reminder",
                "ACTION:DISPLAY",
                "END:VALARM",
                "END:VEVENT"
            ]
        cal.append("END:VCALENDAR")
        with open(ics, "w", newline="\r\n") as f:
            for l in cal: f.write(l + "\r\n")
        print(f"ICS file saved: {ics}")

def extract_segment_by_ticker(ticker):
    cols = []
    if GENERATE_SPLIT_EX_DATE: cols.append("Ex Date")
    if GENERATE_SPLIT_EX_DATE_MINUS_1: cols.append("Ex Date -1")
    if GENERATE_SPLIT_RECORD_DATE: cols.append("Record Date")
    if GENERATE_SPLIT_PAYABLE_DATE: cols.append("Payable Date")
    if ticker in TICKER_COLUMNS_CONFIG:
        cols = TICKER_COLUMNS_CONFIG[ticker]

    print(f"Extracting segment for ticker '{ticker}' from PDF: {PDF_PATH}")
    lines = []
    with pdfplumber.open(PDF_PATH) as pdf:
        for p in pdf.pages:
            t = p.extract_text()
            if not t: continue
            lines += ([] if not lines else [""]) + [l.strip() for l in t.splitlines()]

    pattern = re.compile(rf"\b{ticker}\b")
    ticker_idxs = [i for i,l in enumerate(lines) if pattern.search(l)]
    if not ticker_idxs:
        print(f"Ticker '{ticker}' not found in the document.")
        return
    ticker_idx = ticker_idxs[-1]

    header_idx = None
    for i in range(ticker_idx, -1, -1):
        if lines[i].startswith("Ex Date Record Date Payable Date"):
            header_idx = i
            break
    if header_idx is None:
        print(f"No header found before ticker '{ticker}'.")
        return

    footer_idx = None
    for j in range(header_idx+1, len(lines)):
        if FOOTER_RE.match(lines[j]):
            footer_idx = j
            break
    if footer_idx is None:
        print(f"No footer found after header at line {header_idx}.")
        return

    segment = lines[header_idx:footer_idx+1]
    rows = []
    for ln in segment[1:]:
        m = MONTH_RE.match(ln)
        if m:
            month,exd,recd,payd = m.groups()
            rows.append([month, exd, get_business_day_before(exd), recd, payd])
        else:
            f = FOOTER_RE.match(ln)
            if f:
                exd,recd,payd = f.groups()
                rows.append(["Potential Excise Distribution", exd, get_business_day_before(exd), recd, payd])

    base = f"{ticker}_SPDR_Schedule"
    if GENERATE_FULL_CSV:
        write_csv(rows, OUTPUT_CSV)
    if any([GENERATE_SPLIT_EX_DATE,GENERATE_SPLIT_EX_DATE_MINUS_1,GENERATE_SPLIT_RECORD_DATE,GENERATE_SPLIT_PAYABLE_DATE]):
        write_ticker_splits(rows, base, cols)
    if any([GENERATE_ICS_EX_DATE,GENERATE_ICS_EX_DATE_MINUS_1,GENERATE_ICS_RECORD_DATE,GENERATE_ICS_PAYABLE_DATE]):
        generate_ics_from_rows(rows, base, cols, ticker)

if __name__ == "__main__":
    download_pdf()
    rows = extract_rows()
    if GENERATE_FULL_CSV:
        write_csv(rows, OUTPUT_CSV)
    for t in [x.strip() for x in TICKERS.split(",")]:
        extract_segment_by_ticker(t)
