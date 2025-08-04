#!/usr/bin/env python3

import io
import csv
import requests
import re
import argparse
from datetime import datetime, timedelta
from PyPDF2 import PdfReader

PDF_URL = (
    "https://www.ishares.com/us/literature/shareholder-letters/"
    "isharesandblackrocketfsdistributionschedule.pdf"
)

def download_pdf(url: str) -> bytes:
    resp = requests.get(url)
    resp.raise_for_status()
    return resp.content

def extract_text(pdf_bytes: bytes) -> str:
    reader = PdfReader(io.BytesIO(pdf_bytes))
    text = ""
    for page in reader.pages:
        text += (page.extract_text() or "") + "\n"
    return text

def parse_dates(text: str, marker: str):
    idx_mark = text.rfind(marker)
    if idx_mark < 0:
        raise RuntimeError(f"Ticker '{marker.strip()}' not found")
    snippet = text[:idx_mark]
    purpose_positions = [m.start() for m in re.finditer("Purposes", snippet)]
    if not purpose_positions:
        raise RuntimeError("'Purposes' keyword not found before ticker")
    start_idx = purpose_positions[-1]
    block = snippet[start_idx:].splitlines()

    decl_blocks, ex_blocks, pay_blocks = [], [], []
    cur, buf = None, []

    for line in block:
        if line.startswith("DECLARATION DATE:"):
            if cur and buf:
                (decl_blocks if cur=="DECL" else ex_blocks if cur=="EX" else pay_blocks).append(" ".join(buf))
            cur, buf = "DECL", [line.split(":",1)[1]]
        elif line.startswith("EX-DATE/RECORD DATE:"):
            if cur and buf:
                (decl_blocks if cur=="DECL" else ex_blocks if cur=="EX" else pay_blocks).append(" ".join(buf))
            cur, buf = "EX", [line.split(":",1)[1]]
        elif line.startswith("PAY DATE:"):
            if cur and buf:
                (decl_blocks if cur=="DECL" else ex_blocks if cur=="EX" else pay_blocks).append(" ".join(buf))
            cur, buf = "PAY", [line.split(":",1)[1]]
        else:
            if cur and line.strip():
                buf.append(line)
    if cur and buf:
        (decl_blocks if cur=="DECL" else ex_blocks if cur=="EX" else pay_blocks).append(" ".join(buf))

    if not (len(decl_blocks)==len(ex_blocks)==len(pay_blocks)):
        raise RuntimeError("Mismatched block counts")

    if len(decl_blocks) > 3:
        decl_blocks = decl_blocks[-3:]
        ex_blocks   = ex_blocks[-3:]
        pay_blocks  = pay_blocks[-3:]

    rows = []
    dr = re.compile(r"\d{1,2}-[A-Za-z]{3}-\d{2,4}")
    for decl, ex, pay in zip(decl_blocks, ex_blocks, pay_blocks):
        ds = dr.findall(decl)
        es = dr.findall(ex)
        ps = dr.findall(pay)
        for d, e, p in zip(ds, es, ps):
            def norm(tok):
                dd, mm, yy = tok.split("-")
                if len(yy) == 2:
                    yy = "20" + yy
                return datetime.strptime(f"{dd}-{mm}-{yy}", "%d-%b-%Y").date().isoformat()
            rows.append({
                "declaration_date": norm(d),
                "ex_date":          norm(e),
                "pay_date":         norm(p),
            })
    return rows

def write_csv(rows, fname):
    with open(fname, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["declaration_date","ex_date","pay_date"])
        writer.writeheader()
        for r in rows:
            writer.writerow(r)

def write_ics(rows, ticker, write_decl, write_ex, write_pay):
    """Write ICS files based on flags."""
    settings = [
        ("declaration_date", f"{ticker} Declaration Date", write_decl),
        ("ex_date",          f"{ticker} Ex-Dividend Date", write_ex),
        ("pay_date",         f"{ticker} Pay Date",         write_pay),
    ]
    for field, title, enabled in settings:
        if not enabled:
            continue

        fname = f"{ticker.lower()}_{field}.ics"
        lines = [
            "BEGIN:VCALENDAR\n",
            "VERSION:2.0\n",
            f"PRODID:-//{ticker} ETL//EN\n",
        ]
        for r in rows:
            dt = r[field]
            dts = datetime.fromisoformat(dt).strftime("%Y%m%d")
            dte = (datetime.fromisoformat(dt) + timedelta(days=1)).strftime("%Y%m%d")
            uid = f"{ticker}-{field}-{dts}"
            lines += [
                "BEGIN:VEVENT\n",
                f"UID:{uid}\n",
                "CLASS:PUBLIC\n",
                f"SUMMARY:{title}\n",
                f"DTSTART;VALUE=DATE:{dts}\n",
                f"DTEND;VALUE=DATE:{dte}\n",
                f"RELATED-TO:{ticker}\n",
                "BEGIN:VALARM\n",
                "ACTION:DISPLAY\n",
                f"DESCRIPTION:{title}\n",
                "TRIGGER;RELATED=START:PT20H\n",  # alarm at 8pm UTC+8 same day
                "END:VALARM\n",
                "END:VEVENT\n",
            ]
        lines.append("END:VCALENDAR\n")
        with open(fname, "w") as f:
            f.writelines(lines)
        print(f"Wrote {fname} with {len(rows)} events")

def main():
    parser = argparse.ArgumentParser(
        description="Fetch dividend schedule and output CSV and optional ICS files."
    )
    parser.add_argument("ticker", help="ETF ticker (e.g., SGOV or GGOV)")
    parser.add_argument(
        "--no-declaration-ics", dest="decl", action="store_false",
        help="Disable writing ICS for declaration_date"
    )
    parser.add_argument(
        "--no-ex-ics", dest="ex", action="store_false",
        help="Disable writing ICS for ex_date"
    )
    parser.add_argument(
        "--no-pay-ics", dest="pay", action="store_false",
        help="Disable writing ICS for pay_date"
    )
    args = parser.parse_args()

    write_decl = getattr(args, "decl", True)
    write_ex   = getattr(args, "ex",   True)
    write_pay  = getattr(args, "pay",  True)

    pdf_bytes = download_pdf(PDF_URL)
    text = extract_text(pdf_bytes)
    rows = parse_dates(text, args.ticker + " ")
    csv_file = f"{args.ticker.lower()}_dates.csv"
    write_csv(rows, csv_file)
    print(f"Wrote {csv_file}")

    write_ics(rows, args.ticker.upper(), write_decl, write_ex, write_pay)

if __name__ == "__main__":
    main()
