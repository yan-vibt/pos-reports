from __future__ import annotations

import json
import os
import sys
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from decimal import Decimal, InvalidOperation
from pathlib import Path
import subprocess

import pyodbc


# =========================
# CONFIG (edit if needed)
# =========================
DSN = os.getenv("POS_DSN", "pos")
UID = os.getenv("POS_UID", "db")
PWD = os.getenv("POS_PWD", "db")

# POS business day start (06:30 -> next day 06:30)
BUSINESS_DAY_START = os.getenv("BUSINESS_DAY_START", "06:30")  # "HH:MM"

# Git options
DO_GIT_PUSH = True  # set False if you only want files generated locally
GIT_COMMIT_MESSAGE = "Backfill reports (daily + category)"

# Where to write in this repo
REPO_ROOT = Path(__file__).resolve().parent
REPORTS_DIR = REPO_ROOT / "reports"
INDEX_JSON = REPO_ROOT / "report_index.json"

# Backfill range: "starting December last year"
TODAY = date.today()
START_DATE = date(TODAY.year - 1, 12, 1)
END_DATE = TODAY  # inclusive

# =========================
# Helpers
# =========================
def d0(v) -> Decimal:
    try:
        if v is None:
            return Decimal("0")
        if isinstance(v, Decimal):
            return v
        return Decimal(str(v))
    except (InvalidOperation, ValueError):
        return Decimal("0")

def fmt2(v) -> str:
    return f"{d0(v):,.2f}"

def hhmm_to_time(hhmm: str) -> time:
    hh, mm = map(int, hhmm.split(":"))
    return time(hh, mm)

def business_window(day: date) -> tuple[datetime, datetime]:
    start = datetime.combine(day, hhmm_to_time(BUSINESS_DAY_START))
    end = start + timedelta(days=1)
    return start, end

def db_conn() -> pyodbc.Connection:
    return pyodbc.connect(f"DSN={DSN};UID={UID};PWD={PWD};", autocommit=True)

def ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)

def write_text(p: Path, s: str) -> None:
    ensure_dir(p.parent)
    p.write_text(s, encoding="utf-8")

def run(cmd: list[str], cwd: Path) -> None:
    r = subprocess.run(cmd, cwd=str(cwd), capture_output=True, text=True)
    if r.returncode != 0:
        raise RuntimeError(f"Command failed: {' '.join(cmd)}\nSTDOUT:\n{r.stdout}\nSTDERR:\n{r.stderr}")

# =========================
# HTML templates
# =========================
def html_shell(title: str, body: str, subtitle: str = "") -> str:
    return f"""<!doctype html>
<html>
<head>
  <meta charset="utf-8">
  <title>{title}</title>
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <style>
    body {{ font-family: Arial, sans-serif; background:#0b1220; color:#e9eefc; margin:0; padding:24px; }}
    .wrap {{ max-width: 1100px; margin: 0 auto; }}
    .card {{ background:#121a2b; border:1px solid rgba(255,255,255,.08); border-radius:10px; padding:16px; }}
    .muted {{ color: rgba(233,238,252,.75); }}
    .mono {{ font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", monospace; }}
    table {{ width:100%; border-collapse: collapse; }}
    th, td {{ padding:8px 10px; border-bottom:1px solid rgba(255,255,255,.08); }}
    thead th {{ background: rgba(255,255,255,.06); text-align:left; }}
    .right {{ text-align:right; }}
    .red {{ color:#ff6b6b; font-weight:bold; }}
    a, a:hover {{ color:#9ec5fe; }}
  </style>
</head>
<body>
  <div class="wrap">
    <div class="card">
      <div style="display:flex; justify-content:space-between; align-items:flex-start; gap:12px;">
        <div>
          <h2 style="margin:0;">{title}</h2>
          <div class="muted">{subtitle}</div>
        </div>
        <div class="muted mono">{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}</div>
      </div>
      <div style="margin-top:14px;">
        {body}
      </div>
    </div>
  </div>
</body>
</html>
"""

def html_table(headers: list[str], rows: list[list[str]]) -> str:
    th = "".join(f"<th>{h}</th>" for h in headers)
    tr = ""
    for r in rows:
        tds = "".join(f"<td>{c}</td>" for c in r)
        tr += f"<tr>{tds}</tr>"
    if not tr:
        tr = f"<tr><td colspan='{len(headers)}' class='muted'>No rows</td></tr>"
    return f"<table><thead><tr>{th}</tr></thead><tbody>{tr}</tbody></table>"

# =========================
# REPORT QUERIES (POS-matching)
# =========================
def build_daily_summary(conn: pyodbc.Connection, start_dt: datetime, end_dt: datetime, day: date) -> str:
    """
    POS-matching Daily Summary core math:
    - Sales base = TransType=101 only
    - Gross Total = Sum(Amount + TaxTotal*(1-TaxInclude))
    - Taxes = Sum(TaxXAmount*(1-TaxInclude))
    - Net = Gross - Taxes
    - Discount = Sum(DiscountAmount) for TransType=101
    - Customers = Count(distinct ReceiptN) where GroupTransType=1
    """
    cur = conn.cursor()
    sql = """
    SELECT
      TransType, GroupTransType, ReceiptN, SubCategoryID,
      Amount, DiscountAmount, TaxInclude,
      Tax1Amount, Tax2Amount, Tax3Amount, Tax4Amount
    FROM Journal
    WHERE DateR >= ? AND DateR < ?
      AND Status = 0
      AND (TransType IN (101,102,103,104,311,501,780) OR GroupTransType IN (1,2))
    """
    rows = cur.execute(sql, start_dt, end_dt).fetchall()
    sales = [r for r in rows if int(r.TransType or 0) == 101]

    def tax_total(r) -> Decimal:
        return d0(r.Tax1Amount) + d0(r.Tax2Amount) + d0(r.Tax3Amount) + d0(r.Tax4Amount)

    def one_minus_taxinclude(r) -> Decimal:
        return Decimal("1") - d0(r.TaxInclude)

    gross_total = sum(d0(r.Amount) + tax_total(r) * one_minus_taxinclude(r) for r in sales)
    gst = sum(d0(r.Tax1Amount) * one_minus_taxinclude(r) for r in sales)
    pst = sum(d0(r.Tax2Amount) * one_minus_taxinclude(r) for r in sales)
    liq = sum(d0(r.Tax3Amount) * one_minus_taxinclude(r) for r in sales)
    tax4 = sum(d0(r.Tax4Amount) * one_minus_taxinclude(r) for r in sales)

    total_taxes = gst + pst + liq + tax4
    net_total = gross_total - total_taxes
    discount = sum(d0(r.DiscountAmount) for r in sales)

    customers = len({int(r.ReceiptN) for r in rows if int(r.GroupTransType or 0) == 1 and r.ReceiptN is not None})
    avg_sale = (net_total / Decimal(customers)) if customers else Decimal("0")

    # Breakdown by SubCategoryID (net style)
    by_subcat: dict[str, Decimal] = {}
    for r in sales:
        key = (str(r.SubCategoryID or "").strip() or "UNSPECIFIED")
        net_line = d0(r.Amount) - tax_total(r) * d0(r.TaxInclude)
        by_subcat[key] = by_subcat.get(key, Decimal("0")) + net_line

    # Build table like POS
    table_rows: list[list[str]] = []
    # total sales at top
    table_rows.append([f"<span class='red'>Total Sales:</span>", f"<span class='right red'>{fmt2(gross_total)}</span>", f"<span class='right red'>{fmt2(gross_total)}</span>"])

    for k in sorted(by_subcat.keys()):
        v = by_subcat[k]
        table_rows.append([k, f"<span class='right mono'>{fmt2(v)}</span>", f"<span class='right mono'>{fmt2(v)}</span>"])

    # totals section
    def add_line(label: str, val: Decimal, red: bool = False):
        cls = "red" if red else ""
        table_rows.append([f"<span class='{cls}'>{label}</span>", f"<span class='right mono {cls}'>{fmt2(val)}</span>", f"<span class='right mono {cls}'>{fmt2(val)}</span>"])

    add_line("Net Total Sales", net_total, red=True)
    add_line("GST 5%", gst)
    add_line("PST 7%", pst)
    add_line("LIQ TAX 10%", liq)
    if tax4 != 0:
        add_line("Tax4", tax4)
    add_line("Total taxes", total_taxes, red=True)
    add_line("Total Sales", gross_total, red=True)
    add_line("Discount", discount)
    table_rows.append(["Customer count", f"<span class='right mono'>{customers}</span>", f"<span class='right mono'>{customers}</span>"])
    add_line("Average Sale", avg_sale)

    body = html_table(
        ["Description", day.strftime("%B %d"), "Total"],
        table_rows
    )
    subtitle = f"Business window: {start_dt} → {end_dt} | DSN={DSN}"
    return html_shell("Summary Report Daily", body, subtitle)

def build_category_report(conn: pyodbc.Connection, start_dt: datetime, end_dt: datetime, day: date) -> str:
    """
    POS Category Report shape:
    Group | Amount | Amount (Taxes Included) | Category Count | Customers
    Uses Category join (C.SubCategoryID) so it won't show UNSPECIFIED unless category itself is null.
    """
    cur = conn.cursor()

    # POS log uses DateR >= start AND DateR <= end (inclusive).
    # We'll do <= end_dt for closer match.
    sql = """
    SELECT
      C.SubCategoryID AS GroupName,
      SUM((J.Amount) - (J.Tax1Amount+J.Tax2Amount+J.Tax3Amount+J.Tax4Amount) * (J.TaxInclude)) AS AmountNet,
      SUM((J.Amount) + (J.Tax1Amount+J.Tax2Amount+J.Tax3Amount+J.Tax4Amount) * (1 - J.TaxInclude)) AS AmountTaxIncl,
      SUM(J.Quantity) AS CategoryCount,
      COUNT(DISTINCT J.ReceiptN) AS Customers
    FROM Journal J
      LEFT OUTER JOIN Category C ON C.CategoryID = J.CategoryID
    WHERE
      J.DateR >= ? AND J.DateR <= ?
      AND J.Status = 0
      AND (1 - C.SalesFlag) = 1
      AND J.TransType IN (101,102,112,111)
    GROUP BY C.SubCategoryID
    ORDER BY C.SubCategoryID
    """

    rows = cur.execute(sql, start_dt, end_dt).fetchall()

    sql_total = """
    SELECT
      SUM((J.Amount) - (J.Tax1Amount+J.Tax2Amount+J.Tax3Amount+J.Tax4Amount) * (J.TaxInclude)) AS AmountNet,
      SUM((J.Amount) + (J.Tax1Amount+J.Tax2Amount+J.Tax3Amount+J.Tax4Amount) * (1 - J.TaxInclude)) AS AmountTaxIncl,
      SUM(J.Quantity) AS CategoryCount,
      COUNT(DISTINCT J.ReceiptN) AS Customers
    FROM Journal J
      LEFT OUTER JOIN Category C ON C.CategoryID = J.CategoryID
    WHERE
      J.DateR >= ? AND J.DateR <= ?
      AND J.Status = 0
      AND (1 - C.SalesFlag) = 1
      AND J.TransType IN (101,102,112,111)
    """
    tot = cur.execute(sql_total, start_dt, end_dt).fetchone()

    table_rows: list[list[str]] = []
    for gname, amt_net, amt_incl, qty, cust in rows:
        label = (str(gname).strip() if gname is not None else "UNSPECIFIED")
        table_rows.append([
            label,
            f"<span class='right mono'>{fmt2(amt_net)}</span>",
            f"<span class='right mono'>{fmt2(amt_incl)}</span>",
            f"<span class='right mono'>{int(qty or 0)}</span>",
            f"<span class='right mono'>{int(cust or 0)}</span>",
        ])

    # TOTAL row
    table_rows.append([
        "<span class='red'>TOTAL</span>",
        f"<span class='right mono red'>{fmt2(tot.AmountNet)}</span>",
        f"<span class='right mono red'>{fmt2(tot.AmountTaxIncl)}</span>",
        f"<span class='right mono red'>{int(tot.CategoryCount or 0)}</span>",
        f"<span class='right mono red'>{int(tot.Customers or 0)}</span>",
    ])

    body = html_table(
        ["Group", "Amount", "Amount (Taxes Included)", "Category Count", "Customers"],
        table_rows
    )
    subtitle = f"Business window: {start_dt} → {end_dt} | DSN={DSN}"
    return html_shell("Category Report", body, subtitle)

# =========================
# Index.json management
# =========================
def load_index() -> dict:
    if INDEX_JSON.exists():
        try:
            return json.loads(INDEX_JSON.read_text(encoding="utf-8"))
        except Exception:
            pass
    return {"latest": None, "dates": []}

def save_index(latest: str, dates: list[str]) -> None:
    payload = {
        "latest": latest,
        "dates": dates,
        "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }
    write_text(INDEX_JSON, json.dumps(payload, indent=2))

# =========================
# Main backfill
# =========================
def main():
    print(f"Repo: {REPO_ROOT}")
    print(f"Generating reports from {START_DATE} to {END_DATE} (inclusive)")
    print(f"Business start: {BUSINESS_DAY_START}")
    ensure_dir(REPORTS_DIR)

    idx = load_index()
    dates_set = set(idx.get("dates") or [])

    generated = 0
    failed: list[str] = []

    with db_conn() as conn:
        d = START_DATE
        while d <= END_DATE:
            ds = d.strftime("%Y-%m-%d")
            out_dir = REPORTS_DIR / ds
            ensure_dir(out_dir)

            start_dt, end_dt = business_window(d)

            try:
                daily_html = build_daily_summary(conn, start_dt, end_dt, d)
                cat_html = build_category_report(conn, start_dt, end_dt, d)

                write_text(out_dir / "summary_daily.html", daily_html)
                write_text(out_dir / "category_report.html", cat_html)

                dates_set.add(ds)
                generated += 1
                if generated % 10 == 0:
                    print(f"  generated {generated} days... (latest: {ds})")

            except Exception as e:
                failed.append(f"{ds}: {e}")
                print(f"  FAILED {ds}: {e}")

            d += timedelta(days=1)

    # Sort dates descending (latest first)
    dates_sorted = sorted(dates_set, reverse=True)
    latest = dates_sorted[0] if dates_sorted else None
    if latest:
        save_index(latest, dates_sorted)

    print(f"\nDone. Generated days: {generated}")
    if failed:
        print("\nFailures:")
        for f in failed[:50]:
            print(" -", f)
        if len(failed) > 50:
            print(f" ... plus {len(failed)-50} more")

    # Optional git push
    if DO_GIT_PUSH:
        print("\nRunning git add/commit/push...")
        try:
            run(["git", "add", "."], cwd=REPO_ROOT)
            # commit may fail if nothing changed; handle gracefully
            r = subprocess.run(["git", "commit", "-m", GIT_COMMIT_MESSAGE], cwd=str(REPO_ROOT), capture_output=True, text=True)
            if r.returncode != 0:
                if "nothing to commit" in (r.stdout + r.stderr).lower():
                    print("Nothing new to commit.")
                else:
                    raise RuntimeError(f"git commit failed:\n{r.stdout}\n{r.stderr}")

            run(["git", "push", "origin", "main"], cwd=REPO_ROOT)
            print("✅ Pushed to GitHub.")
        except Exception as e:
            print(f"⚠️ Git push failed: {e}")
            print("You can still push manually: git push origin main")

    print("\nOpen:")
    print("  https://yan-vibt.github.io/pos-reports/")
    if latest:
        print(f"Latest date folder: reports/{latest}/")

if __name__ == "__main__":
    main()
