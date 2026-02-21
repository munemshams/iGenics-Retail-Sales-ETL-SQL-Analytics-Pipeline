import os
import re
import glob
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from dotenv import load_dotenv


RAW_DIR = os.path.join("data", "raw")
OUT_DIR = "outputs"

TABLE_RAW = "weekly_summary_raw"
TABLE_CLEAN = "weekly_metrics_clean"

# ====== TIME SCOPE SETTINGS ======
CUTOFF_2025_WEEK = 31   # keep only Week 1–31 for 2025 (January–July)


def ensure_dirs():
    os.makedirs(OUT_DIR, exist_ok=True)


def mysql_engine() -> Engine:
    load_dotenv()
    host = os.getenv("MYSQL_HOST", "localhost")
    port = os.getenv("MYSQL_PORT", "3306")
    user = os.getenv("MYSQL_USER", "root")
    pw = os.getenv("MYSQL_PASSWORD", "")
    db = os.getenv("MYSQL_DB", "marketology_retail")

    # Create DB if missing
    server_engine = create_engine(f"mysql+pymysql://{user}:{pw}@{host}:{port}/")
    with server_engine.begin() as conn:
        conn.execute(text(f"CREATE DATABASE IF NOT EXISTS {db};"))

    return create_engine(f"mysql+pymysql://{user}:{pw}@{host}:{port}/{db}")


def run_sql_query(engine: Engine, query: str) -> pd.DataFrame:
    """
    Run a SQL query against the MySQL database and return the result as a DataFrame.
    """
    with engine.begin() as conn:
        return pd.read_sql(text(query), conn)


def list_csvs():
    files = sorted(glob.glob(os.path.join(RAW_DIR, "*.csv")))
    if not files:
        raise FileNotFoundError(
            f"No CSV found in {RAW_DIR}. Put your weekly summary CSVs into data/raw/."
        )
    return files


def read_csv_any(path: str) -> pd.DataFrame:
    # Try common encodings
    for enc in ["utf-8", "utf-8-sig", "cp1252"]:
        try:
            return pd.read_csv(path, encoding=enc)
        except Exception:
            continue
    return pd.read_csv(path)


def tidy_weekly_summary(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    # Expect first column to be metric names like "CB Revenue"
    first_col = df.columns[0]
    df = df.dropna(subset=[first_col])

    # Drop "Unnamed" columns that show up at the end
    df = df.loc[:, ~df.columns.astype(str).str.contains(r"^Unnamed", regex=True)]

    # Melt wide weeks into long format
    m = df.melt(id_vars=[first_col], var_name="week_label", value_name="value_raw")
    m = m.dropna(subset=["week_label"])

    # Parse week number + year from "Week 01, 2024"
    pat = re.compile(r"Week\s*(\d{1,2})\s*,\s*(\d{4})", re.IGNORECASE)

    def parse_week_year(lbl):
        mo = pat.search(str(lbl))
        if not mo:
            return (np.nan, np.nan)
        return (int(mo.group(1)), int(mo.group(2)))

    wkyr = m["week_label"].apply(parse_week_year)
    m["week"] = wkyr.apply(lambda x: x[0])
    m["year"] = wkyr.apply(lambda x: x[1])
    m = m.dropna(subset=["week", "year"])

    # Parse currency values like $2,345 or (1,234)
    v = (
        m["value_raw"]
        .astype(str)
        .str.replace(r"[\$,]", "", regex=True)
        .str.replace(r"\(([^)]+)\)", r"-\1", regex=True)
        .str.strip()
    )
    m["value"] = pd.to_numeric(v, errors="coerce")

    m = m.rename(columns={first_col: "metric"})

    # Channel prefix: CB, BG, DS, Total
    m["channel"] = m["metric"].str.extract(r"^(CB|BG|DS|Total)", expand=False)

    # metric_name: e.g. CB Revenue -> Revenue
    m["metric_name"] = m["metric"].str.replace(r"^(CB|BG|DS|Total)\s*", "", regex=True)

    return m[["year", "week", "channel", "metric_name", "value", "metric"]]


def plot_weekly_total_metric(clean: pd.DataFrame, metric_name: str, outpath: str):
    df = clean[clean["metric_name"].str.lower() == metric_name.lower()].copy()
    grp = (
        df.groupby(["year", "week"], as_index=False)["value"]
        .sum()
        .sort_values(["year", "week"])
    )
    labels = (
        grp["year"].astype(int).astype(str)
        + "-W"
        + grp["week"].astype(int).astype(str).str.zfill(2)
    )

    plt.figure()
    plt.plot(labels, grp["value"])
    plt.xticks(rotation=60, ha="right")
    plt.title(f"Weekly Total {metric_name}")
    plt.xlabel("Year-Week")
    plt.ylabel(metric_name)
    plt.tight_layout()
    plt.savefig(outpath, dpi=200)
    plt.close()


def plot_by_year(clean: pd.DataFrame, metric_name: str, outpath_prefix: str):
    df = clean[clean["metric_name"].str.lower() == metric_name.lower()].copy()
    grp = df.groupby(["year", "week"], as_index=False)["value"].sum()

    for yr in sorted(grp["year"].unique()):
        g = grp[grp["year"] == yr].sort_values("week")

        plt.figure()
        plt.plot(g["week"], g["value"])
        plt.title(f"Weekly {metric_name} — {int(yr)}")
        plt.xlabel("Week")
        plt.ylabel(metric_name)
        plt.tight_layout()
        plt.savefig(f"{outpath_prefix}_{int(yr)}.png", dpi=200)
        plt.close()


def main():
    ensure_dirs()

    # === 1. Ingest and combine raw CSVs ===
    files = list_csvs()
    frames = []
    for f in files:
        df = read_csv_any(f)
        df["source_file"] = os.path.basename(f)
        frames.append(df)

    raw = pd.concat(frames, ignore_index=True)

    # === 2. Clean and transform to tidy weekly metrics ===
    clean = tidy_weekly_summary(raw)

    # ====== APPLY SCOPE: keep all 2024 + only up to end of July 2025 ======
    clean = clean[
        (clean["year"] == 2024)
        | ((clean["year"] == 2025) & (clean["week"] <= CUTOFF_2025_WEEK))
    ].copy()

    # === 3. Save cleaned CSV ===
    clean_csv = os.path.join(OUT_DIR, "weekly_metrics_clean.csv")
    clean.to_csv(clean_csv, index=False)

    # === 4. Load raw + clean tables into MySQL ===
    engine = mysql_engine()
    raw.to_sql(TABLE_RAW, con=engine, if_exists="replace", index=False)
    clean.to_sql(TABLE_CLEAN, con=engine, if_exists="replace", index=False)

    # === 5. Calculate metrics in Python (for quick reference) ===
    weeks_total = clean[["year", "week"]].drop_duplicates().shape[0]
    weeks_by_year = (
        clean[["year", "week"]]
        .drop_duplicates()
        .groupby("year")
        .size()
        .to_dict()
    )

    revenue = (
        clean[clean["metric_name"].str.lower() == "revenue"]
        .groupby("year")["value"]
        .sum()
        .to_dict()
    )
    total_net = (
        clean[clean["metric"].str.lower() == "total net income"]
        .groupby("year")["value"]
        .sum()
        .to_dict()
    )

    # === 6. Charts (Revenue & Net Income by year) ===
    plot_by_year(clean, "Revenue", os.path.join(OUT_DIR, "weekly_revenue"))
    plot_by_year(clean, "Net Income", os.path.join(OUT_DIR, "weekly_net_income"))

    # === 7. SQL Analytics: run queries directly on MySQL ===
    queries = {
        "total_revenue_per_year": """
            SELECT year, SUM(value) AS total_revenue
            FROM weekly_metrics_clean
            WHERE metric_name = 'Revenue'
            GROUP BY year
            ORDER BY year;
        """,
        "total_net_income_per_year": """
            SELECT year, SUM(value) AS net_income
            FROM weekly_metrics_clean
            WHERE metric_name = 'Net Income'
            GROUP BY year
            ORDER BY year;
        """,
        "most_profitable_week": """
            SELECT year, week, value AS net_income
            FROM weekly_metrics_clean
            WHERE metric_name = 'Net Income'
            ORDER BY value DESC
            LIMIT 1;
        """,
        "avg_weekly_revenue": """
            SELECT year, AVG(value) AS avg_weekly_revenue
            FROM weekly_metrics_clean
            WHERE metric_name = 'Revenue'
            GROUP BY year;
        """,
        "weekly_revenue": """
            SELECT year, week, SUM(value) AS weekly_revenue
            FROM weekly_metrics_clean
            WHERE metric_name = 'Revenue'
            GROUP BY year, week
            ORDER BY year, week;
        """,
    }

    sql_results = {}
    for name, query in queries.items():
        df_sql = run_sql_query(engine, query)
        sql_results[name] = df_sql
        df_sql.to_csv(os.path.join(OUT_DIR, f"{name}.csv"), index=False)

    # === 8. Portfolio metrics file ===
    summary_path = os.path.join(OUT_DIR, "project_summary.txt")
    with open(summary_path, "w", encoding="utf-8") as f:
        f.write("PROJECT SUMMARY:\n")
        f.write(f"- Files ingested: {len(files)}\n")
        f.write(f"- Total weeks (after scope filter): {weeks_total}\n")
        f.write(f"- Weeks by year: {weeks_by_year}\n")
        f.write(f"- Total revenue by year (Python calc): {revenue}\n")
        f.write(
            f"- Total net income (Total Net Income, Python calc): {total_net}\n"
        )
        f.write("\nSQL ANALYTICS OUTPUTS SAVED:\n")
        for name in queries.keys():
            f.write(f"- {name}.csv\n")

    print("DONE ✅")
    print(f"- MySQL tables created: {TABLE_RAW}, {TABLE_CLEAN}")
    print(f"- Outputs saved in: {OUT_DIR}/")
    print(f"- Metrics written to: {summary_path}")
    print("- SQL analytics CSVs saved for:")
    for name in queries.keys():
        print(f"  • {name}.csv")


if __name__ == "__main__":
    main()
