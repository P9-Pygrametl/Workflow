# 1. create baseline datawarehouse in csv
# 2. create nogil datawarehouse in csv
# 3. compare the two datawarehouses for semantic correctness

import os
from pathlib import Path
import sys

from dotenv import load_dotenv
import pandas as pd
import psycopg

from baseline import main as baseline_main
from nogil import main as nogil_main

ROOT = Path(__file__).resolve().parent.parent

# Allow imports from the repository root when running this file directly.
sys.path.insert(0, str(ROOT))

load_dotenv(ROOT / ".env")

DW_DATABASE = os.getenv("DW_DATABASE")
DW_HOST = os.getenv("DW_HOST", "localhost")
DW_PORT_RAW = os.getenv("DW_PORT", "5432")
try:
    DW_PORT = int(DW_PORT_RAW)
except ValueError as error:
    raise ValueError(
        f"DW_PORT must be an integer, got {DW_PORT_RAW!r}"
    ) from error


def check_dimension_content(f1, f2, filename, key_col, sort_cols):
    print(f"\n=== Comparing Dimension: {filename} (Content Verification) ===")
    df1 = pd.read_csv(f1 / filename)
    df2 = pd.read_csv(f2 / filename)

    # Drop surrogate key column if present
    if key_col in df1.columns:
        df1 = df1.drop(columns=[key_col])
    if key_col in df2.columns:
        df2 = df2.drop(columns=[key_col])

    # Sort by natural attributes
    df1 = df1.sort_values(by=sort_cols).reset_index(drop=True)
    df2 = df2.sort_values(by=sort_cols).reset_index(drop=True)

    try:
        pd.testing.assert_frame_equal(df1, df2, check_dtype=False)
        print("  Result: EXACT MATCH")
        return True
    except AssertionError as e:
        print("  Result: MISMATCH")
        print(f"  Details: {str(e).splitlines()[0]}")
        return False


def check_fact_table_semantic(f1, f2):
    print("\n=== Comparing Fact Table: testresults (Semantic Verification) ===")

    # Load baseline tables
    base_fact = pd.read_csv(f1 / "testresults_export.csv")
    base_page = pd.read_csv(f1 / "page_export.csv")
    base_test = pd.read_csv(f1 / "test_export.csv")
    base_date = pd.read_csv(f1 / "date_export.csv")

    # Load NoGIL tables
    nogil_fact = pd.read_csv(f2 / "testresults_export.csv")
    nogil_page = pd.read_csv(f2 / "page_export.csv")
    nogil_test = pd.read_csv(f2 / "test_export.csv")
    nogil_date = pd.read_csv(f2 / "date_export.csv")

    def denormalize(fact, page, test, date):
        # Join facts back to dimensions on surrogate keys
        m = fact.merge(page[["pageid", "url"]], on="pageid", how="inner")
        m = m.merge(test[["testid", "testname"]], on="testid", how="inner")
        m = m.merge(date[["dateid", "date"]], on="dateid", how="inner")

        # Keep only business keys and measures
        cols = ["url", "testname", "date", "errors"]
        return m[cols].sort_values(by=cols).reset_index(drop=True)

    denorm_base = denormalize(base_fact, base_page, base_test, base_date)
    denorm_nogil = denormalize(nogil_fact, nogil_page, nogil_test, nogil_date)

    try:
        pd.testing.assert_frame_equal(denorm_base, denorm_nogil, check_dtype=False)
        print("  Result: EXACT MATCH")
        return True
    except AssertionError as e:
        print("  Result: MISMATCH")
        print(f"  Details: {str(e).splitlines()[0]}")
        return False


def checker():
    print("=== Starting correctness check ===")
    correctness_check_dir = Path(__file__).resolve().parent
    f1 = correctness_check_dir / "baselineCSV/"
    f2 = correctness_check_dir / "nogilCSV/"
    print(f"Baseline directory: {f1}")
    print(f"Nogil directory: {f2}")

    # 1. Compare Dimensions by Business Keys
    check_dimension_content(
        f1, f2, "date_export.csv", key_col="dateid", sort_cols=["date"]
    )
    check_dimension_content(
        f1, f2, "test_export.csv", key_col="testid", sort_cols=["testname"]
    )
    check_dimension_content(
        f1,
        f2,
        "page_export.csv",
        key_col="pageid",
        sort_cols=["url", "version", "validfrom"],
    )

    # 2. Compare Fact Table via Semantic De-normalization
    check_fact_table_semantic(f1, f2)


def reset_warehouse():
    """Reset the PostgreSQL data warehouse using the star schema."""
    schema = (ROOT / "starschema.sql").read_text()

    connection = psycopg.connect(
        host=DW_HOST,
        port=DW_PORT,
        dbname=DW_DATABASE,
        user=os.getenv("USERNAME"),
    )

    try:
        with connection.cursor() as cursor:
            cursor.execute(schema)

        connection.commit()
    finally:
        connection.close()


def main():
    reset_warehouse()
    baseline_main()
    reset_warehouse()
    nogil_main()
    checker()


if __name__ == "__main__":
    main()