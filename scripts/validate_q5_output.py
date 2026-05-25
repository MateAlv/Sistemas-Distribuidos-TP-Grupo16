#!/usr/bin/env python3
"""
Valida que el flujo Q5 funcionó correctamente.
Q5: Contar transacciones en [2022-09-01, 2022-09-05] con formato Wire o ACH
cuyo monto convertido a USD sea menor a 1.
"""
import os
import sys
import csv
import json
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from common.rates.q5_reference_rates import Q5_REFERENCE_RATES

Q5_START_DATE = "2022-09-01"
Q5_END_DATE = "2022-09-05"
Q5_FORMATS = {"Wire", "ACH"}
Q5_MAX_AMOUNT_USD = 1.0
USD_CURRENCY = "US Dollar"
DATASET_DIR = os.environ.get("Q5_DATASET_DIR", "data/datasets/client-1/LI-Small")
DATASET_TRANS = os.environ.get("Q5_DATASET_TRANS", "LI-Small_Trans.csv")
RATES_CACHE = Path("data/rates/cache.json")

CURRENCY_NAME_TO_ISO = {
    "US Dollar": "USD",
    "Euro": "EUR",
    "UK Pound": "GBP",
    "Yen": "JPY",
    "Swiss Franc": "CHF",
    "Canadian Dollar": "CAD",
    "Australian Dollar": "AUD",
    "Mexican Peso": "MXN",
    "Brazil Real": "BRL",
    "Yuan": "CNY",
    "Rupee": "INR",
    "Ruble": "RUB",
    "Saudi Riyal": "SAR",
    "Swedish Krona": "SEK",
    "New Zealand Dollar": "NZD",
    "Singapore Dollar": "SGD",
    "Hong Kong Dollar": "HKD",
    "Norwegian Krone": "NOK",
    "South Korean Won": "KRW",
    "Turkish Lira": "TRY",
    "South African Rand": "ZAR",
    "Thai Baht": "THB",
    "Polish Zloty": "PLN",
    "Czech Koruna": "CZK",
    "Shekel": "ILS",
    "Philippine Peso": "PHP",
    "Indonesian Rupiah": "IDR",
    "Malaysian Ringgit": "MYR",
    "Hungarian Forint": "HUF",
    "Icelandic Krona": "ISK",
    "Croatian Kuna": "HRK",
    "Romanian Leu": "RON",
    "Danish Krone": "DKK",
    "Bulgarian Lev": "BGN",
    "Bitcoin": "BTC",
}


def load_rates():
    if not RATES_CACHE.exists():
        return dict(Q5_REFERENCE_RATES)
    with open(RATES_CACHE, "r") as f:
        rates = json.load(f)
    for date, day_rates in Q5_REFERENCE_RATES.items():
        rates.setdefault(date, {}).update(day_rates)
    return rates


def convert_to_usd(amount, currency_name, date, rates):
    if currency_name == USD_CURRENCY:
        return amount
    iso = CURRENCY_NAME_TO_ISO.get(currency_name)
    if iso is None or iso == "USD":
        return amount
    day_rates = rates.get(date) if rates else None
    if day_rates is None:
        return None
    rate_to_currency = day_rates.get(iso)
    if rate_to_currency is None:
        return None
    return amount * (1.0 / float(rate_to_currency))


def in_date_range(date_str):
    normalized = date_str[:10].replace("/", "-")
    return Q5_START_DATE <= normalized <= Q5_END_DATE


def validate_q5_results():
    dataset_file = Path(DATASET_DIR) / DATASET_TRANS
    output_dir = Path("data/output")

    if not dataset_file.exists():
        print(f"ERROR: Dataset not found: {dataset_file}")
        return False

    if not output_dir.exists():
        print("ERROR: No output directory found. Test may have failed.")
        return False

    output_files = list(output_dir.glob("results_q5_*.csv"))
    if not output_files:
        print("ERROR: No Q5 output files found in data/output")
        return False

    print(f"Found {len(output_files)} output file(s)")

    rates = load_rates()
    if rates:
        print("Loaded rates cache for currency conversion")
    else:
        print("WARNING: No rates cache found, only USD transactions will be counted")

    original_transactions = []
    try:
        with open(dataset_file, "r") as f:
            reader = csv.DictReader(f)
            for row in reader:
                original_transactions.append(row)
    except Exception as e:
        print(f"ERROR reading dataset: {e}")
        return False

    print(f"Dataset has {len(original_transactions)} transactions")

    expected_count = 0
    skipped_conversion = 0
    for tx in original_transactions:
        fmt = tx["Payment Format"].strip()
        if fmt not in Q5_FORMATS:
            continue

        date = tx["Timestamp"][:10].replace("/", "-")
        if not in_date_range(date):
            continue

        currency = tx["Payment Currency"].strip()
        amount = float(tx["Amount Paid"])

        amount_usd = convert_to_usd(amount, currency, date, rates)
        if amount_usd is None:
            skipped_conversion += 1
            continue

        if amount_usd < Q5_MAX_AMOUNT_USD:
            expected_count += 1

    if skipped_conversion > 0:
        print(f"WARNING: Skipped {skipped_conversion} transactions (missing rate)")

    print(f"Expected Q5 count: {expected_count}")

    # Multiclient: cada client_X procesa el mismo dataset y debe emitir su
    # propio count que matchea expected. NO se suman entre clientes.
    mismatches = 0
    for output_file in sorted(output_files):
        print(f"\n  Reading: {output_file.name}")
        try:
            with open(output_file, "r") as f:
                reader = csv.DictReader(f)
                rows = list(reader)
        except Exception as e:
            print(f"ERROR reading {output_file}: {e}")
            return False

        if not rows:
            print(f"    ERROR: {output_file.name} is empty")
            mismatches += 1
            continue

        client_total = sum(int(row["count"]) for row in rows)
        for row in rows:
            print(f"    count = {row['count']}")

        if client_total == 0:
            print(f"    ERROR: {output_file.name} count is 0")
            mismatches += 1
        elif client_total != expected_count:
            print(
                f"    ERROR: {output_file.name} count {client_total} "
                f"!= expected {expected_count}"
            )
            mismatches += 1
        else:
            print(f"    ✓ matches expected ({expected_count})")

    if mismatches > 0:
        print(f"\nERROR: {mismatches} of {len(output_files)} client outputs do not match")
        return False

    print(
        f"\nAll {len(output_files)} client outputs match expected count "
        f"({expected_count})"
    )
    return True


if __name__ == "__main__":
    print("=" * 60)
    print("Q5 FLOW VALIDATION")
    print("=" * 60)
    success = validate_q5_results()
    print("=" * 60)
    if success:
        print("Q5 TEST PASSED")
        sys.exit(0)
    else:
        print("Q5 TEST FAILED")
        sys.exit(1)
