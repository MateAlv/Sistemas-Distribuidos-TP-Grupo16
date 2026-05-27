#!/usr/bin/env python3
"""Reference (expected) results for Q1-Q5, shared by precompute_expected.py and
the validators. Comparison is an order-independent, bidirectional multiset
equality. Computed to match the pipeline semantics."""
import csv
import json
import sys
from collections import Counter, defaultdict
from pathlib import Path

_SRC = Path(__file__).resolve().parents[1] / "src"
if str(_SRC) not in sys.path:
    sys.path.insert(0, str(_SRC))

USD_CURRENCY = "US Dollar"

QUERIES = ("q1", "q2", "q3", "q4", "q5")

OUTPUT_COLUMNS = {
    "q1": ["From Bank", "Account", "To Bank", "Account.1", "Amount Paid"],
    "q2": ["From Bank", "Account", "Bank Name", "Amount Paid"],
    "q3": ["From Bank", "Account", "Amount Paid"],
    "q4": ["from_account", "to_account"],
    "q5": ["count"],
}

# Q1: USD transactions paying less than 50.
Q1_MAX_AMOUNT = 50.0
# Q3: candidate USD txns whose amount is < 1/100 of the per-format baseline avg.
Q3_BASELINE = ("2022-09-01", "2022-09-05")
Q3_CANDIDATE = ("2022-09-06", "2022-09-15")
# Q4: USD txns in the window, >= 5 distinct intermediaries per (A, B) pair.
Q4_WINDOW = ("2022-09-01", "2022-09-05")
Q4_MIN_INTERMEDIARIES = 5
# Q5: count of USD-converted < 1 Wire/ACH txns in the window.
Q5_WINDOW = ("2022-09-01", "2022-09-05")
Q5_FORMATS = {"Wire", "ACH"}
Q5_MAX_AMOUNT_USD = 1.0

GENERATOR_VERSION = 1


# --------------------------------------------------------------------------- #
# dataset / column helpers (shared by every query)
# --------------------------------------------------------------------------- #
def dataset_paths(dataset_dir, trans_name):
    """Return (transactions_file, accounts_file) for a dataset directory."""
    d = Path(dataset_dir)
    trans = d / trans_name
    if trans_name.endswith("_Trans.csv"):
        accounts = d / trans_name.replace("_Trans.csv", "_accounts.csv")
    else:
        accounts = d / "accounts.csv"
    return trans, accounts


def expected_dir(dataset_dir):
    return Path(dataset_dir) / "expected_results"


def expected_path(dataset_dir, query):
    return expected_dir(dataset_dir) / f"{query}.csv"


def _column_index_after(header, name, start_index):
    for index in range(start_index + 1, len(header)):
        if header[index] == name:
            return index
    raise ValueError(f"missing required column {name!r} after index {start_index}")


def _columns(header):
    from_bank = header.index("From Bank")
    to_bank = header.index("To Bank")
    return {
        "from_bank": from_bank,
        "from_account": _column_index_after(header, "Account", from_bank),
        "to_bank": to_bank,
        "to_account": _column_index_after(header, "Account", to_bank),
        "amount": header.index("Amount Paid"),
        "currency": header.index("Payment Currency"),
        "date": header.index("Timestamp"),
        "payment_format": header.index("Payment Format"),
    }


def _normalize_date(value):
    return value[:10].replace("/", "-")


def _normalize_bank_id(value):
    value = (value or "").strip()
    if not value:
        return ""
    if value.isdigit():
        return str(int(value))
    return value.lstrip("0") or "0"


# --------------------------------------------------------------------------- #
# per-query reference computation -> list of normalized output-row tuples
# (each tuple matches OUTPUT_COLUMNS[query], amounts formatted as :.2f)
# --------------------------------------------------------------------------- #
def compute_q1(trans_file, _accounts_file=None):
    rows = []
    with open(trans_file, "r") as f:
        reader = csv.reader(f)
        col = _columns(next(reader))
        for row in reader:
            if row[col["currency"]].strip() != USD_CURRENCY:
                continue
            amount = float(row[col["amount"]])
            if amount < Q1_MAX_AMOUNT:
                rows.append((
                    row[col["from_bank"]].strip(),
                    row[col["from_account"]].strip(),
                    row[col["to_bank"]].strip(),
                    row[col["to_account"]].strip(),
                    f"{amount:.2f}",
                ))
    return rows


def compute_q2(trans_file, accounts_file=None):
    bank_names = {}
    if accounts_file and Path(accounts_file).exists():
        with open(accounts_file, "r") as f:
            for row in csv.DictReader(f):
                bank_id = _normalize_bank_id(row["Bank ID"])
                if bank_id:
                    bank_names.setdefault(bank_id, row["Bank Name"].strip())

    max_by_bank = {}
    with open(trans_file, "r") as f:
        reader = csv.reader(f)
        col = _columns(next(reader))
        for row in reader:
            if row[col["currency"]].strip() != USD_CURRENCY:
                continue
            bank_id = row[col["from_bank"]].strip()
            amount = float(row[col["amount"]])
            if bank_id not in max_by_bank or amount > max_by_bank[bank_id][1]:
                max_by_bank[bank_id] = (row[col["from_account"]].strip(), amount)

    rows = []
    for bank_id, (account, amount) in max_by_bank.items():
        bank_name = bank_names.get(_normalize_bank_id(bank_id), "")
        rows.append((bank_id, account, bank_name, f"{amount:.2f}"))
    return rows


def compute_q3(trans_file, _accounts_file=None):
    sums = defaultdict(float)
    counts = defaultdict(int)
    candidates = []
    with open(trans_file, "r") as f:
        reader = csv.reader(f)
        col = _columns(next(reader))
        for row in reader:
            if row[col["currency"]].strip() != USD_CURRENCY:
                continue
            date = _normalize_date(row[col["date"]])
            fmt = row[col["payment_format"]].strip()
            amount = float(row[col["amount"]])
            if Q3_BASELINE[0] <= date <= Q3_BASELINE[1]:
                sums[fmt] += amount
                counts[fmt] += 1
            elif Q3_CANDIDATE[0] <= date <= Q3_CANDIDATE[1]:
                candidates.append((
                    fmt,
                    row[col["from_bank"]].strip(),
                    row[col["from_account"]].strip(),
                    amount,
                ))

    averages = {fmt: sums[fmt] / counts[fmt] for fmt in counts}
    rows = []
    for fmt, from_bank, from_account, amount in candidates:
        avg = averages.get(fmt)
        if avg is not None and amount < (avg / 100):
            rows.append((from_bank, from_account, f"{amount:.2f}"))
    return rows


def compute_q4(trans_file, _accounts_file=None):
    # Mirrors the scatter-gather linker/detector: a txn X->Y feeds incoming[Y]
    # (A->M with M=Y) and outgoing[X] (M->B with M=X); a pair (A,B) is emitted
    # once it has >= Q4_MIN_INTERMEDIARIES distinct M with both A->M and M->B.
    incoming = defaultdict(set)
    outgoing = defaultdict(set)
    with open(trans_file, "r") as f:
        reader = csv.reader(f)
        col = _columns(next(reader))
        for row in reader:
            if row[col["currency"]].strip() != USD_CURRENCY:
                continue
            date = _normalize_date(row[col["date"]])
            if not (Q4_WINDOW[0] <= date <= Q4_WINDOW[1]):
                continue
            src = row[col["from_account"]].strip()
            dst = row[col["to_account"]].strip()
            incoming[dst].add(src)
            outgoing[src].add(dst)

    intermediaries = defaultdict(set)
    for m in set(incoming) & set(outgoing):
        for a in incoming[m]:
            for b in outgoing[m]:
                intermediaries[(a, b)].add(m)

    return [
        (a, b)
        for (a, b), ms in intermediaries.items()
        if len(ms) >= Q4_MIN_INTERMEDIARIES
    ]


RATES_CACHE = Path("data/rates/cache.json")

CURRENCY_NAME_TO_ISO = {
    "US Dollar": "USD", "Euro": "EUR", "UK Pound": "GBP", "Yen": "JPY",
    "Swiss Franc": "CHF", "Canadian Dollar": "CAD", "Australian Dollar": "AUD",
    "Mexican Peso": "MXN", "Brazil Real": "BRL", "Yuan": "CNY", "Rupee": "INR",
    "Ruble": "RUB", "Saudi Riyal": "SAR", "Swedish Krona": "SEK",
    "New Zealand Dollar": "NZD", "Singapore Dollar": "SGD",
    "Hong Kong Dollar": "HKD", "Norwegian Krone": "NOK",
    "South Korean Won": "KRW", "Turkish Lira": "TRY",
    "South African Rand": "ZAR", "Thai Baht": "THB", "Polish Zloty": "PLN",
    "Czech Koruna": "CZK", "Shekel": "ILS", "Philippine Peso": "PHP",
    "Indonesian Rupiah": "IDR", "Malaysian Ringgit": "MYR",
    "Hungarian Forint": "HUF", "Icelandic Krona": "ISK", "Croatian Kuna": "HRK",
    "Romanian Leu": "RON", "Danish Krone": "DKK", "Bulgarian Lev": "BGN",
    "Bitcoin": "BTC",
}


def _load_rates():
    from common.rates.q5_reference_rates import Q5_REFERENCE_RATES
    if not RATES_CACHE.exists():
        return dict(Q5_REFERENCE_RATES)
    with open(RATES_CACHE, "r") as f:
        rates = json.load(f)
    for date, day_rates in Q5_REFERENCE_RATES.items():
        rates.setdefault(date, {}).update(day_rates)
    return rates


def _convert_to_usd(amount, currency_name, date, rates):
    if currency_name == USD_CURRENCY:
        return amount
    iso = CURRENCY_NAME_TO_ISO.get(currency_name)
    if iso is None or iso == "USD":
        return amount
    day_rates = rates.get(date) if rates else None
    if day_rates is None:
        return None
    rate = day_rates.get(iso)
    if rate is None:
        return None
    return amount * (1.0 / float(rate))


def compute_q5(trans_file, _accounts_file=None):
    rates = _load_rates()
    count = 0
    with open(trans_file, "r") as f:
        for tx in csv.DictReader(f):
            if tx["Payment Format"].strip() not in Q5_FORMATS:
                continue
            date = _normalize_date(tx["Timestamp"])
            if not (Q5_WINDOW[0] <= date <= Q5_WINDOW[1]):
                continue
            amount = float(tx["Amount Paid"])
            usd = _convert_to_usd(amount, tx["Payment Currency"].strip(), date, rates)
            if usd is None:
                continue
            if usd < Q5_MAX_AMOUNT_USD:
                count += 1
    return [(str(count),)]


_COMPUTE = {
    "q1": compute_q1,
    "q2": compute_q2,
    "q3": compute_q3,
    "q4": compute_q4,
    "q5": compute_q5,
}


def compute(query, dataset_dir, trans_name):
    """Compute the reference rows for ``query`` directly from the dataset."""
    trans_file, accounts_file = dataset_paths(dataset_dir, trans_name)
    if not trans_file.exists():
        raise FileNotFoundError(f"transactions file not found: {trans_file}")
    return _COMPUTE[query](trans_file, accounts_file)


# --------------------------------------------------------------------------- #
# normalization + I/O shared by reference files and pipeline output files
# --------------------------------------------------------------------------- #
def normalize_row(query, fields):
    """Normalize a positional CSV row (reference or pipeline output) into a comparable tuple."""
    f = [x.strip() for x in fields]
    if query == "q1":
        return (f[0], f[1], f[2], f[3], f"{float(f[4]):.2f}")
    if query == "q2":
        # Account is excluded: under a tie for a bank's max amount, which
        # account wins is non-deterministic. Key on (bank id, bank name, amount).
        return (f[0], f[2], f"{float(f[3]):.2f}")
    if query == "q3":
        return (f[0], f[1], f"{float(f[2]):.2f}")
    if query == "q4":
        return (f[0], f[1])
    if query == "q5":
        return ("count", str(int(f[0])))
    raise ValueError(f"unknown query {query!r}")


def _data_rows(path):
    """Yield CSV rows of a results/expected file, skipping ``#`` comment lines."""
    with open(path, "r") as f:
        lines = [line for line in f if not line.startswith("#")]
    yield from csv.reader(lines)


def load_counter(query, path):
    """Load a results/expected CSV into a multiset of normalized rows (Q5 sums to one count)."""
    rows = list(_data_rows(path))
    if not rows:
        return Counter()
    data = [r for r in rows[1:] if r]  # rows[0] is the header
    if query == "q5":
        total = sum(int(r[0].strip()) for r in data)
        return Counter({("count", str(total)): 1})
    return Counter(normalize_row(query, r) for r in data)


def expected_counter(query, dataset_dir, trans_name):
    """Reference multiset for ``query``: prefer the precomputed file, else compute."""
    path = expected_path(dataset_dir, query)
    if path.exists():
        return load_counter(query, path)
    rows = compute(query, dataset_dir, trans_name)
    return _rows_to_counter(query, rows)


def _rows_to_counter(query, rows):
    if query == "q5":
        total = sum(int(r[0]) for r in rows)
        return Counter({("count", str(total)): 1})
    return Counter(normalize_row(query, r) for r in rows)


def write_expected(query, rows, path, source_name):
    """Write reference rows to ``path`` with a provenance comment header."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="") as f:
        f.write(
            f"# generated by precompute_expected.py v{GENERATOR_VERSION} "
            f"from {source_name}\n"
        )
        writer = csv.writer(f)
        writer.writerow(OUTPUT_COLUMNS[query])
        writer.writerows(rows)


def compare(expected, actual):
    """Return (missing, unexpected) multisets between expected and actual."""
    return expected - actual, actual - expected


def _describe(query, counter):
    if query == "q5":
        _, total = next(iter(counter))
        return f"expected count = {total}"
    return f"{sum(counter.values())} expected rows"


def _summarize_actual(query, counter):
    if query == "q5":
        _, total = next(iter(counter)) if counter else ("count", "0")
        return f"count = {total}"
    return f"{sum(counter.values())} rows"


def validate_query(query, dataset_dir, trans_name, output_dir="data/output"):
    """Compare every results_<query>_*.csv against the reference multiset; True iff all match."""
    try:
        expected = expected_counter(query, dataset_dir, trans_name)
    except Exception as e:
        print(f"ERROR computing/loading expected {query} rows: {e}")
        return False

    src = expected_path(dataset_dir, query)
    print(f"Reference: {src if src.exists() else 'computed from dataset'}")
    print(f"{_describe(query, expected)}")

    output_files = sorted(Path(output_dir).glob(f"results_{query}_*.csv"))
    if not output_files:
        print(f"ERROR: no {query} output files found in {output_dir}")
        return False
    print(f"Found {len(output_files)} output file(s)")

    all_ok = True
    for output_file in output_files:
        print(f"\n  Reading: {output_file.name}")
        try:
            actual = load_counter(query, output_file)
        except Exception as e:  # noqa: BLE001
            print(f"    ERROR reading {output_file.name}: {e}")
            all_ok = False
            continue

        print(f"    {_summarize_actual(query, actual)}")
        missing, unexpected = compare(expected, actual)
        if missing or unexpected:
            print(
                f"    ERROR: differs from reference "
                f"(missing={sum(missing.values())}, "
                f"unexpected={sum(unexpected.values())})"
            )
            for row in list(missing)[:5]:
                print(f"      missing: {row}")
            for row in list(unexpected)[:5]:
                print(f"      unexpected: {row}")
            all_ok = False
        else:
            print("    ✓ matches reference")

    if all_ok:
        print(f"\nAll {len(output_files)} client outputs match the reference")
    return all_ok
