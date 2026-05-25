from pathlib import Path
import csv


PROJECT_ROOT = Path(__file__).resolve().parents[1]

RAW_PATH = PROJECT_ROOT / "data" / "raw" / "flight_data_2024.csv"
OUTPUT_DIR = PROJECT_ROOT / "data" / "hive" / "flights_clean"
OUTPUT_PATH = OUTPUT_DIR / "flights_clean_hive.psv"


REQUIRED_COLUMNS = [
    "op_unique_carrier",
    "origin",
    "dest",
    "month",
    "dep_delay",
    "arr_delay",
    "cancelled",
    "cancellation_code",
    "carrier_delay",
    "weather_delay",
    "nas_delay",
    "security_delay",
    "late_aircraft_delay",
]


OUTPUT_COLUMNS = [
    "airline",
    "origin",
    "dest",
    "route",
    "month",
    "dep_delay",
    "arr_delay",
    "cancelled",
    "cancellation_code",
    "cause",
]


def clean_text(value):
    if value is None:
        return ""

    value = str(value).strip()

    if value.lower() in {"nan", "none", "null"}:
        return ""

    return value.upper()


def clean_number(value):
    if value is None:
        return ""

    value = str(value).strip()

    if value == "" or value.lower() in {"nan", "none", "null"}:
        return ""

    return value


def cancelled_to_int(value):
    value = str(value).strip().lower()

    if value in {"1", "1.0", "true", "yes", "y"}:
        return "1"

    return "0"


def to_float(value):
    try:
        if value is None:
            return 0.0

        value = str(value).strip()

        if value == "":
            return 0.0

        return float(value)

    except ValueError:
        return 0.0


def infer_cause(row):
    cancellation_code = clean_text(row.get("cancellation_code", ""))

    if cancelled_to_int(row.get("cancelled", "")) == "1" and cancellation_code:
        return f"CANCEL_{cancellation_code}"

    causes = {
        "CARRIER_DELAY": to_float(row.get("carrier_delay")),
        "WEATHER_DELAY": to_float(row.get("weather_delay")),
        "NAS_DELAY": to_float(row.get("nas_delay")),
        "SECURITY_DELAY": to_float(row.get("security_delay")),
        "LATE_AIRCRAFT_DELAY": to_float(row.get("late_aircraft_delay")),
    }

    best_cause = max(causes.items(), key=lambda item: item[1])

    if best_cause[1] > 0:
        return best_cause[0]

    return "UNKNOWN"


def main():
    if not RAW_PATH.exists():
        raise FileNotFoundError(f"Raw dataset not found: {RAW_PATH}")

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    total_rows = 0
    written_rows = 0

    with RAW_PATH.open("r", encoding="utf-8", newline="") as input_file:
        reader = csv.DictReader(input_file)

        missing = [col for col in REQUIRED_COLUMNS if col not in reader.fieldnames]

        if missing:
            raise ValueError(
                "Missing required columns in raw CSV: "
                + ", ".join(missing)
                + f"\nAvailable columns: {reader.fieldnames}"
            )

        with OUTPUT_PATH.open("w", encoding="utf-8", newline="") as output_file:
            writer = csv.DictWriter(
                output_file,
                fieldnames=OUTPUT_COLUMNS,
                delimiter="|",
                lineterminator="\n"
            )

            writer.writeheader()

            for row in reader:
                total_rows += 1

                airline = clean_text(row.get("op_unique_carrier"))
                origin = clean_text(row.get("origin"))
                dest = clean_text(row.get("dest"))
                month = clean_number(row.get("month"))

                if not airline or not origin or not month:
                    continue

                route = f"{origin}-{dest}" if dest else origin

                writer.writerow({
                    "airline": airline,
                    "origin": origin,
                    "dest": dest,
                    "route": route,
                    "month": month,
                    "dep_delay": clean_number(row.get("dep_delay")),
                    "arr_delay": clean_number(row.get("arr_delay")),
                    "cancelled": cancelled_to_int(row.get("cancelled")),
                    "cancellation_code": clean_text(row.get("cancellation_code")),
                    "cause": infer_cause(row),
                })

                written_rows += 1

                if written_rows % 500000 == 0:
                    print(f"Written rows: {written_rows}")

    print("Hive text dataset created successfully.")
    print("Input rows:", total_rows)
    print("Written rows:", written_rows)
    print("Output:", OUTPUT_PATH)


if __name__ == "__main__":
    main()