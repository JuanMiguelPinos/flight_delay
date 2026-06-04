from pathlib import Path
import csv

metrics_dir = Path("results/metrics")
metrics_dir.mkdir(parents=True, exist_ok=True)

input_files = [
    ("execution_times_local.csv", "Local", "local"),
    ("cluster_execution_times.csv", "EMR", "1-primary-1-core"),
    ("hive_cluster_execution_times_clean.csv", "EMR", "1-primary-1-core"),
]

output_file = metrics_dir / "execution_times_all.csv"

fields = ["Environment", "Technology", "Analysis", "Input_Size", "Cluster_Size", "Time_s"]
rows = []

for filename, default_env, default_cluster in input_files:
    path = metrics_dir / filename

    if not path.exists():
        print(f"Skipping missing file: {path}")
        continue

    with path.open("r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)

        for row in reader:
            clean_row = {
                "Environment": row.get("Environment") or default_env,
                "Technology": row.get("Technology", ""),
                "Analysis": row.get("Analysis", ""),
                "Input_Size": row.get("Input_Size", ""),
                "Cluster_Size": row.get("Cluster_Size") or default_cluster,
                "Time_s": row.get("Time_s", ""),
            }

            if clean_row["Technology"] and clean_row["Time_s"]:
                rows.append(clean_row)

with output_file.open("w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=fields)
    writer.writeheader()
    writer.writerows(rows)

print(f"Created {output_file} with {len(rows)} rows")