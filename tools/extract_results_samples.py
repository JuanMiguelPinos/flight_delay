from pathlib import Path
import pandas as pd

samples_dir = Path("results/samples")
samples_dir.mkdir(parents=True, exist_ok=True)

def write_text_head(source_dir: Path, output_file: Path, n: int = 10) -> None:
    if not source_dir.exists():
        print(f"Skipping missing folder: {source_dir}")
        return

    files = [
        p for p in source_dir.iterdir()
        if p.is_file() and not p.name.startswith("_")
    ]

    if not files:
        print(f"No data files found in: {source_dir}")
        return

    with files[0].open("r", encoding="utf-8", errors="ignore") as src:
        lines = []
        for _ in range(n):
            line = src.readline()
            if not line:
                break
            lines.append(line)

    with output_file.open("w", encoding="utf-8") as dst:
        dst.writelines(lines)

    print(f"Created {output_file}")

def write_parquet_head(source_dir: Path, output_file: Path, n: int = 10) -> None:
    if not source_dir.exists():
        print(f"Skipping missing folder: {source_dir}")
        return

    parquet_files = list(source_dir.glob("*.parquet"))

    if not parquet_files:
        print(f"No Parquet files found in: {source_dir}")
        return

    df = pd.read_parquet(parquet_files[0])
    df.head(n).to_csv(output_file, index=False)

    print(f"Created {output_file}")

write_text_head(
    Path("results/cluster_outputs/hive_3_1"),
    samples_dir / "hive_3_1_head.txt",
)

write_text_head(
    Path("results/cluster_outputs/hive_3_2"),
    samples_dir / "hive_3_2_head.txt",
)

write_parquet_head(
    Path("results/cluster_outputs/spark_sql_3_1_50pct"),
    samples_dir / "spark_sql_3_1_50pct_head.csv",
)

write_parquet_head(
    Path("results/cluster_outputs/spark_sql_3_1_100pct"),
    samples_dir / "spark_sql_3_1_100pct_head.csv",
)