import os
import csv
import matplotlib.pyplot as plt
import numpy as np

TIMES_CSV = os.getenv("TIMES_CSV", "../data/results/metrics/execution_times.csv")
FIGURES_DIR = os.getenv("FIGURES_DIR", "../report/figures")

def load_times():
    rows = []

    with open(TIMES_CSV, newline="", encoding="utf-8") as file:
        reader = csv.DictReader(file)

        for row in reader:
            rows.append({
                "Environment": row.get("Environment", "Local"),
                "Technology": row["Technology"],
                "Analysis": row["Analysis"],
                "Input_Size": row["Input_Size"],
                "Cluster_Size": row.get("Cluster_Size", "local"),
                "Time_s": float(row["Time_s"]),
            })

    return rows

def chart_by_technology(rows):
    full = [row for row in rows if row["Input_Size"] == "100%"]
    analyses = sorted(set(row["Analysis"] for row in full))
    techs = sorted(set(row["Technology"] for row in full))
    environments = sorted(set(row["Environment"] for row in full))

    x = np.arange(len(analyses))
    width = 0.8 / max(1, len(techs) * len(environments))
    fig, ax = plt.subplots(figsize=(12, 6))

    offset_index = 0
    for environment in environments:
        for tech in techs:
            times = []

            for analysis in analyses:
                match = [
                    row["Time_s"]
                    for row in full
                    if row["Environment"] == environment
                    and row["Technology"] == tech
                    and row["Analysis"] == analysis
                ]
                times.append(match[0] if match else 0)

            offset = (offset_index - ((len(techs) * len(environments) - 1) / 2)) * width
            bars = ax.bar(x + offset, times, width, label=f"{environment} - {tech}")

            for bar, value in zip(bars, times):
                if value > 0:
                    ax.text(
                        bar.get_x() + bar.get_width() / 2,
                        bar.get_height(),
                        f"{value}s",
                        ha="center",
                        va="bottom",
                        fontsize=8,
                        rotation=90,
                    )

            offset_index += 1

    ax.set_xlabel("Analysis")
    ax.set_ylabel("Execution Time (seconds)")
    ax.set_title("Execution Time by Technology and Environment (100% dataset)")
    ax.set_xticks(x)
    ax.set_xticklabels([f"Analysis {analysis}" for analysis in analyses])
    ax.legend(fontsize=8)
    ax.grid(axis="y", alpha=0.3)

    plt.tight_layout()
    path = os.path.join(FIGURES_DIR, "execution_time_by_technology_environment.png")
    plt.savefig(path, dpi=150)
    plt.close()
    print(f"Saved: {path}")

def chart_by_input_size(rows):
    sizes = ["25%", "50%", "100%"]
    techs = sorted(set(row["Technology"] for row in rows))
    environments = sorted(set(row["Environment"] for row in rows))

    for analysis in sorted(set(row["Analysis"] for row in rows)):
        fig, ax = plt.subplots(figsize=(10, 6))

        for environment in environments:
            for tech in techs:
                times = []

                for size in sizes:
                    match = [
                        row["Time_s"]
                        for row in rows
                        if row["Environment"] == environment
                        and row["Technology"] == tech
                        and row["Analysis"] == analysis
                        and row["Input_Size"] == size
                    ]
                    times.append(match[0] if match else None)

                valid_sizes = [size for size, time_value in zip(sizes, times) if time_value is not None]
                valid_times = [time_value for time_value in times if time_value is not None]

                if valid_times:
                    ax.plot(valid_sizes, valid_times, marker="o", label=f"{environment} - {tech}", linewidth=2)

                    for x_value, y_value in zip(valid_sizes, valid_times):
                        ax.annotate(
                            f"{y_value}s",
                            (x_value, y_value),
                            textcoords="offset points",
                            xytext=(0, 8),
                            ha="center",
                            fontsize=8,
                        )

        ax.set_xlabel("Input Size (% of dataset)")
        ax.set_ylabel("Execution Time (seconds)")
        ax.set_title(f"Scalability — Analysis {analysis}")
        ax.legend(fontsize=8)
        ax.grid(alpha=0.3)

        plt.tight_layout()
        path = os.path.join(FIGURES_DIR, f"scalability_analysis_{analysis}.png")
        plt.savefig(path, dpi=150)
        plt.close()
        print(f"Saved: {path}")

def main():
    os.makedirs(FIGURES_DIR, exist_ok=True)
    rows = load_times()
    chart_by_technology(rows)
    chart_by_input_size(rows)
    print("All charts generated.")

if __name__ == "__main__":
    main()
