import os
import csv
import matplotlib.pyplot as plt
import numpy as np

TIMES_CSV   = "../data/results/metrics/execution_times.csv"
FIGURES_DIR = "../report/figures"

def load_times():
    rows = []
    with open(TIMES_CSV, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append({
                "Technology": row["Technology"],
                "Analysis":   row["Analysis"],
                "Input_Size": row["Input_Size"],
                "Time_s":     float(row["Time_s"]),
            })
    return rows

def chart_by_technology(rows):
    """Tiempo por tecnología para cada análisis (100% del dataset)."""
    full = [r for r in rows if r["Input_Size"] == "100%"]
    analyses = sorted(set(r["Analysis"] for r in full))
    techs    = sorted(set(r["Technology"] for r in full))
    colors   = {"Spark SQL": "#2196F3", "Spark Core": "#FF9800", "Hive": "#4CAF50"}

    x    = np.arange(len(analyses))
    w    = 0.25
    fig, ax = plt.subplots(figsize=(10, 6))

    for i, tech in enumerate(techs):
        times = []
        for an in analyses:
            match = [r["Time_s"] for r in full
                     if r["Technology"] == tech and r["Analysis"] == an]
            times.append(match[0] if match else 0)
        bars = ax.bar(x + i*w, times, w, label=tech,
                      color=colors.get(tech, "#9C27B0"))
        for bar, t in zip(bars, times):
            if t > 0:
                ax.text(bar.get_x() + bar.get_width()/2,
                        bar.get_height() + 0.3,
                        f"{t}s", ha="center", va="bottom", fontsize=9)

    ax.set_xlabel("Analysis")
    ax.set_ylabel("Execution Time (seconds)")
    ax.set_title("Execution Time by Technology (100% dataset, local mode)")
    ax.set_xticks(x + w)
    ax.set_xticklabels([f"Analysis {a}" for a in analyses])
    ax.legend()
    ax.grid(axis="y", alpha=0.3)
    plt.tight_layout()
    path = os.path.join(FIGURES_DIR, "execution_time_by_technology.png")
    plt.savefig(path, dpi=150)
    plt.close()
    print(f"Guardado: {path}")

def chart_by_input_size(rows):
    """Tiempo vs tamaño de entrada por tecnología."""
    colors = {"Spark SQL": "#2196F3", "Spark Core": "#FF9800", "Hive": "#4CAF50"}
    sizes  = ["25%", "50%", "100%"]
    techs  = sorted(set(r["Technology"] for r in rows))

    for analysis in sorted(set(r["Analysis"] for r in rows)):
        fig, ax = plt.subplots(figsize=(9, 5))
        for tech in techs:
            times = []
            for s in sizes:
                match = [r["Time_s"] for r in rows
                         if r["Technology"] == tech
                         and r["Analysis"] == analysis
                         and r["Input_Size"] == s]
                times.append(match[0] if match else None)
            valid_s = [s for s, t in zip(sizes, times) if t is not None]
            valid_t = [t for t in times if t is not None]
            if valid_t:
                ax.plot(valid_s, valid_t, marker="o",
                        label=tech, color=colors.get(tech, "#9C27B0"),
                        linewidth=2)
                for xs, yt in zip(valid_s, valid_t):
                    ax.annotate(f"{yt}s", (xs, yt),
                                textcoords="offset points",
                                xytext=(0, 8), ha="center", fontsize=8)

        ax.set_xlabel("Input Size (% of dataset)")
        ax.set_ylabel("Execution Time (seconds)")
        ax.set_title(f"Scalability — Analysis {analysis} (local mode)")
        ax.legend()
        ax.grid(alpha=0.3)
        plt.tight_layout()
        path = os.path.join(FIGURES_DIR, f"scalability_analysis_{analysis}.png")
        plt.savefig(path, dpi=150)
        plt.close()
        print(f"Guardado: {path}")

def main():
    os.makedirs(FIGURES_DIR, exist_ok=True)
    rows = load_times()
    chart_by_technology(rows)
    chart_by_input_size(rows)
    print("Todos los gráficos generados.")

if __name__ == "__main__":
    main()