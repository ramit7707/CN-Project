#!/usr/bin/env python3
"""Generate the three report graphs from sample_results.csv.

The supplied reference values are illustrative. Replace them with measured benchmark
results before presenting them as experimental findings.
"""
import csv
import os
import matplotlib.pyplot as plt

RESULTS_DIR = os.path.dirname(os.path.abspath(__file__))
CSV_PATH = os.path.join(RESULTS_DIR, "sample_results.csv")
PROBS = [0.00, 0.02, 0.05, 0.08, 0.10]

def load_rows():
    if not os.path.isfile(CSV_PATH):
        raise FileNotFoundError(f"Missing {CSV_PATH}. Run `python3 benchmark.py --sample` first.")
    with open(CSV_PATH, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))

def rows_for(rows, experiment, case=None):
    return [r for r in rows if r["Experiment"] == experiment and (case is None or r["Case"] == case)]

def save_concurrency(rows):
    fig, ax = plt.subplots(figsize=(12, 6.5))
    for case, color, marker in [
        ("Single-threaded", "#1f77b4", "o"),
        ("Multi-threaded", "#ff7f0e", "s"),
    ]:
        subset = sorted(rows_for(rows, "Part I", case), key=lambda r: int(r["Concurrent_Clients"]))
        x = [int(r["Concurrent_Clients"]) for r in subset]
        y = [float(r["Throughput_MBs"]) for r in subset]
        ax.plot(x, y, marker=marker, linewidth=2.5, markersize=9,
                color=color, label=f"{case} (illustrative)")
    ax.set_title("Part I Throughput vs. Concurrent Client Count\nIllustrative values — replace with benchmark measurements", fontsize=15)
    ax.set_xlabel("Concurrent client count", fontsize=12)
    ax.set_ylabel("Aggregate throughput (MiB/s)", fontsize=12)
    ax.set_xticks([1, 2, 3])
    ax.set_ylim(0, 200)
    ax.grid(True, alpha=0.3)
    ax.legend(loc="upper right", fontsize=11)
    fig.tight_layout()
    fig.savefig(os.path.join(RESULTS_DIR, "graph_concurrency.png"), dpi=160)
    plt.close(fig)

def save_throughput(rows):
    fig, ax = plt.subplots(figsize=(12, 7))
    styles = [
        ("Case 1 — restart from beginning", "#1f77b4", "o"),
        ("Case 2 — checkpoint resume", "#ff7f0e", "s"),
        ("Case 2 Enhanced — parallel streams", "#2ca02c", "^"),
    ]
    for case, color, marker in styles:
        subset = sorted(rows_for(rows, "Part II throughput", case), key=lambda r: float(r["Failure_Probability"]))
        x = [float(r["Failure_Probability"]) for r in subset]
        y = [float(r["Throughput_MBs"]) for r in subset]
        ax.plot(x, y, marker=marker, linewidth=2.5, markersize=8, color=color, label=case)
    ax.set_title("Part II: Useful Throughput vs. Failure Probability", fontsize=15)
    ax.set_xlabel("Failure probability per data chunk (p)", fontsize=12)
    ax.set_ylabel("Throughput (MB/s, as recorded in CSV)", fontsize=12)
    ax.set_xticks(PROBS, [f"{p:.2f}" for p in PROBS])
    ax.grid(True, linestyle="--", alpha=0.3)
    ax.legend(loc="upper right", fontsize=10)
    fig.text(0.5, 0.015, "Source: sample_results.csv — illustrative sample data, not verified live benchmark measurements.",
             ha="center", fontsize=9)
    fig.tight_layout(rect=[0, 0.04, 1, 1])
    fig.savefig(os.path.join(RESULTS_DIR, "graph_useful_throughput.png"), dpi=160)
    plt.close(fig)

def save_overhead(rows):
    fig, ax = plt.subplots(figsize=(12, 7))
    styles = [
        ("Case 1 — restart from beginning", "#1f77b4", "o"),
        ("Case 2 — checkpoint resume", "#ff7f0e", "s"),
        ("Case 2 Enhanced — parallel streams", "#2ca02c", "^"),
    ]
    plot_data = {}
    for case, color, marker in styles:
        subset = sorted(rows_for(rows, "Part II overhead", case), key=lambda r: float(r["Failure_Probability"]))
        x = [float(r["Failure_Probability"]) for r in subset]
        y = [float(r["Overhead_Pct"]) for r in subset]
        plot_data[case] = (x, y, color, marker)
        ax.plot(x, y, marker=marker, linewidth=2.5, markersize=8, color=color, label=case)
    ax.set_title("Part II: Retransmission Overhead vs. Failure Probability", fontsize=15)
    ax.set_xlabel("Failure probability per data chunk (p)", fontsize=12)
    ax.set_ylabel("Redundant bytes / useful bytes (%)", fontsize=12)
    ax.set_xticks(PROBS, [f"{p:.2f}" for p in PROBS])
    ax.grid(True, linestyle="--", alpha=0.3)
    ax.legend(loc="upper left", fontsize=10)

    # Inset keeps the low-overhead cases visible despite the large restart overhead.
    inset = ax.inset_axes([0.55, 0.60, 0.42, 0.34])
    for case, (x, y, color, marker) in plot_data.items():
        inset.plot(x, [min(v, 100) for v in y], marker=marker, linewidth=2, markersize=5, color=color)
    inset.set_ylim(-10, 110)
    inset.set_xticks(PROBS, [f"{p:.2f}" for p in PROBS], fontsize=7)
    inset.set_title("Zoom: 0–100% overhead (values clipped)", fontsize=9)
    inset.grid(True, linestyle="--", alpha=0.3)
    fig.text(0.5, 0.015,
             "Case 2 and Enhanced are 0% in the supplied sample data; not independently verified.",
             ha="center", fontsize=9)
    fig.tight_layout(rect=[0, 0.04, 1, 1])
    fig.savefig(os.path.join(RESULTS_DIR, "graph_retransmission_overhead.png"), dpi=160)
    plt.close(fig)

def main():
    rows = load_rows()
    save_concurrency(rows)
    save_throughput(rows)
    save_overhead(rows)
    print("Generated graph_concurrency.png, graph_useful_throughput.png, and graph_retransmission_overhead.png")

if __name__ == "__main__":
    main()
