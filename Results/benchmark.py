            }
            results.append(row)
            print(f"  [{case_name}] p={p_fail:.2f} | Time={stats['elapsed_sec']:.2f}s | Socket={socket_mb:.2f}MB (Useful={useful_mb:.2f}MB) | Overhead={overhead_pct:.1f}% | Retries={failures}")

    with open(csv_file, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(results)
    print(f"[BENCH] Saved: {csv_file}")

def run_case2_enhanced_experiment():
    """Experiment 3: Case 2 vs Case 2 Enhanced Parallel Connections"""
    print("\n=======================================================")
    print(" Running Experiment 3: Case 2 Enhanced Parallel Streams")
    print("=======================================================")
    csv_file = os.path.join(RESULTS_DIR, "exp3_enhanced_parallel.csv")
    fieldnames = ["Connections", "Failure_Probability", "Elapsed_Sec", "Throughput_MBs", "Retransmission_Overhead_MB"]
    results = []

    server_bin = os.path.join(ENHANCED_DIR, "server")
    client_bin = os.path.join(ENHANCED_DIR, "client")

    for p_fail in [0.0, 0.02]:
        for conns in [1, 2, 4]:
            port = PORT_BASE + random.randint(10, 800)
            srv_proc = start_server(server_bin, port, DATASET_DIR, str(p_fail))
            clean_output()
            out_c = os.path.join(OUTPUT_DIR, f"enhanced_{p_fail}_{conns}")

            start_t = time.monotonic()
            cmd = [client_bin, "127.0.0.1", str(port), "topic_concurrency", out_c, str(conns)]
            cp = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            elapsed = time.monotonic() - start_t
            stop_server(srv_proc)

            stats = parse_client_stats(cp.stdout)
            tp = stats["throughput_mbs"] if stats["throughput_mbs"] > 0 else (7.5 / elapsed if elapsed > 0 else 0)
            retrans_mb = stats["redundant_bytes"] / (1024.0 * 1024.0)

            row = {
                "Connections": conns,
                "Failure_Probability": p_fail,
                "Elapsed_Sec": round(stats["elapsed_sec"] if stats["elapsed_sec"] > 0 else elapsed, 4),
                "Throughput_MBs": round(tp, 2),
                "Retransmission_Overhead_MB": round(retrans_mb, 2)
            }
            results.append(row)
            print(f"  [Enhanced] Conns={conns} | p={p_fail:.2f} | Time={row['Elapsed_Sec']:.2f}s | Throughput={tp:.2f} MB/s | Overhead={retrans_mb:.2f} MB")

    with open(csv_file, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(results)
    print(f"[BENCH] Saved: {csv_file}")


def write_graph_reference_data(output_path=None):
    """Write the illustrative values represented by the supplied reference graphs.

    These are graph-reference/sample values, NOT measurements from a live benchmark.
    Use the normal benchmark run to collect actual measurements.
    """
    output_path = output_path or os.path.join(os.path.dirname(os.path.abspath(__file__)), "sample_results.csv")
    fields = ["Experiment", "Case", "Metric", "Failure_Probability", "Concurrent_Clients",
              "Connections", "Useful_Bytes", "Total_Bytes", "Redundant_Bytes",
              "Elapsed_Sec", "Throughput_MBs", "Overhead_Pct"]
    rows = []
    for mode, values in [("Single-threaded", [105, 112, 109]),
                         ("Multi-threaded", [106, 145, 171])]:
        for clients, throughput in zip([1, 2, 3], values):
            rows.append({"Experiment":"Part I", "Case":mode, "Metric":"throughput",
                         "Concurrent_Clients":clients, "Throughput_MBs":throughput})
    probs = [0.00, 0.02, 0.05, 0.08, 0.10]
    throughput_series = {
        "Case 1 — restart from beginning": [82.5, 2.0, 0.2, 0.0, 0.0],
        "Case 2 — checkpoint resume": [86.0, 13.0, 6.0, 3.5, 2.5],
        "Case 2 Enhanced — parallel streams": [19.8, 14.5, 9.6, 7.25, 5.16],
    }
    for case, values in throughput_series.items():
        for p, throughput in zip(probs, values):
            rows.append({"Experiment":"Part II throughput", "Case":case,
                         "Metric":"useful_throughput", "Failure_Probability":f"{p:.2f}",
                         "Throughput_MBs":throughput})
    overhead_series = {
        "Case 1 — restart from beginning": [0, 100, 400, 1000, 1600],
        "Case 2 — checkpoint resume": [0, 0, 0, 0, 0],
        "Case 2 Enhanced — parallel streams": [0, 0, 0, 0, 0],
    }
    for case, values in overhead_series.items():
        for p, overhead in zip(probs, values):
            rows.append({"Experiment":"Part II overhead", "Case":case,
                         "Metric":"retransmission_overhead", "Failure_Probability":f"{p:.2f}",
                         "Overhead_Pct":overhead})
    with open(output_path, "w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)
    print(f"[BENCH] Wrote illustrative graph-reference data (not live measurements): {output_path}")

if __name__ == "__main__":
    if "--sample" in sys.argv:
        write_graph_reference_data(os.path.join(os.path.dirname(os.path.abspath(__file__)), "sample_results.csv"))
        sys.exit(0)
    os.makedirs(RESULTS_DIR, exist_ok=True)
    create_benchmark_datasets()
    run_part1_experiment()
    run_case1_vs_case2_experiment()
    run_case2_enhanced_experiment()
    clean_output()
    print("\n[BENCH] All benchmark experiments executed successfully and saved to Results/!\n")
