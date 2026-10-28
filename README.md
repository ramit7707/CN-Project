# Fault-Tolerant Topic-Based File Distribution Service

**Course:** IT305 — Computer Networks Lab  
**Project:** Socket Programming  
**Groups:** 078 + 074

## 1. Project Overview

This project implements a TCP-based topic file distribution service in C. The server treats each immediate subdirectory under a dataset root as a topic. A client requests one topic and receives the regular files contained in that topic directory, storing them under `<output_dir>/<topic>/`.

The implementation is organized into four versions:

| Version | Purpose | Recovery behaviour |
|---|---|---|
| **Part I — Baseline** | Topic-based file transfer with single-threaded and multi-threaded server modes | No fault recovery beyond detecting transfer errors |
| **Part II — Case 1** | Adds probabilistic connection failure injection | Reconnects and retransfers the topic from the beginning |
| **Part II — Case 2** | Adds session tracking and checkpoint-based resumption | Reconnects and resumes from the last locally stored byte offset |
| **Part II — Case 2 Enhanced** | Adds multiple parallel TCP connections and a shared work queue | A failed worker retries its assigned file from its saved offset while other workers continue |

The four versions share the same overall goal but differ in scheduling and recovery strategy. The report discusses their design, protocol, limitations, and the available performance plots.

## 2. Team Members and Contributions

| Student | Roll number | Main contribution |
|---|---:|---|
| Shyam Ramani | 202401175 | Server architecture, TCP listening setup, topic discovery, and Part I single-threaded baseline |
| Sherashiya Ramit | 202401198 | Client transfer workflow, protocol framing, chunked reception, output validation, and Part I integration |
| Om Patel | 202401150 | Part II fault injection, failure handling, retry policy, and Case 1 restart-from-beginning behaviour |
| Dev Fultariya | 202401254 | Case 2 session management, checkpoint validation, byte-offset resume, and recovery logic |
| Yagnik Arvadiya | 202401252 | Case 2 Enhanced parallel TCP streams, shared work queue, worker synchronization, and recovery |
| Khush Vadhadiya | 202401267 | Benchmark scripts, performance counters, CSV generation, plotting, and report preparation |

## 3. Requirements and Environment

The implementation uses C and POSIX TCP sockets with POSIX threads where required. Build and run on a POSIX-compatible environment with a C compiler and GNU Make.

- **Compiler:** `gcc` or compatible C compiler
- **Build tool:** GNU Make
- **Operating system:** Linux is the recommended environment for the socket and `sendfile()` functionality used by the project
- **Benchmark tools:** Python 3 for the included benchmark and plotting scripts
- **Dataset:** A directory containing topic subdirectories and regular files

The exact build and runtime requirements are defined by the source code and Makefiles in the submitted project. No external Python plotting package should be assumed unless it is installed and required by `Results/plot_results.py`.

## 4. Repository Structure

The submission is organized as follows:

```text
FinalSubmission_Group_078_074/
├── README.md
├── FinalReport_Group_078_074.pdf
├── Part1/
│   ├── server.c
│   ├── client.c
│   ├── protocol.h
│   ├── utils.h / utils.c
│   └── Makefile
├── Part2_Case1/
│   ├── server.c
│   ├── client.c
│   ├── protocol.h
│   ├── utils.h / utils.c
│   └── Makefile
├── Part2_Case2/
│   ├── server.c
│   ├── client.c
│   ├── protocol.h
│   ├── utils.h / utils.c
│   └── Makefile
├── Part2_Case2_Enhanced/
│   ├── server.c
│   ├── client.c
│   ├── protocol.h
│   ├── utils.h / utils.c
│   └── Makefile
└── Results/
    ├── benchmark.py
    ├── plot_results.py
    └── sample_results.csv
```

The precise files present in a particular ZIP may vary; use the actual directory contents rather than assuming that every example graph or CSV listed in older documentation is included.

## 5. Build Instructions

Each implementation directory has its own Makefile. From the project root, build each version separately:

```bash
cd Part1
make clean
make
cd ..

cd Part2_Case1
make clean
make
cd ..

cd Part2_Case2
make clean
make
cd ..

cd Part2_Case2_Enhanced
make clean
make
cd ..
```

If a directory's Makefile does not define `clean`, omit that command and run `make`. Resolve compiler errors before starting the benchmark. The expected executable names are `server` and `client`, as defined by each Makefile.

## 6. Dataset Layout

The server's `<topics_root_dir>` must contain one subdirectory per topic. Regular files directly inside a topic directory are the files served by that topic.

```text
dataset_root/
├── cane/
│   ├── image1.jpg
│   └── image2.jpg
├── gatto/
│   ├── image3.jpg
│   └── image4.jpg
└── documents/
    └── example.pdf
```

Use topic names that exactly match the directory names, subject to the implementation's topic-matching rules. Pass the dataset root to the server, not the path of an individual topic. For example, if the directory is `dataset/raw-image/`, pass that root only if it contains the topic subdirectories expected by the program.

## 7. Command-Line Usage

The examples below use port numbers `8080`–`8083`; choose an unused port and use the same port in the corresponding client command. Replace `./dataset_root` with the actual dataset path. Run the server in one terminal and the client in another.

### 7.1 Part I — Baseline

**Server**

```bash
./Part1/server <port> <topics_root_dir> [single|multi]
```

- `single` selects sequential client servicing.
- `multi` selects concurrent client servicing, if supported by the implementation.
- If the mode is omitted, the implementation's default mode applies.

**Client**

```bash
./Part1/client <server_ip> <server_port> <topic> <output_dir>
```

**Example**

```bash
# Terminal 1
./Part1/server 8080 ./dataset_root multi

# Terminal 2
./Part1/client 127.0.0.1 8080 cane ./downloads
```

### 7.2 Part II — Case 1: Restart from the Beginning

**Server**

```bash
./Part2_Case1/server <port> <topics_root_dir> <failure_probability>
```

**Client**

```bash
./Part2_Case1/client <server_ip> <server_port> <topic> <output_dir>
```

`<failure_probability>` is expected to be in the interval `[0, 1]`. In this project, fault injection is described as a per-chunk probability for a connection interruption before a file-data chunk is sent. The precise chunk size and fault location should be checked against the implementation; the report describes the configured 64 KiB chunk model.

**Example**

```bash
# Terminal 1: 2% fault probability per eligible chunk
./Part2_Case1/server 8081 ./dataset_root 0.02

# Terminal 2
./Part2_Case1/client 127.0.0.1 8081 cane ./downloads_case1
```

After a transfer failure, Case 1 reconnects and requests the topic again. Since it does not resume from a checkpoint, previously transferred data may be transmitted again. Use a low failure probability for a basic smoke test.

### 7.3 Part II — Case 2: Checkpoint-Based Resume

**Server**

```bash
./Part2_Case2/server <port> <topics_root_dir> <failure_probability>
```

**Client**

```bash
./Part2_Case2/client <server_ip> <server_port> <topic> <output_dir>
```

**Example**

```bash
# Terminal 1
./Part2_Case2/server 8082 ./dataset_root 0.05

# Terminal 2
./Part2_Case2/client 127.0.0.1 8082 cane ./downloads_case2
```

Case 2 records session and checkpoint information so that a reconnect can request the remaining bytes from the last validated offset. The client-side partial file must represent bytes actually written to disk. The server validates the resume request before sending the remaining data. This reduces redundant file-data transmission compared with restarting the entire topic, but reconnections and control messages still incur time overhead.

### 7.4 Part II — Case 2 Enhanced: Parallel TCP Streams

**Server**

```bash
./Part2_Case2_Enhanced/server <port> <topics_root_dir> <failure_probability>
```

**Client**

```bash
./Part2_Case2_Enhanced/client <server_ip> <server_port> <topic> <output_dir> [connections]
```

**Example**

```bash
# Terminal 1
./Part2_Case2_Enhanced/server 8083 ./dataset_root 0.02

# Terminal 2: request four parallel connections
./Part2_Case2_Enhanced/client 127.0.0.1 8083 cane ./downloads_enhanced 4
```

The optional connection count is implementation-dependent; use a value accepted by the client. The enhanced design distributes file requests among worker connections. If a worker fails, recovery applies to the affected work item while other workers may continue. Parallel connections can improve utilization for workloads containing many files, but they do not guarantee higher throughput: the result depends on file sizes, CPU and disk performance, network capacity, and connection overhead.

## 8. Protocol and Recovery Summary

The protocol uses a framed TCP message exchange. TCP is a byte stream, so a receiver must not assume that one `send()` corresponds to one `recv()`. The implementation's send/receive helpers are responsible for transferring complete headers and payloads.

| Version | Transfer state | Behaviour after injected connection failure |
|---|---|---|
| Part I | No checkpoint-based recovery | Transfer fails and must be retried by the caller |
| Case 1 | No persistent transfer checkpoint | Reconnect and restart the topic from the beginning |
| Case 2 | Session and byte-offset checkpoint | Validate the checkpoint and resume from the stored offset |
| Case 2 Enhanced | Session/checkpoint logic plus worker queue | Retry the failed work item from its valid offset; other workers may continue |

For Case 2 and Enhanced, a resume offset must not exceed the file size or refer to a mismatched file index/name. A rejected or invalid checkpoint must not be treated as proof that the file is complete. Session state is held in server memory in the baseline implementation, so a server process restart may invalidate active sessions; this is a limitation, not durable server-side persistence.

## 9. Running the Benchmark and Plotting Scripts

The `Results/` directory contains benchmark and plotting scripts plus a sample results CSV. Inspect the scripts before running them to confirm the expected executable paths, command-line arguments, dataset path, and output filenames.

From the project root:

```bash
python3 Results/benchmark.py
python3 Results/plot_results.py
```

Run the benchmark only after compiling all required versions and configuring the dataset path expected by the script. If the script requires command-line arguments or environment variables, follow its usage message and source comments.

### Metrics used in the report

- **Useful bytes delivered:** bytes of file content successfully received and retained by the client.
- **Total bytes transmitted:** file-data bytes sent over the transfer attempts, including repeated data where the metric captures it.
- **Throughput:** useful bytes delivered divided by elapsed transfer time, converted to the unit specified by the CSV/plot.
- **Redundant bytes:** transmitted file-data bytes that do not contribute to the final useful output.
- **Retransmission overhead:** redundant bytes divided by useful bytes, multiplied by 100:

  \[
  \text{Overhead (\%)} =
  \frac{\text{Redundant Bytes}}{\text{Useful Bytes}}\times 100
  \]

Control messages, reconnect delay, and back-off can reduce throughput even when retransmission overhead is zero. Therefore, a zero retransmission-overhead value does not imply zero recovery cost.

### Interpreting the current plots

The Part I graph included in the final PDF is an illustrative comparison of single-threaded
and multi-threaded throughput at client counts 1, 2, and 3. It displays approximately
105, 112, and 109 MiB/s for single-threaded mode and 106, 145, and 171 MiB/s for
multi-threaded mode. These values are explicitly labelled illustrative in the graph; they
are not benchmark measurements and must not be cited as experimental evidence. The
graph is useful for explaining a possible trend: a concurrent server may use available
resources more effectively as simultaneous clients increase, while a sequential server
may show limited aggregate scaling. Actual performance can differ and must be established
by running the benchmark. Do not copy the illustrative values into the results CSV or
present them as measured results.

The supplied sample CSV is a separate data source and does not provide matching
single-threaded and multi-threaded measurements at client counts 1, 2, and 3. Therefore,
the illustrative graph in the PDF should not be described as having been generated from
that CSV. For a quantitative performance claim, run both modes at the same client counts
on the same topic and dataset, repeat each configuration, and regenerate a graph from the
validated measurements.

The Part II throughput plot compares the recorded throughput as failure probability
increases. Case 1 can retransmit the topic from the beginning after a failure, so repeated
file data and restart delay may reduce effective throughput. Case 2 and Enhanced aim to
avoid retransmitting bytes already stored, but reconnects and protocol exchanges still
cost time. Enhanced parallelism may improve throughput in some workloads, but the graph
alone should be interpreted according to the measured conditions.


## 10. Validation Checklist

Use this checklist when testing the project:

- [ ] Each implementation compiles successfully.
- [ ] The server starts with a valid topic root and an unused port.
- [ ] A valid topic can be requested and output files are created under `<output_dir>/<topic>/`.
- [ ] An unknown topic is rejected with an error rather than silently treated as success.
- [ ] Zero-byte files and empty topics complete without hanging, if these cases are supported by the implementation.
- [ ] Received files are compared with source files using checksums or byte-for-byte comparison.
- [ ] With a nonzero failure probability, logs show the expected failure and recovery path.
- [ ] Case 1 restarts the topic after a failure.
- [ ] Case 2 resumes from a validated stored offset.
- [ ] Enhanced mode continues independent worker tasks when one worker fails, where supported.
- [ ] Benchmark rows contain valid completion times, useful-byte counts, and transmission-byte counts.
- [ ] The final graphs are generated from the CSV identified in the report.

Do not mark a test as passed unless it has actually been run.

## 11. Known Limitations

- In Case 2, server-side session information is volatile in the baseline implementation. A server process restart can invalidate active sessions.
- Parallel worker scheduling is file-oriented; a topic consisting of one very large file may not benefit from file-level parallelism unless byte-range splitting is implemented.
- Parallel TCP connections add coordination and recovery overhead and may not improve performance for every dataset or network.
- Benchmark results depend on machine load, file-system cache state, dataset composition, network conditions, and the exact test configuration.
- The current sample results include values that require verification against a fresh run before they are treated as final experimental evidence.

## 12. Report

The accompanying `FinalReport_Group_078_074.pdf` is the formal report for this project. It documents the system design, the four implementation variants, recovery behaviour, graph interpretation, and limitations of the available results. Keep this README and the report consistent: when benchmark values or implementation details change, update both the CSV/plots and the corresponding explanations.

---

**Submission note:** Replace example dataset paths, ports, and topic names with values appropriate to your environment. Report only features and test outcomes supported by the submitted code and reproducible results.
