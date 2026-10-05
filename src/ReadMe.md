# OllamaSemi - High-Performance Rust STDF Parser Engine

A commercial-grade, ultra-fast STDF (Standard Test Data Format v4) binary parser written in Rust. 
Designed as the core high-throughput extraction engine for **OllamaSemi**—an offline, air-gapped semiconductor yield analysis tool powered by local LLMs.
plus other featurs to be followed

## 🚀 Key Features

- **Ultra-High Speed**: Parses binary STDF datalogs at ~8.75 million records/second.
- **Zero-Copy Memory Mapping**: Stream-reads raw binary records with minimal RAM overhead.
- **Parametric Test Analysis**: Instantly extracts PTR records, identifies failing tests (`test_flg`), and summarizes test counts.
- **Lot Metadata Extraction**: Captures Master Information Records (MIR) including Device ID and Operator names.

## ⚡ Real-World Benchmark Performance

Tested on a sample STDF datalog (`test_data2.std`):

| Metric | Result |
| :--- | :--- |
| **Execution Time** | `46.86 ms` |
| **Total Records Parsed** | `410,273` |
| **PTR (Parametric) Records** | `392,356` |
| **Passed / Failed Tests** | `392,355` / `1` |
| **Processing Throughput** | **~8,753,984 records/sec** |

## 🛠️ Prerequisites & Installation

### 1. Install Rust
open CMD
curl --proto '=https' --tlsv1.2 -sSf [https://sh.rustup.rs](https://sh.rustup.rs) | sh
Press 1 and continue

### 2. Build in Release Mode
open CMD
cargo build --release

### 3. Usage
Run the compiled executable passing your target .stdf or .std file path:
Open CMD
cargo run --release -- "path/to/your/file.stdf"

🏗️ Architecture Roadmap
[x] High-speed Rust binary STDF reader

[ ] In-process DuckDB analytical columnar ingestion

[ ] Real-time Adaptive Statistical Downsampling engine

[ ] Ollama API integration (Qwen2.5-Coder context feeder)

[ ] Desktop GUI (Tauri / Web interface)