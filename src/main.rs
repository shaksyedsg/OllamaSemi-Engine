use duckdb::{params, Connection};
use rust_stdf::stdf_file::StdfReader;
use rust_stdf::StdfRecord;
use std::env;
use std::time::Instant;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 1. READ COMMAND LINE ARGUMENTS
    let args: Vec<String> = env::args().collect();
    if args.len() < 2 {
        println!("❌ Error: Please provide an STDF file path.");
        println!("Usage: cargo run --release -- <path_to_stdf_file>");
        return Ok(());
    }
    let file_path = &args[1];

    println!("🚀 Opening STDF File: {}", file_path);
    let start_time = Instant::now();

    // 2. INITIALIZE DUCKDB WITH DISK PERSISTENCE
    let conn = Connection::open("stdf_analytics.db")?;

    // Create database tables if they don't exist yet
    conn.execute_batch(
        "
        CREATE TABLE IF NOT EXISTS lots (
            lot_id TEXT,
            device TEXT,
            operator TEXT
        );

        CREATE TABLE IF NOT EXISTS parametric_tests (
            test_num INTEGER,
            test_name TEXT,
            result DOUBLE,
            test_flag INTEGER,
            is_fail BOOLEAN
        );
        ",
    )?;

    // 3. OPEN STDF PARSER
    let mut reader = match StdfReader::new(file_path) {
        Ok(r) => r,
        Err(err) => {
            println!("❌ Failed to open STDF file: {}", err);
            return Ok(());
        }
    };

    // Prepare DuckDB Appender for high-speed batching
    let mut ptr_appender = conn.appender("parametric_tests")?;

    let mut total_records = 0u64;
    let mut ptr_count = 0u64;

    // 4. STREAM PARSE & APPEND TO DUCKDB
    for record in reader.get_record_iter() {
        total_records += 1;

        match record {
            // Master Information Record (MIR)
            Ok(StdfRecord::MIR(mir)) => {
                conn.execute(
                    "INSERT INTO lots (lot_id, device, operator) VALUES (?, ?, ?)",
                    params![mir.lot_id, mir.part_typ, mir.oper_nam],
                )?;
            }

            // Parametric Test Record (PTR)
            Ok(StdfRecord::PTR(ptr)) => {
                ptr_count += 1;

                let flag_byte = ptr.test_flg[0];
                let is_fail = (flag_byte & 0b1100_0000) != 0;

                // Stream record directly to columnar buffer
                ptr_appender.append_row(params![
                    ptr.test_num,
                    ptr.test_txt,
                    ptr.result,
                    flag_byte,
                    is_fail
                ])?;
            }

            _ => {}
        }
    }

    // Flush all batched memory blocks directly to disk
    ptr_appender.flush()?;

    let parse_duration = start_time.elapsed();
    println!("✅ Ingestion into 'stdf_analytics.db' completed in {:.3?}", parse_duration);

    // 5. RUN SQL ANALYTICS ON DISK DATABASE
    println!("\n📊 --- DUCKDB DISK ANALYTICAL SUMMARY ---");

    // Query Lot Info
    let mut lot_stmt = conn.prepare("SELECT lot_id, device, operator FROM lots LIMIT 1")?;
    let mut lot_rows = lot_stmt.query([])?;
    if let Some(row) = lot_rows.next()? {
        let lot_id: String = row.get(0)?;
        let device: String = row.get(1)?;
        let operator: String = row.get(2)?;
        println!("📌 Device: {} | Operator: {} | Lot ID: '{}'", device, operator, lot_id);
    }

    // Query Test Summary
    let mut summary_stmt = conn.prepare(
        "SELECT 
            COUNT(*) as total,
            COUNT(CASE WHEN is_fail = false THEN 1 END) as passed,
            COUNT(CASE WHEN is_fail = true THEN 1 END) as failed
         FROM parametric_tests",
    )?;

    let mut summary_rows = summary_stmt.query([])?;
    if let Some(row) = summary_rows.next()? {
        let total: i64 = row.get(0)?;
        let passed: i64 = row.get(1)?;
        let failed: i64 = row.get(2)?;

        println!("📦 Total PTR Rows Saved: {}", total);
        println!("✅ Passed PTR Measurements: {}", passed);
        println!("❌ Failed PTR Measurements: {}", failed);
        println!("📦 Total Records Tracked by Rust: {}", total_records);
        println!("📦 Total PTR Counts by Rust: {}", ptr_count);
    }

    Ok(())
}