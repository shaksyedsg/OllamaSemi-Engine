use duckdb::{params, Connection};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::env;
use std::time::Instant;

#[derive(Serialize)]
struct OllamaRequest {
    model: String,
    prompt: String,
    stream: bool,
    options: serde_json::Value,
}

#[derive(Deserialize)]
struct OllamaResponse {
    response: String,
}

#[derive(Serialize)]
struct DeviceDetail {
    part_id: String,
    head_num: u8,
    site_num: u8,
    x_coord: i16,
    y_coord: i16,
    hard_bin: u16,
    soft_bin: u16,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = env::args().collect();
    if args.len() < 2 {
        println!("❌ Error: Please provide an STDF file path.");
        println!("Usage: cargo run --release -- <path_to_stdf_file>");
        return Ok(());
    }
    let file_path = &args[1];

    println!("🚀 Opening STDF File: {}", file_path);
    let start_time = Instant::now();

    // 1. IN-MEMORY DUCKDB FOR CLEAN RUNS
    let conn = Connection::open_in_memory()?;

    conn.execute_batch(
        "
        CREATE TABLE lots (
            lot_id TEXT,
            device TEXT,
            operator TEXT
        );

        CREATE TABLE device_starts (
            head_num INTEGER,
            site_num INTEGER
        );

        CREATE TABLE devices (
            head_num INTEGER,
            site_num INTEGER,
            part_id TEXT,
            x_coord INTEGER,
            y_coord INTEGER,
            hard_bin INTEGER,
            soft_bin INTEGER,
            part_flg INTEGER
        );

        CREATE TABLE parametric_tests (
            test_num INTEGER,
            test_name TEXT,
            result DOUBLE,
            test_flag INTEGER,
            is_fail BOOLEAN
        );
        ",
    )?;

    // 2. PARSE STDF INTO DUCKDB
    let mut reader = match rust_stdf::stdf_file::StdfReader::new(file_path) {
        Ok(r) => r,
        Err(err) => {
            println!("❌ Failed to open STDF file: {}", err);
            return Ok(());
        }
    };

    let mut pir_appender = conn.appender("device_starts")?;
    let mut prr_appender = conn.appender("devices")?;
    let mut ptr_appender = conn.appender("parametric_tests")?;

    for record in reader.get_record_iter() {
        match record {
            Ok(rust_stdf::StdfRecord::MIR(mir)) => {
                conn.execute(
                    "INSERT INTO lots (lot_id, device, operator) VALUES (?, ?, ?)",
                    params![mir.lot_id, mir.part_typ, mir.oper_nam],
                )?;
            }
            Ok(rust_stdf::StdfRecord::PIR(pir)) => {
                pir_appender.append_row(params![pir.head_num, pir.site_num])?;
            }
            Ok(rust_stdf::StdfRecord::PRR(prr)) => {
                let part_flg_byte = prr.part_flg[0];
                prr_appender.append_row(params![
                    prr.head_num,
                    prr.site_num,
                    prr.part_id.to_string(),
                    prr.x_coord,
                    prr.y_coord,
                    prr.hard_bin,
                    prr.soft_bin,
                    part_flg_byte
                ])?;
            }
            Ok(rust_stdf::StdfRecord::PTR(ptr)) => {
                let flag_byte = ptr.test_flg[0];
                let is_fail = (flag_byte & 0b1100_0000) != 0;

                ptr_appender.append_row(params![
                    ptr.test_num,
                    ptr.test_txt.to_string(),
                    ptr.result,
                    flag_byte,
                    is_fail
                ])?;
            }
            _ => {}
        }
    }
    pir_appender.flush()?;
    prr_appender.flush()?;
    ptr_appender.flush()?;

    let parse_duration = start_time.elapsed();
    println!("✅ Ingestion completed in {:.3?}", parse_duration);

    // 3. PRINT ALL RAW DEVICE IDENTIFIERS (PRR.PART_ID) DIRECTLY TO CONSOLE
    println!("\n🔍 --- PRINTING DEVICE IDENTIFIERS (PRR) ---");

    let mut all_devices: Vec<DeviceDetail> = Vec::new();
    let mut dev_stmt = conn.prepare(
        "SELECT part_id, head_num, site_num, x_coord, y_coord, hard_bin, soft_bin 
         FROM devices",
    )?;
    let dev_rows = dev_stmt.query_map([], |row| {
        Ok(DeviceDetail {
            part_id: row.get(0)?,
            head_num: row.get(1)?,
            site_num: row.get(2)?,
            x_coord: row.get(3)?,
            y_coord: row.get(4)?,
            hard_bin: row.get(5)?,
            soft_bin: row.get(6)?,
        })
    })?;

    for (index, dev) in dev_rows.enumerate() {
        let d = dev?;
        println!(
            "  [{:03}] Part ID: {:<15} | Head: {} | Site: {} | Wafer X/Y: ({:3}, {:3}) | HardBin: {} | SoftBin: {}",
            index + 1,
            if d.part_id.trim().is_empty() { "<EMPTY/NULL>" } else { &d.part_id },
            d.head_num,
            d.site_num,
            d.x_coord,
            d.y_coord,
            d.hard_bin,
            d.soft_bin
        );
        all_devices.push(d);
    }

    // 4. METRICS CALCULATION
    let mut device_name = String::from("Unknown");
    let mut lot_id = String::from("Unknown");

    let mut lot_stmt = conn.prepare("SELECT lot_id, device FROM lots LIMIT 1")?;
    let mut lot_rows = lot_stmt.query([])?;
    if let Some(row) = lot_rows.next()? {
        lot_id = row.get(0)?;
        device_name = row.get(1)?;
    }

    let total_pir_starts: i64 = conn.query_row("SELECT COUNT(*) FROM device_starts", [], |row| row.get(0))?;
    let total_prr_devices = all_devices.len();
    
    // Check if PART_IDs are actually populated or if they use Wafer X/Y coordinates instead
    let distinct_part_ids: i64 = conn.query_row(
        "SELECT COUNT(DISTINCT part_id) FROM devices WHERE part_id != ''",
        [],
        |row| row.get(0),
    )?;

    let passed_devices = all_devices.iter().filter(|d| d.hard_bin == 1).count();
    let failed_devices = total_prr_devices - passed_devices;

    let total_measurements: i64 = conn.query_row("SELECT COUNT(*) FROM parametric_tests", [], |row| row.get(0))?;
    let failed_measurements: i64 = conn.query_row("SELECT COUNT(*) FROM parametric_tests WHERE is_fail = true", [], |row| row.get(0))?;
    let passed_measurements = total_measurements - failed_measurements;

    let device_yield = if total_prr_devices > 0 {
        (passed_devices as f64 / total_prr_devices as f64) * 100.0
    } else {
        0.0
    };

    println!("\n📊 --- SUMMARY METRICS ---");
    println!("📌 Device Name: {} | Lot ID: '{}'", device_name, lot_id);
    println!("📊 Total PIR (Starts): {} | Total PRR Records: {}", total_pir_starts, total_prr_devices);
    println!("📊 Distinct Non-Empty PART_IDs: {}", distinct_part_ids);
    println!("📊 Device Pass Yield: {:.2}% ({}/{} passed)", device_yield, passed_devices, total_prr_devices);
    println!("📊 Total Parametric Measurements: {}", total_measurements);

    // Filter failing devices to send to LLaMA
    let failed_device_list: Vec<&DeviceDetail> = all_devices.iter().filter(|d| d.hard_bin != 1).collect();

    // 5. CALL OLLAMA WITH STRICTLY VERIFIED DEVICE DATA
    println!("\n🤖 --- GENERATING AI YIELD SUMMARY (Ollama) ---");

    let prompt_data = json!({
        "context": "Semiconductor Yield & Failure Analysis Report",
        "lot_info": {
            "device_name": device_name,
            "lot_id": lot_id
        },
        "device_level_summary": {
            "pir_test_starts": total_pir_starts,
            "total_prr_devices_tested": total_prr_devices,
            "distinct_part_ids_found": distinct_part_ids,
            "chips_passed": passed_devices,
            "chips_failed": failed_devices,
            "chip_yield_percent": format!("{:.2}%", device_yield)
        },
        "failed_device_details": failed_device_list,
        "parametric_measurement_summary": {
            "total_measurements": total_measurements,
            "passed_measurements": passed_measurements,
            "failed_measurements": failed_measurements
        },
        "instructions": "Provide 3 concise engineering observations. Note whether PART_IDs are populated or if devices are identified by Site/Wafer coordinates."
    });
	
	let json_string = serde_json::to_string_pretty(&prompt_data)?;
    std::fs::write("stdf_yield_report.json", &json_string)?;
    println!("💾 Saved JSON payload to 'stdf_yield_report.json'");

    let prompt_text = format!(
        "You are an expert semiconductor yield engineer. Analyze the JSON test metrics below:\n```json\n{}\n```\nProvide exactly 3 concise engineering observations based solely on the provided JSON values.",
        serde_json::to_string_pretty(&prompt_data)?
    );

    let client = reqwest::blocking::Client::new();
    let request_body = OllamaRequest {
        model: "llama3.2:latest".to_string(),
        prompt: prompt_text,
        stream: false,
        options: json!({ "temperature": 0.0 }),
    };

    let res = client
        .post("http://localhost:11434/api/generate")
        .json(&request_body)
        .send();

    match res {
        Ok(response) => {
            if response.status().is_success() {
                let ollama_res: OllamaResponse = response.json()?;
                println!("\n--- AI ENGINEER REPORT ---\n{}", ollama_res.response);
            } else {
                println!("❌ Ollama API returned status: {}", response.status());
            }
        }
        Err(e) => {
            println!("❌ Failed to connect to Ollama: {}", e);
        }
    }

    Ok(())
}