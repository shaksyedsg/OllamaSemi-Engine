use axum::{
    extract::{Query, State},
    response::{Html, IntoResponse},
    routing::{get, post},
    Json, Router,
};
use duckdb::{params, Connection};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::env;
use std::sync::{Arc, Mutex};
use std::time::Instant;
use tower_http::cors::CorsLayer;

#[derive(Serialize, Deserialize, Clone)]
struct DeviceDetail {
    part_id: String,
    head_num: u8,
    site_num: u8,
    x_coord: i16,
    y_coord: i16,
    hard_bin: u16,
    soft_bin: u16,
}

#[derive(Serialize, Deserialize, Clone)]
struct PtrRecord {
    test_num: u32,
    test_name: String,
    result: f64,
    lo_limit: Option<f64>,
    hi_limit: Option<f64>,
    units: String,
    site_num: u8,
    is_fail: bool,
}

#[derive(Serialize, Deserialize, Clone)]
struct DtrRecord {
    text_data: String,
}

#[derive(Serialize, Deserialize, Clone)]
struct SiteYield {
    site_num: u8,
    total_tested: i64,
    passed: i64,
    failed: i64,
    yield_percent: String,
}

#[derive(Serialize, Clone)]
struct SummaryMetrics {
    device_name: String,
    lot_id: String,
    total_pir_starts: i64,
    total_prr_devices: usize,
    passed_devices: usize,
    failed_devices: usize,
    device_yield: f64,
    total_measurements: i64,
    failed_measurements: i64,
    total_dtr_logs: i64,
}

struct AppState {
    db_conn: Mutex<Connection>,
    metrics: SummaryMetrics,
    devices: Vec<DeviceDetail>,
    site_yields: Vec<SiteYield>,
    raw_datalog_text: String,
}

#[derive(Deserialize)]
struct ChatRequest {
    prompt: String,
    selected_site: Option<u8>,
}

#[derive(Deserialize)]
struct PtrQuery {
    site_num: u8,
    part_id: Option<String>,
    limit: Option<usize>,
    offset: Option<usize>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = env::args().collect();
    if args.len() < 2 {
        println!("❌ Error: Please provide an STDF file path.");
        println!("Usage: cargo run --release -- <path_to_stdf_file>");
        return Ok(());
    }
    let file_path = args[1].clone();

    println!("🚀 SemiAI Enterprise Engine Initializing...");
    println!("📂 Parsing STDF File: {}", file_path);
    let start_time = Instant::now();

    let conn = Connection::open_in_memory()?;

    conn.execute_batch(
        "
        CREATE TABLE lots (lot_id TEXT, device TEXT, operator TEXT);
        CREATE TABLE device_starts (head_num INTEGER, site_num INTEGER);
        CREATE TABLE devices (
            head_num INTEGER, site_num INTEGER, part_id TEXT,
            x_coord INTEGER, y_coord INTEGER, hard_bin INTEGER,
            soft_bin INTEGER, part_flg INTEGER
        );
        CREATE TABLE parametric_tests (
            test_num INTEGER, test_name TEXT, result DOUBLE,
            lo_limit DOUBLE, hi_limit DOUBLE, units TEXT,
            site_num INTEGER, part_id TEXT, test_flag INTEGER, is_fail BOOLEAN
        );
        CREATE TABLE datalog_text_records (text_data TEXT);
        ",
    )?;

    let mut reader = match rust_stdf::stdf_file::StdfReader::new(&file_path) {
        Ok(r) => r,
        Err(err) => {
            println!("❌ Failed to open STDF file: {}", err);
            return Ok(());
        }
    };

    let mut datalog_builder = String::new();
    datalog_builder.push_str("========================================================================================================\n");
    datalog_builder.push_str("                        TERADYNE IG-XL / ATE TEXT DATALOG REPORT                        \n");
    datalog_builder.push_str("========================================================================================================\n");

    let mut current_part_id = String::from("0");

    {
        let mut pir_appender = conn.appender("device_starts")?;
        let mut prr_appender = conn.appender("devices")?;
        let mut ptr_appender = conn.appender("parametric_tests")?;
        let mut dtr_appender = conn.appender("datalog_text_records")?;

        for record in reader.get_record_iter() {
            match record {
                Ok(rust_stdf::StdfRecord::MIR(mir)) => {
                    datalog_builder.push_str(&format!(
                        "Lot ID       : {}\nDevice       : {}\nOperator     : {}\nTest Node    : {}\nDate/Time    : {}\n",
                        mir.lot_id, mir.part_typ, mir.oper_nam, mir.node_nam, mir.start_t
                    ));
                    datalog_builder.push_str("--------------------------------------------------------------------------------------------------------\n");
                    datalog_builder.push_str("TestNum  Test Name                 Site     Low Limit       Measured      High Limit   Units  Status\n");
                    datalog_builder.push_str("--------------------------------------------------------------------------------------------------------\n");

                    conn.execute(
                        "INSERT INTO lots (lot_id, device, operator) VALUES (?, ?, ?)",
                        params![mir.lot_id, mir.part_typ, mir.oper_nam],
                    )?;
                }
                Ok(rust_stdf::StdfRecord::DTR(dtr)) => {
                    let txt = dtr.text_dat.to_string();
                    datalog_builder.push_str(&format!(">>> DTR MSG: {}\n", txt));
                    dtr_appender.append_row(params![txt])?;
                }
                Ok(rust_stdf::StdfRecord::PIR(pir)) => {
                    pir_appender.append_row(params![pir.head_num, pir.site_num])?;
                }
                Ok(rust_stdf::StdfRecord::PRR(prr)) => {
                    current_part_id = prr.part_id.to_string();
                    let part_flg_byte = prr.part_flg[0];
                    let is_pass = prr.hard_bin == 1;

                    datalog_builder.push_str("--------------------------------------------------------------------------------------------------------\n");
                    datalog_builder.push_str(&format!(
                        "SUMMARY: Part ID {:<5} | Site {:<2} | Wafer({:>2},{:>2}) | HardBin: {:<2} | SoftBin: {:<2} | Result: {}\n",
                        prr.part_id, prr.site_num, prr.x_coord, prr.y_coord, prr.hard_bin, prr.soft_bin,
                        if is_pass { "PASS" } else { "FAIL" }
                    ));
                    datalog_builder.push_str("--------------------------------------------------------------------------------------------------------\n\n");

                    prr_appender.append_row(params![
                        prr.head_num, prr.site_num, current_part_id.clone(),
                        prr.x_coord, prr.y_coord, prr.hard_bin, prr.soft_bin, part_flg_byte
                    ])?;
                }
                Ok(rust_stdf::StdfRecord::PTR(ptr)) => {
                    let flag_byte = ptr.test_flg[0];
                    let is_fail = (flag_byte & 0b1100_0000) != 0;

                    let lo_lim: Option<f64> = ptr.lo_limit.map(|v| v as f64);
                    let hi_lim: Option<f64> = ptr.hi_limit.map(|v| v as f64);
                    let unit_str = ptr.units.clone().unwrap_or_default();

                    let lo_str = lo_lim.map_or("         -".to_string(), |v| format!("{:>10.4}", v));
                    let hi_str = hi_lim.map_or("         -".to_string(), |v| format!("{:>10.4}", v));

                    datalog_builder.push_str(&format!(
                        "{:<8} {:<25} {:<8} {} {:>14.4} {} {:<6} {}\n",
                        ptr.test_num,
                        if ptr.test_txt.len() > 25 { &ptr.test_txt[..25] } else { &ptr.test_txt },
                        ptr.site_num,
                        lo_str,
                        ptr.result,
                        hi_str,
                        if unit_str.is_empty() { "-" } else { &unit_str },
                        if is_fail { "(F)" } else { " P " }
                    ));

                    ptr_appender.append_row(params![
                        ptr.test_num, ptr.test_txt.to_string(), ptr.result as f64,
                        lo_lim, hi_lim, unit_str, ptr.site_num, current_part_id.clone(), flag_byte, is_fail
                    ])?;
                }
                _ => {}
            }
        }
        pir_appender.flush()?;
        prr_appender.flush()?;
        ptr_appender.flush()?;
        dtr_appender.flush()?;
    }

    let parse_duration = start_time.elapsed();
    println!("✅ Ingestion completed in {:.3?}", parse_duration);

    // COMPUTE METRICS
    let mut device_name = String::from("Unknown");
    let mut lot_id = String::from("Unknown");

    let mut lot_stmt = conn.prepare("SELECT lot_id, device FROM lots LIMIT 1")?;
    let mut lot_rows = lot_stmt.query([])?;
    if let Some(row) = lot_rows.next()? {
        lot_id = row.get(0)?;
        device_name = row.get(1)?;
    }

    let mut all_devices: Vec<DeviceDetail> = Vec::new();
    let mut dev_stmt = conn.prepare("SELECT part_id, head_num, site_num, x_coord, y_coord, hard_bin, soft_bin FROM devices")?;
    let dev_rows = dev_stmt.query_map([], |row| {
        Ok(DeviceDetail {
            part_id: row.get(0)?, head_num: row.get(1)?, site_num: row.get(2)?,
            x_coord: row.get(3)?, y_coord: row.get(4)?, hard_bin: row.get(5)?, soft_bin: row.get(6)?,
        })
    })?;
    for dev in dev_rows { all_devices.push(dev?); }

    let total_pir_starts: i64 = conn.query_row("SELECT COUNT(*) FROM device_starts", [], |row| row.get(0))?;
    let total_prr_devices = all_devices.len();
    let passed_devices = all_devices.iter().filter(|d| d.hard_bin == 1).count();
    let failed_devices = total_prr_devices - passed_devices;
    let total_measurements: i64 = conn.query_row("SELECT COUNT(*) FROM parametric_tests", [], |row| row.get(0))?;
    let failed_measurements: i64 = conn.query_row("SELECT COUNT(*) FROM parametric_tests WHERE is_fail = true", [], |row| row.get(0))?;
    let total_dtr_logs: i64 = conn.query_row("SELECT COUNT(*) FROM datalog_text_records", [], |row| row.get(0))?;

    let device_yield = if total_prr_devices > 0 {
        (passed_devices as f64 / total_prr_devices as f64) * 100.0
    } else { 0.0 };

    let metrics = SummaryMetrics {
        device_name, lot_id, total_pir_starts, total_prr_devices,
        passed_devices, failed_devices, device_yield, total_measurements, failed_measurements,
        total_dtr_logs,
    };

    let mut site_yields: Vec<SiteYield> = Vec::new();
    let mut site_stmt = conn.prepare(
        "SELECT site_num, COUNT(*) as total_site, 
                COUNT(CASE WHEN hard_bin = 1 THEN 1 END) as pass_site, 
                COUNT(CASE WHEN hard_bin != 1 THEN 1 END) as fail_site 
         FROM devices GROUP BY site_num ORDER BY site_num ASC",
    )?;
    let site_rows = site_stmt.query_map([], |row| {
        let site_num: u8 = row.get(0)?;
        let total_site: i64 = row.get(1)?;
        let pass_site: i64 = row.get(2)?;
        let fail_site: i64 = row.get(3)?;
        let s_yield = if total_site > 0 { (pass_site as f64 / total_site as f64) * 100.0 } else { 0.0 };
        Ok(SiteYield {
            site_num, total_tested: total_site, passed: pass_site, failed: fail_site,
            yield_percent: format!("{:.2}%", s_yield),
        })
    })?;
    for s in site_rows { site_yields.push(s?); }

    let shared_state = Arc::new(AppState {
        db_conn: Mutex::new(conn),
        metrics,
        devices: all_devices,
        site_yields,
        raw_datalog_text: datalog_builder,
    });

    let app = Router::new()
        .route("/", get(serve_dashboard))
        .route("/api/summary", get(get_summary))
        .route("/api/devices", get(get_devices))
        .route("/api/ptr", get(get_ptr_paginated))
        .route("/api/dtr", get(get_dtr_logs))
        .route("/api/datalog", get(get_raw_datalog))
        .route("/api/chat", post(handle_chat))
        .layer(CorsLayer::permissive())
        .with_state(shared_state);

    let listener = tokio::net::TcpListener::bind("127.0.0.1:8080").await?;
    println!("\n🌐 SemiAI Dashboard online at: http://localhost:8080\n");

    axum::serve(listener, app).await?;

    Ok(())
}

async fn get_summary(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    Json(json!({ "metrics": state.metrics, "site_yields": state.site_yields }))
}

async fn get_devices(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    Json(state.devices.clone())
}

async fn get_raw_datalog(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    state.raw_datalog_text.clone()
}

async fn get_dtr_logs(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let conn = state.db_conn.lock().unwrap();
    let mut stmt = conn.prepare("SELECT text_data FROM datalog_text_records").unwrap();
    let rows = stmt
        .query_map([], |row| {
            Ok(DtrRecord {
                text_data: row.get(0)?,
            })
        })
        .unwrap();

    let mut logs = Vec::new();
    for r in rows {
        if let Ok(rec) = r {
            logs.push(rec);
        }
    }
    Json(logs)
}

async fn get_ptr_paginated(
    State(state): State<Arc<AppState>>,
    Query(params): Query<PtrQuery>,
) -> impl IntoResponse {
    let limit = params.limit.unwrap_or(100);
    let offset = params.offset.unwrap_or(0);
    let conn = state.db_conn.lock().unwrap();

    let (query_str, query_params) = if let Some(ref pid) = params.part_id {
        if !pid.is_empty() {
            (
                "SELECT test_num, test_name, result, lo_limit, hi_limit, units, site_num, is_fail 
                 FROM parametric_tests WHERE site_num = ? AND part_id = ? LIMIT ? OFFSET ?",
                vec![params.site_num.to_string(), pid.clone(), limit.to_string(), offset.to_string()],
            )
        } else {
            (
                "SELECT test_num, test_name, result, lo_limit, hi_limit, units, site_num, is_fail 
                 FROM parametric_tests WHERE site_num = ? LIMIT ? OFFSET ?",
                vec![params.site_num.to_string(), limit.to_string(), offset.to_string()],
            )
        }
    } else {
        (
            "SELECT test_num, test_name, result, lo_limit, hi_limit, units, site_num, is_fail 
             FROM parametric_tests WHERE site_num = ? LIMIT ? OFFSET ?",
            vec![params.site_num.to_string(), limit.to_string(), offset.to_string()],
        )
    };

    let mut stmt = conn.prepare(query_str).unwrap();

    let mut records = Vec::new();
    if query_params.len() == 4 {
        let rows = stmt
            .query_map(
                params![query_params[0].parse::<u8>().unwrap(), query_params[1].clone(), query_params[2].parse::<i64>().unwrap(), query_params[3].parse::<i64>().unwrap()],
                |row| {
                    Ok(PtrRecord {
                        test_num: row.get(0)?,
                        test_name: row.get(1)?,
                        result: row.get(2)?,
                        lo_limit: row.get(3)?,
                        hi_limit: row.get(4)?,
                        units: row.get(5)?,
                        site_num: row.get(6)?,
                        is_fail: row.get(7)?,
                    })
                },
            )
            .unwrap();
        for r in rows { if let Ok(rec) = r { records.push(rec); } }
    } else {
        let rows = stmt
            .query_map(
                params![query_params[0].parse::<u8>().unwrap(), query_params[1].parse::<i64>().unwrap(), query_params[2].parse::<i64>().unwrap()],
                |row| {
                    Ok(PtrRecord {
                        test_num: row.get(0)?,
                        test_name: row.get(1)?,
                        result: row.get(2)?,
                        lo_limit: row.get(3)?,
                        hi_limit: row.get(4)?,
                        units: row.get(5)?,
                        site_num: row.get(6)?,
                        is_fail: row.get(7)?,
                    })
                },
            )
            .unwrap();
        for r in rows { if let Ok(rec) = r { records.push(rec); } }
    }

    Json(records)
}

async fn handle_chat(
    State(state): State<Arc<AppState>>,
    Json(payload): Json<ChatRequest>,
) -> impl IntoResponse {
    let prompt = payload.prompt.trim();

    // 1. Scoped DuckDB execution (lock released immediately after math completion)
    let (context_summary, exact_stats_block) = {
        let conn = state.db_conn.lock().unwrap();
        let mut summary_text = String::new();
        let mut stats_display = String::new();

        if prompt.to_lowercase().contains("mean")
            || prompt.to_lowercase().contains("average")
            || prompt.to_lowercase().contains("avg")
            || prompt.to_lowercase().contains("min")
            || prompt.to_lowercase().contains("max")
            || prompt.to_lowercase().contains("std")
        {
            let mut extracted_test = String::new();

            if let Some(start) = prompt.find('"') {
                if let Some(end) = prompt[start + 1..].find('"') {
                    extracted_test = prompt[start + 1..start + 1 + end].to_string();
                }
            }

            if extracted_test.is_empty() {
                let words: Vec<&str> = prompt.split_whitespace().collect();
                if let Some(last_word) = words.last() {
                    extracted_test = last_word.trim_matches(|c| c == '\'' || c == '"' || c == '?').to_string();
                }
            }

            let query = if !extracted_test.is_empty() && extracted_test.to_lowercase() != "test" {
                format!(
                    "SELECT COUNT(*), AVG(result), MIN(result), MAX(result), STDDEV(result) FROM parametric_tests WHERE test_name ILIKE '%{}%'",
                    extracted_test
                )
            } else {
                "SELECT COUNT(*), AVG(result), MIN(result), MAX(result), STDDEV(result) FROM parametric_tests".to_string()
            };

            if let Ok(mut stmt) = conn.prepare(&query) {
                if let Ok(mut rows) = stmt.query([]) {
                    if let Ok(Some(row)) = rows.next() {
                        let count: i64 = row.get(0).unwrap_or(0);
                        let avg: f64 = row.get(1).unwrap_or(0.0);
                        let min: f64 = row.get(2).unwrap_or(0.0);
                        let max: f64 = row.get(3).unwrap_or(0.0);
                        let stddev: f64 = row.get(4).unwrap_or(0.0);

                        if count > 0 {
                            let matched_name = if extracted_test.is_empty() { "All Tests" } else { &extracted_test };
                            
                            summary_text = format!(
                                "Test: {}\nSamples (N): {}\nMean (μ): {:.8}\nMin: {:.8}\nMax: {:.8}\nStdDev (σ): {:.8}",
                                matched_name, count, avg, min, max, stddev
                            );

                            stats_display = format!(
                                "📊 **DuckDB Mathematical Ground Truth**\n\n\
                                • **Test Matched**: {}\n\
                                • **Total Samples (N)**: {}\n\
                                • **Mean (μ)**: {:.8}\n\
                                • **Min / Max**: {:.8} / {:.8}\n\
                                • **Std Dev (σ)**: {:.8}\n\n",
                                matched_name, count, avg, min, max, stddev
                            );
                        }
                    }
                }
            }
        }
        (summary_text, stats_display)
    };

    // 2. Build non-blocking system prompt for Ollama
    let system_prompt = format!(
        "You are SemiAI, a semiconductor ATE yield and test engineer.\n\
        Review the ground truth statistical data calculated by DuckDB below and provide a concise, high-level 2-sentence engineering commentary explaining what this mean/distribution indicates.\n\n\
        Data:\n{}\n\n\
        Lot Summary:\n\
        - Device: {}\n\
        - Lot ID: {}\n\
        - Overall Yield: {:.2}%\n\n\
        User Query: {}\n",
        if context_summary.is_empty() { "No specific test matched." } else { &context_summary },
        state.metrics.device_name,
        state.metrics.lot_id,
        state.metrics.device_yield,
        prompt
    );

    // 3. Asynchronous non-blocking HTTP request
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(60))
        .build()
        .unwrap_or_default();

    let res = client
        .post("http://localhost:11434/api/generate")
        .json(&json!({
            "model": "llama3.2:latest",
            "prompt": system_prompt,
            "stream": false,
            "options": { "temperature": 0.2 }
        }))
        .send()
        .await;

    match res {
        Ok(response) => {
            if let Ok(ollama_res) = response.json::<serde_json::Value>().await {
                if let Some(answer) = ollama_res["response"].as_str() {
                    let full_response = format!("{}💬 **SemiAI Assistant Commentary**:\n{}", exact_stats_block, answer);
                    return Json(json!({ "success": true, "response": full_response }));
                }
            }
            Json(json!({ "success": false, "response": "❌ Invalid response received from local Ollama model." }))
        }
        Err(_) => {
            // Fallback response with DuckDB math if Ollama isn't running or timed out
            if !exact_stats_block.is_empty() {
                let fallback = format!(
                    "{}⚠️ *Note: Local Ollama AI timed out or is offline. Displaying exact DuckDB calculation.*",
                    exact_stats_block
                );
                return Json(json!({ "success": true, "response": fallback }));
            }
            Json(json!({ "success": false, "response": "❌ Failed to connect to Ollama at http://localhost:11434. Ensure `ollama serve` is active." }))
        }
    }
}

async fn serve_dashboard() -> Html<&'static str> {
    Html(DASHBOARD_HTML)
}

const DASHBOARD_HTML: &str = r#"
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <title>SemiAI - High Density Datalog & Yield Workbench</title>
    <style>
        :root {
            --bg-color: #0d0d0d;
            --panel-bg: #181818;
            --border-color: #2a2a2a;
            --text-color: #cccccc;
            --accent-color: #007acc;
            --fail-color: #ff5555;
            --pass-color: #50fa7b;
            --font-mono: 'Consolas', 'Courier New', monospace;
        }
        * { box-sizing: border-box; margin: 0; padding: 0; }
        body {
            background-color: var(--bg-color);
            color: var(--text-color);
            font-family: var(--font-mono);
            font-size: 11px;
            display: flex;
            flex-direction: column;
            height: 100vh;
            overflow: hidden;
        }
        header {
            background: #000;
            padding: 6px 12px;
            border-bottom: 1px solid var(--border-color);
            display: flex;
            justify-content: space-between;
            align-items: center;
        }
        header h1 { font-size: 13px; color: #569cd6; }
        .view-toggle { display: flex; gap: 8px; }
        .tab-btn {
            background: #222;
            color: #888;
            border: 1px solid var(--border-color);
            padding: 3px 10px;
            cursor: pointer;
            font-family: var(--font-mono);
            font-size: 11px;
        }
        .tab-btn.active { background: var(--accent-color); color: white; border-color: var(--accent-color); }
        .main-layout {
            display: grid;
            grid-template-columns: 1fr 380px;
            flex: 1;
            height: calc(100vh - 30px);
        }
        .data-section {
            display: flex;
            flex-direction: column;
            border-right: 1px solid var(--border-color);
            overflow: hidden;
        }
        .metrics-bar {
            background: var(--panel-bg);
            padding: 6px 12px;
            border-bottom: 1px solid var(--border-color);
            display: flex;
            gap: 16px;
        }
        .metric-item label { color: #666; font-size: 9px; display: block; }
        .metric-item span { font-weight: bold; font-size: 11px; }
        .view-panel { display: none; flex: 1; overflow: auto; background: var(--bg-color); }
        .view-panel.active { display: flex; flex-direction: column; }
        
        #datalog-view {
            padding: 10px;
            background: #050505;
        }
        #datalog-view pre {
            color: #00ff66;
            font-family: 'Consolas', 'Courier New', monospace;
            font-size: 11px;
            line-height: 1.3;
            white-space: pre;
            overflow-x: auto;
        }
        
        .grid-container { overflow: auto; flex: 1; border-bottom: 1px solid var(--border-color); }
        table { width: 100%; border-collapse: collapse; white-space: nowrap; }
        th, td { padding: 3px 6px; border: 1px solid var(--border-color); font-size: 11px; }
        th { background: #202020; color: #9cdcfe; position: sticky; top: 0; z-index: 2; }
        tr:hover { background-color: #222; cursor: pointer; }
        tr.selected { background-color: #094771 !important; }
        .pass-tag { color: var(--pass-color); font-weight: bold; }
        .fail-tag { color: var(--fail-color); font-weight: bold; }
        
        .ai-sidebar { background: var(--panel-bg); display: flex; flex-direction: column; height: 100%; }
        .sidebar-header { background: #202020; padding: 6px; font-weight: bold; border-bottom: 1px solid var(--border-color); }
        .chat-output { flex: 1; padding: 8px; overflow-y: auto; display: flex; flex-direction: column; gap: 8px; background: #111; }
        .msg { padding: 6px 8px; border-radius: 3px; font-size: 11px; line-height: 1.4; }
        .msg.system { background: #222; border-left: 3px solid #569cd6; }
        .msg.user { background: #0e3a5a; align-self: flex-end; max-width: 90%; }
        .msg.ai { background: #1a2a1a; border-left: 3px solid var(--pass-color); }
        .chat-input-container { padding: 6px; border-top: 1px solid var(--border-color); display: flex; gap: 4px; }
        textarea { flex: 1; background: #111; border: 1px solid var(--border-color); color: var(--text-color); font-family: var(--font-mono); font-size: 11px; padding: 4px; resize: none; height: 42px; }
        button { background: var(--accent-color); color: white; border: none; padding: 0 10px; cursor: pointer; font-family: var(--font-mono); }
    </style>
</head>
<body>
    <header>
        <h1>⚡ SemiAI - Enterprise Semiconductor Yield Workbench (Teradyne IG-XL Datalog Mode)</h1>
        <div class="view-toggle">
            <button class="tab-btn active" onclick="switchTab('grid')">📊 Master Grid View</button>
            <button class="tab-btn" onclick="switchTab('datalog')">📄 Raw .TXT Datalog View</button>
            <button class="tab-btn" style="background:#28a745; color:white;" onclick="downloadDatalog()">💾 Save .TXT File</button>
        </div>
    </header>

    <div class="main-layout">
        <div class="data-section">
            <div class="metrics-bar" id="metrics-summary">Loading metrics...</div>

            <div id="grid-view" class="view-panel active">
                <div class="grid-container" style="max-height: 45%;">
                    <table>
                        <thead>
                            <tr><th>#</th><th>Part ID</th><th>Head</th><th>Site</th><th>Wafer (X, Y)</th><th>Hard Bin</th><th>Soft Bin</th><th>Status</th></tr>
                        </thead>
                        <tbody id="device-rows"><tr><td colspan="8">Loading devices...</td></tr></tbody>
                    </table>
                </div>
                <div class="grid-container">
                    <div style="background:#202020; padding:3px 6px; font-weight:bold; color:#9cdcfe;" id="ptr-header">🔍 PARAMETRIC (PTR) TEST RECORDS</div>
                    <table>
                        <thead>
                            <tr><th>Test #</th><th>Test Name</th><th>Result</th><th>Lo Limit</th><th>Hi Limit</th><th>Units</th><th>Site</th><th>Status</th></tr>
                        </thead>
                        <tbody id="ptr-rows"><tr><td colspan="8" style="color:#666;">Select a device above.</td></tr></tbody>
                    </table>
                </div>
            </div>

            <div id="datalog-view" class="view-panel">
                <pre id="datalog-text">Loading .txt datalog stream...</pre>
            </div>
        </div>

        <div class="ai-sidebar">
            <div class="sidebar-header">🤖 SemiAI Local Assistant</div>
            <div class="chat-output" id="chat-box">
                <div class="msg system">Ready. Select a device or type mathematical queries.</div>
            </div>
            <div class="chat-input-container">
                <textarea id="user-prompt" placeholder="Ask math query or yield question..."></textarea>
                <button onclick="sendQuery()">Send</button>
            </div>
        </div>
    </div>

    <script>
        let selectedSite = null;
        let selectedPartId = null;
        let rawDatalogString = "";

        function switchTab(tab) {
            document.querySelectorAll('.tab-btn').forEach((b, idx) => { if (idx < 2) b.classList.remove('active'); });
            document.querySelectorAll('.view-panel').forEach(p => p.classList.remove('active'));
            if (tab === 'grid') {
                document.querySelectorAll('.tab-btn')[0].classList.add('active');
                document.getElementById('grid-view').classList.add('active');
            } else {
                document.querySelectorAll('.tab-btn')[1].classList.add('active');
                document.getElementById('datalog-view').classList.add('active');
                loadDatalog();
            }
        }

        async function init() {
            const sumRes = await fetch('/api/summary');
            const sumData = await sumRes.json();
            const m = sumData.metrics;
            document.getElementById('metrics-summary').innerHTML = `
                <div class="metric-item"><label>Device</label><span>${m.device_name}</span></div>
                <div class="metric-item"><label>Lot ID</label><span>${m.lot_id || 'N/A'}</span></div>
                <div class="metric-item"><label>Yield</label><span style="color:${m.device_yield > 90 ? 'var(--pass-color)' : 'var(--fail-color)'}">${m.device_yield.toFixed(2)}%</span></div>
                <div class="metric-item"><label>Tested Units</label><span>${m.total_prr_devices}</span></div>
                <div class="metric-item"><label>PTR Tests</label><span>${m.total_measurements}</span></div>
                <div class="metric-item"><label>DTR Text Logs</label><span>${m.total_dtr_logs}</span></div>
            `;

            const devRes = await fetch('/api/devices');
            const devices = await devRes.json();
            const tbody = document.getElementById('device-rows');
            tbody.innerHTML = '';

            devices.forEach((d, idx) => {
                const tr = document.createElement('tr');
                const isPass = d.hard_bin === 1;
                tr.onclick = () => selectRow(tr, d);
                tr.innerHTML = `
                    <td>${(idx + 1).toString().padStart(3, '0')}</td>
                    <td>${d.part_id.trim() ? d.part_id : '&lt;EMPTY&gt;'}</td>
                    <td>${d.head_num}</td>
                    <td>${d.site_num}</td>
                    <td>(${d.x_coord}, ${d.y_coord})</td>
                    <td>${d.hard_bin}</td>
                    <td>${d.soft_bin}</td>
                    <td class="${isPass ? 'pass-tag' : 'fail-tag'}">${isPass ? 'PASS' : 'FAIL'}</td>
                `;
                tbody.appendChild(tr);
            });
        }

        async function selectRow(tr, device) {
            document.querySelectorAll('#device-rows tr.selected').forEach(r => r.classList.remove('selected'));
            tr.classList.add('selected');
            selectedSite = device.site_num;
            selectedPartId = device.part_id;

            document.getElementById('ptr-header').innerText = `🔍 PARAMETRIC (PTR) RECORDS FOR PART ID: ${device.part_id} (Site ${device.site_num})`;

            const ptrRes = await fetch(`/api/ptr?site_num=${device.site_num}&part_id=${device.part_id}&limit=200&offset=0`);
            const ptrRecords = await ptrRes.json();

            const ptrTbody = document.getElementById('ptr-rows');
            ptrTbody.innerHTML = '';

            ptrRecords.forEach(p => {
                const ptrTr = document.createElement('tr');
                const loStr = p.lo_limit !== null ? p.lo_limit.toFixed(6) : '-';
                const hiStr = p.hi_limit !== null ? p.hi_limit.toFixed(6) : '-';

                ptrTr.innerHTML = `
                    <td>${p.test_num}</td>
                    <td>${p.test_name}</td>
                    <td style="font-weight:bold; color:${p.is_fail ? 'var(--fail-color)' : 'var(--text-color)'}">${p.result.toFixed(6)}</td>
                    <td>${loStr}</td>
                    <td>${hiStr}</td>
                    <td>${p.units}</td>
                    <td>${p.site_num}</td>
                    <td class="${p.is_fail ? 'fail-tag' : 'pass-tag'}">${p.is_fail ? 'FAIL' : 'PASS'}</td>
                `;
                ptrTbody.appendChild(ptrTr);
            });
        }

        async function loadDatalog() {
            const txtRes = await fetch('/api/datalog');
            rawDatalogString = await txtRes.text();
            document.getElementById('datalog-text').innerText = rawDatalogString;
        }

        function downloadDatalog() {
            if (!rawDatalogString) {
                alert("Please click 'Raw .TXT Datalog View' first to load the datalog stream.");
                return;
            }
            const blob = new Blob([rawDatalogString], { type: 'text/plain' });
            const url = window.URL.createObjectURL(blob);
            const a = document.createElement('a');
            a.href = url;
            a.download = 'stdf_datalog_report.txt';
            a.click();
            window.URL.revokeObjectURL(url);
        }

        function appendMessage(type, text) {
            const chatBox = document.getElementById('chat-box');
            const div = document.createElement('div');
            div.className = `msg ${type}`;
            div.innerHTML = text.replace(/\n/g, '<br>');
            chatBox.appendChild(div);
            chatBox.scrollTop = chatBox.scrollHeight;
        }

        async function sendQuery() {
            const promptInput = document.getElementById('user-prompt');
            const prompt = promptInput.value.trim();
            if (!prompt) return;

            appendMessage('user', prompt);
            promptInput.value = '';

            appendMessage('system', '⚡ SemiAI querying DuckDB & Ollama...');

            try {
                const res = await fetch('/api/chat', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ prompt: prompt, selected_site: selectedSite })
                });
                const data = await res.json();
                
                const msgs = document.querySelectorAll('.msg.system');
                if (msgs.length > 0) msgs[msgs.length - 1].remove();

                if (data.success) {
                    appendMessage('ai', data.response);
                } else {
                    appendMessage('system', data.response);
                }
            } catch (err) {
                appendMessage('system', '❌ Server timeout or connection error.');
            }
        }

        init();
    </script>
</body>
</html>
"#;