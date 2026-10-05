use rust_stdf::stdf_file::StdfReader;
use rust_stdf::StdfRecord;
use std::env;
use std::time::Instant;

fn main() {
    // 1. Read file path passed from command line
    let args: Vec<String> = env::args().collect();
    if args.len() < 2 {
        println!("❌ Error: Please provide an STDF file path.");
        println!("Usage: cargo run --release -- <path_to_your_stdf_file>");
        return;
    }
    let file_path = &args[1];

    println!("🚀 Opening STDF File: {}", file_path);
    let start_time = Instant::now();

    // 2. Open the STDF Reader using StdfReader::new
    let mut reader = match StdfReader::new(file_path) {
        Ok(r) => r,
        Err(err) => {
            println!("❌ Failed to open STDF file: {}", err);
            return;
        }
    };

    // Counters to measure results
    let mut total_records = 0u64;
    let mut ptr_count = 0u64;
    let mut pass_count = 0u64;
    let mut fail_count = 0u64;

    // 3. Iterate over binary records using get_record_iter()
    for record in reader.get_record_iter() {
        total_records += 1;

        match record {
            // Master Information Record (MIR) - Lot Details
            Ok(StdfRecord::MIR(mir)) => {
                println!("\n=================================");
                println!("📌 Lot ID:   {}", mir.lot_id);
                println!("📌 Device:   {}", mir.part_typ);
                println!("📌 Operator: {}", mir.oper_nam);
                println!("=================================\n");
            }

            // Parametric Test Record (PTR) - Test Measurements
            Ok(StdfRecord::PTR(ptr)) => {
                ptr_count += 1;

                // Extract single byte flag from array [u8; 1]
                let test_flag_byte = ptr.test_flg[0];

                // Bit 6 or Bit 7 set in test_flg flags errors/fails
                if test_flag_byte & 0b1100_0000 != 0 {
                    fail_count += 1;
                } else {
                    pass_count += 1;
                }

                // Print first 3 test measurements as a preview
                if ptr_count <= 3 {
                    println!(
                        "Test #{}: Name='{}' | Result={:.4} | Flag=0x{:02X}",
                        ptr.test_num, ptr.test_txt, ptr.result, test_flag_byte
                    );
                }
            }

            // Ignore record parse errors or unhandled record types
            _ => {}
        }
    }

    let duration = start_time.elapsed();

    // 4. Output performance metrics
    println!("\n✅ --- PARSING COMPLETE ---");
    println!("⏱  Time Taken:     {:.3?}", duration);
    println!("📦 Total Records:  {}", total_records);
    println!("📊 PTR Records:    {}", ptr_count);
    println!("✅ Passed Tests:   {}", pass_count);
    println!("❌ Failed Tests:   {}", fail_count);

    if duration.as_secs_f64() > 0.0 {
        let speed = (total_records as f64) / duration.as_secs_f64();
        println!("⚡ Parsing Speed:  {:.2} records/sec", speed);
    }
}