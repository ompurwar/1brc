use rand::{distributions::Uniform, prelude::*};
use std::fs::File;
use std::io::{BufWriter, Write as IoWrite};
use std::sync::{Arc, Mutex, mpsc, atomic::{AtomicU64, AtomicBool, Ordering}};
use std::time::{Instant, Duration};
use std::thread;
use indicatif::{ProgressBar, ProgressStyle};
use num::integer::gcd;

fn log_stage(start: &Instant, msg: &str) {
    println!("[{:>6.2}s] {}", start.elapsed().as_secs_f64(), msg);
}

fn humanize_number(n: u64) -> String {
    if n >= 1_000_000_000 {
        format!("{:.2}b", n as f64 / 1_000_000_000.0)
    } else if n >= 1_000_000 {
        format!("{:.2}m", n as f64 / 1_000_000.0)
    } else if n >= 1_000 {
        format!("{:.2}K", n as f64 / 1_000.0)
    } else {
        n.to_string()
    }
}

struct ProgressStats {
    total_lines: usize,
    start_time: Instant,

    generated_lines_count: AtomicU64,
    generated_bytes_count: AtomicU64,
    written_lines_count: AtomicU64,
    written_bytes_count: AtomicU64,

    last_report_time: Mutex<Instant>,
    prev_generated_lines: AtomicU64,
    prev_generated_bytes: AtomicU64,
    prev_written_lines: AtomicU64,
    prev_written_bytes: AtomicU64,
}

impl ProgressStats {
    fn new(total_lines: usize) -> Self {
        let now = Instant::now();
        Self {
            total_lines,
            start_time: now,
            generated_lines_count: AtomicU64::new(0),
            generated_bytes_count: AtomicU64::new(0),
            written_lines_count: AtomicU64::new(0),
            written_bytes_count: AtomicU64::new(0),
            last_report_time: Mutex::new(now),
            prev_generated_lines: AtomicU64::new(0),
            prev_generated_bytes: AtomicU64::new(0),
            prev_written_lines: AtomicU64::new(0),
            prev_written_bytes: AtomicU64::new(0),
        }
    }
}

fn main() -> std::io::Result<()> {
    let row_count = 1_000_000_000; // change this: e.g. 1_000_000_000 for 1B
    let unique_cities = 10_000; // Number of unique city names

    let file = File::create(format!("./data/weather_stations_{}.csv", row_count))?;
    let mut writer = BufWriter::new(file);

    let mut rng = rand::thread_rng();
    let temp_range = Uniform::new_inclusive(-50.0, 60.0); // plausible temp range

    let cities: Vec<String> = (0..unique_cities)
        .map(|i| format!("City_{}", i))
        .collect();

    let start = Instant::now();

    for _ in 0..row_count {
        let city = cities.choose(&mut rng).unwrap();
        let temp: f64 = rng.sample(temp_range);
        writeln!(writer, "{};{:.2}", city, temp)?;
    }

    writer.flush()?;

    let duration = start.elapsed();
    println!(
        "✅ Done writing {} rows in {:.2?}",
        row_count, duration
    );

    Ok(())
}
