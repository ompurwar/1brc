use std::fs::File;
use std::hash::BuildHasherDefault;
use std::io::{BufRead, BufReader};
use std::time::Instant;

use hashbrown::HashMap;
use memmap2::Mmap;
use rayon::{prelude::*, ThreadPoolBuilder};
use rustc_hash::FxHasher;

type FxHashMap<K, V> = HashMap<K, V, BuildHasherDefault<FxHasher>>;

fn main() -> std::io::Result<()> {
    let path = "./data/weather_stations_1000000000.csv";
    let file = File::open(path)?;
    let mmap = unsafe { Mmap::map(&file)? };
    let content = std::str::from_utf8(&mmap).expect("File must be valid UTF-8");

    let reader = BufReader::new(content.as_bytes());

    let mut final_map: FxHashMap<String, (u64, f64, f64, f64)> = FxHashMap::default();
    let mut chunk = Vec::with_capacity(10_000);
    let mut total_processed = 0;
    let mut next_log_threshold = 1_000_000;

    let start = Instant::now();

    for line in reader.lines() {
        if let Ok(line) = line {
            chunk.push(line);
            if chunk.len() == 10_000 {
                let partial = process_chunk(&chunk);
                for (city, (count, min, sum, max)) in partial {
                    let entry = final_map.entry(city).or_insert((0, min, 0.0, max));
                    entry.0 += count;
                    entry.1 = entry.1.min(min);
                    entry.2 += sum;
                    entry.3 = entry.3.max(max);
                }

                total_processed += 10_000;
                chunk.clear();

                if total_processed >= next_log_threshold {
                    let speed = total_processed as f64 / start.elapsed().as_secs_f64();
                    log_stage(
                        &start,
                        &format!("📊 Processed {} lines so far. Speed: {:.2} lines/sec", total_processed, speed),
                    );
                    next_log_threshold += 1_000_000;
                }
            }
        }
    }

    if !chunk.is_empty() {
        let partial = process_chunk(&chunk);
        for (city, (count, min, sum, max)) in partial {
            let entry = final_map.entry(city).or_insert((0, min, 0.0, max));
            entry.0 += count;
            entry.1 = entry.1.min(min);
            entry.2 += sum;
            entry.3 = entry.3.max(max);
        }
        total_processed += chunk.len();
    }

    let result_map: FxHashMap<String, (f64, f64, f64)> = final_map
        .into_iter()
        .map(|(city, (count, min, sum, max))| (city, (min, sum / count as f64, max)))
        .collect();

    let duration = start.elapsed();
    log_stage(&start, &format!("✅ Done. Processed {} lines in {:.2?}", total_processed, duration));
    log_stage(&start, &format!("Total unique cities: {}", result_map.len()));
    for (city, (min, mean, max)) in result_map.iter().take(10) {
        log_stage(
            &start,
            &format!("{city}: min={:.2}, mean={:.2}, max={:.2}", min, mean, max),
        );
    }

    Ok(())
}

fn process_chunk(chunk: &[String]) -> FxHashMap<String, (u64, f64, f64, f64)> {
    let mut map: FxHashMap<String, (u64, f64, f64, f64)> = FxHashMap::default();
    for line in chunk {
        if let Some((city, temp_str)) = line.split_once(';') {
            if let Ok(temp) = temp_str.parse::<f64>() {
                let entry = map.entry(city.to_string()).or_insert((0, temp, 0.0, temp));
                entry.0 += 1;
                entry.1 = entry.1.min(temp);
                entry.2 += temp;
                entry.3 = entry.3.max(temp);
            }
        }
    }
    map
}

// helper for timestamped logging
fn log_stage(start: &Instant, msg: &str) {
    let elapsed = start.elapsed().as_secs_f64();
    println!("[{:>6.2}s] {}", elapsed, msg);
}
