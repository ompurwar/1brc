use std::fs::File;
use std::hash::BuildHasherDefault;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Instant;

use hashbrown::HashMap;
use memmap2::Mmap;
use rayon::prelude::*;
use rustc_hash::FxHasher;

type FxMap<'a> = HashMap<&'a str, (u64, f64, f64, f64), BuildHasherDefault<FxHasher>>;
type FinalMap = HashMap<String, (f64, f64, f64), BuildHasherDefault<FxHasher>>;

fn main() -> std::io::Result<()> {
    let start = Instant::now();
    log_stage(&start, "🚀 Memory-mapping the file");

    let file = File::open("./data/weather_stations_1000000000.csv")?;
    let mmap = unsafe { Mmap::map(&file)? };
    let content = std::str::from_utf8(&mmap).expect("UTF-8 expected");

    log_stage(&start, "📦 Streaming lines and dispatching chunks");

    let chunk_size = 5_000_000;
    let mut chunks: Vec<Vec<&str>> = Vec::new();
    let mut current_chunk = Vec::with_capacity(chunk_size);

    for line in content.split('\n') {
        if !line.is_empty() {
            current_chunk.push(line);
            if current_chunk.len() == chunk_size {
                chunks.push(current_chunk);
                current_chunk = Vec::with_capacity(chunk_size);
                let processed_so_far: usize = chunks.iter().map(|c| c.len()).sum();
                log_stage(
                    &start,
                    &format!(
                        "📈 Progress: Collected {} chunks (~{} lines each), {} lines processed so far",
                        chunks.len(),
                        chunk_size,
                        processed_so_far
                    ),
                );
            }
        }
    }

    if !current_chunk.is_empty() {
        chunks.push(current_chunk);
    }

    let total_lines: usize = chunks.iter().map(|c| c.len()).sum();
    log_stage(&start, &format!("🧠 Collected {} chunks (~{} lines each)", chunks.len(), chunk_size));
    log_stage(&start, "⚙️ Parallel processing of chunks");

    let global_counter = Arc::new(AtomicUsize::new(0));
    let log_threshold = 10_000_000;

    let reduced: FxMap = chunks
        .into_par_iter()
        .map_init(
            || (Instant::now(), Arc::clone(&global_counter)),
            |(thread_start, counter), chunk| {
                let chunk_start = Instant::now();
                let processed = chunk.len();
                let partial = process_chunk(chunk);

                let new_total = counter.fetch_add(processed, Ordering::Relaxed) + processed;
                if new_total % log_threshold < processed {
                    let avg_speed = new_total as f64 / thread_start.elapsed().as_secs_f64();
                    let inst_speed = processed as f64 / chunk_start.elapsed().as_secs_f64();
                    log_stage(
                        &thread_start,
                        &format!(
                            "📊 Processed {new_total} lines | Avg: {:.2} lines/sec | ⚡ Instant: {:.2} lines/sec",
                            avg_speed, inst_speed
                        ),
                    );
                }

                partial
            },
        )
        .reduce(
            || FxMap::default(),
            |mut acc, map| {
                for (city, (count, min, sum, max)) in map {
                    let e = acc.entry(city).or_insert((0, min, 0.0, max));
                    e.0 += count;
                    e.1 = e.1.min(min);
                    e.2 += sum;
                    e.3 = e.3.max(max);
                }
                acc
            },
        );

    log_stage(&start, "🧮 Aggregation complete. Finalizing averages");

    let result: FinalMap = reduced
        .into_iter()
        .map(|(city, (count, min, sum, max))| (city.to_string(), (min, sum / count as f64, max)))
        .collect();

    log_stage(&start, &format!("✅ Done. Processed {} lines in {:.2?}", total_lines, start.elapsed()));
    log_stage(&start, &format!("📍 Total unique cities: {}", result.len()));

    for (city, (min, mean, max)) in result.iter().take(10) {
        println!("  {city}: min={min:.2}, mean={mean:.2}, max={max:.2}");
    }

    Ok(())
}

fn process_chunk<'a>(chunk: Vec<&'a str>) -> FxMap<'a> {
    let mut map: FxMap<'a> = FxMap::default();
    for line in chunk {
        if let Some((city, temp_str)) = line.split_once(';') {
            if let Ok(temp) = temp_str.parse::<f64>() {
                let entry = map.entry(city).or_insert((0, temp, 0.0, temp));
                entry.0 += 1;
                entry.1 = entry.1.min(temp);
                entry.2 += temp;
                entry.3 = entry.3.max(temp);
            }
        }
    }
    map
}

fn log_stage(start: &Instant, msg: &str) {
    println!("[{:>6.2}s] {}", start.elapsed().as_secs_f64(), msg);
}
