use std::fs::File;
use std::hash::BuildHasherDefault;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use std::thread;
use std::time::Instant;

use hashbrown::HashMap;
use memchr::memchr_iter;
use memmap2::Mmap;
use rayon::{prelude::*, ThreadPoolBuilder};
use rustc_hash::FxHasher;
use std::str;

type FxHashMap<K, V> = HashMap<K, V, BuildHasherDefault<FxHasher>>;

fn main() -> std::io::Result<()> {
    let start = Instant::now();
    log_stage(&start, "🔄 Mapping file...");

    let path = "./data/weather_stations_1000000000.csv";
    let file = File::open(path)?;
    let mmap = unsafe { Mmap::map(&file)? };

    let thread_count = std::env::var("THREADS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(12);
    log_stage(&start, &format!("🧵 Creating Rayon thread pool with {thread_count} threads..."));

    ThreadPoolBuilder::new()
        .num_threads(thread_count)
        .build_global()
        .expect("❌ Failed to build Rayon thread pool");

    log_stage(&start, "📥 Splitting file into byte chunks by newline...");
    let positions: Vec<usize> = memchr_iter(b'\n', &mmap).collect();
    let total_lines = positions.len();
    log_stage(&start, &format!("📊 Total lines: {}", total_lines));

    let chunk_size = 10000;
    let chunks: Vec<_> = positions.chunks(chunk_size).collect();
    log_stage(&start, &format!("📦 Total chunks: {}", chunks.len()));

    let processed_counter = Arc::new(AtomicUsize::new(0));
    let processed_for_logger = Arc::clone(&processed_counter);

    log_stage(&start, "📡 Starting logger thread...");
    thread::spawn(move || {
        let mut next_log_threshold = 1_000_000;
        loop {
            let processed = processed_for_logger.load(Ordering::Relaxed);
            if processed >= next_log_threshold {
                let elapsed = start.elapsed().as_secs_f64();
                let speed = processed as f64 / elapsed;
                println!(
                    "[{:>6.2}s] 📊 Processed {} lines so far. Speed: {:.2} lines/sec",
                    elapsed, processed, speed
                );
                next_log_threshold += 1_000_000;
            }
            if processed >= total_lines {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(100));
        }
    });

    log_stage(&start, "🚀 Launching parallel processing...");
    let partial_maps: Vec<_> = chunks
        .par_iter()
        .map(|chunk| {
            let mut map: FxHashMap<&str, (u64, f64, f64, f64)> = FxHashMap::default();
            for &pos in *chunk {
                let (line_start, line_end) = find_line(&mmap, pos);
                if let Ok(line) = str::from_utf8(&mmap[line_start..line_end]) {
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
            }
            processed_counter.fetch_add(chunk.len(), Ordering::Relaxed);
            map
        })
        .collect();

    log_stage(&start, "🧮 Merging all partial maps...");
    let mut final_map: FxHashMap<String, (u64, f64, f64, f64)> = FxHashMap::default();
    for partial in partial_maps {
        for (city, (count, min, sum, max)) in partial {
            let entry = final_map.entry(city.to_string()).or_insert((0, min, 0.0, max));
            entry.0 += count;
            entry.1 = entry.1.min(min);
            entry.2 += sum;
            entry.3 = entry.3.max(max);
        }
    }

    log_stage(&start, "🧠 Computing final statistics...");
    let result_map: FxHashMap<String, (f64, f64, f64)> = final_map
        .into_iter()
        .map(|(city, (count, min, sum, max))| (city, (min, sum / count as f64, max)))
        .collect();

    let duration = start.elapsed();
    println!(
        "\n[{:>6.2}s] ✅ Done. Processed {} lines in {:.2?} ({:.2} lines/sec)",
        duration.as_secs_f64(),
        total_lines,
        duration,
        total_lines as f64 / duration.as_secs_f64()
    );
    println!("🏙️ Total unique cities: {}", result_map.len());

    for (city, (min, mean, max)) in result_map.iter().take(10) {
        println!("{city}: min={min:.2}, mean={mean:.2}, max={max:.2}");
    }

    Ok(())
}

fn find_line(mmap: &[u8], end: usize) -> (usize, usize) {
    let start = mmap[..end]
        .iter()
        .rposition(|&b| b == b'\n')
        .map_or(0, |pos| pos + 1);
    (start, end)
}

fn log_stage(start: &Instant, msg: &str) {
    let elapsed = start.elapsed().as_secs_f64();
    println!("[{:>6.2}s] {}", elapsed, msg);
}
