use std::fs::File;
use std::hash::BuildHasherDefault;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use crossbeam::channel::{Receiver, bounded};
use hashbrown::HashMap;
use memmap2::Mmap;
use rayon::ThreadPoolBuilder;
use rustc_hash::FxHasher;

type FxHashMap<K, V> = HashMap<K, V, BuildHasherDefault<FxHasher>>;

fn main() -> std::io::Result<()> {
    log_stage(
        &Instant::now(),
        "🚀 Stage: Opening & memory mapping the file",
    );

    let path = "./data/weather_stations_1000000000.csv";
    let file = File::open(path)?;
    let mmap = Arc::new(unsafe { Mmap::map(&file)? });
    let chunk_size = 5_000_000;

    let start = Instant::now();
    log_stage(&start, "📦 Stage: Dispatching chunks via channel");

    let (sender, receiver) = bounded::<Vec<String>>(8);
    let total_processed = Arc::new(AtomicUsize::new(0));
    let aggregated_map = Arc::new(Mutex::new(
        FxHashMap::<String, (u64, f64, f64, f64)>::default(),
    ));

    log_stage(&start, &format!("🧠 Using {} CPUs", num_cpus::get()));

    ThreadPoolBuilder::new()
        .num_threads(8)
        .build_global()
        .unwrap();

    // Producer thread
    let producer_start = start.clone();
    let mmap_clone = Arc::clone(&mmap);
    std::thread::spawn(move || {
        let content = std::str::from_utf8(&mmap_clone).expect("File must be valid UTF-8");

        let mut chunk: Vec<String> = Vec::with_capacity(chunk_size);
        let mut chunk_count = 0;

        for line in content.split('\n') {
            if !line.is_empty() {
                chunk.push(line.to_string());
                if chunk.len() == chunk_size {
                    chunk_count += 1;
                    sender.send(chunk).unwrap();
                    log_stage(
                        &producer_start,
                        &format!("📤 Dispatched chunk #{chunk_count} ({chunk_size} lines)"),
                    );
                    chunk = Vec::with_capacity(chunk_size);
                }
            }
        }

        if !chunk.is_empty() {
            chunk_count += 1;
            let final_len = chunk.len();
            sender.send(chunk).unwrap();
            log_stage(
                &producer_start,
                &format!("📤 Dispatched final chunk #{chunk_count} ({final_len} lines)"),
            );
        }

        drop(sender); // signal end
    });

    log_stage(&start, "⚙️ Stage: Parallel processing begins");

    let global_start = start.clone(); // for accurate speed calculation

    rayon::scope_fifo(|s| {
        for chunk in ChunkReceiverIter::new(receiver) {
            let map = Arc::clone(&aggregated_map);
            let counter = Arc::clone(&total_processed);

            s.spawn_fifo(move |_| {
                let chunk_start = Instant::now(); // NEW

                let partial = process_chunk(&chunk);

                let mut global = map.lock().unwrap();
                for (city, (count, min, sum, max)) in partial {
                    let entry = global.entry(city.to_string()).or_insert((0, min, 0.0, max));
                    entry.0 += count;
                    entry.1 = entry.1.min(min);
                    entry.2 += sum;
                    entry.3 = entry.3.max(max);
                }
                let chunk_duration = chunk_start.elapsed().as_secs_f64();
                let inst_speed = chunk.len() as f64 / chunk_duration;

                let new_total = counter.fetch_add(chunk.len(), Ordering::Relaxed) + chunk.len();
    
                if new_total % 1_000_000 < chunk.len() {
                    let speed = new_total as f64 / global_start.elapsed().as_secs_f64();
                    log_stage(
                        &global_start,
                        &format!(
                            "📊 Processed {new_total} lines | Avg. Speed: {:.2} lines/sec | ⚡ Instant Speed: {:.2} lines/sec",
                            speed,inst_speed
                        ),
                    );
                }
            });
        }
    });

    log_stage(&start, "🧮 Stage: Final aggregation and averaging");

    let final_map = Arc::try_unwrap(aggregated_map)
        .expect("Arc still in use")
        .into_inner()
        .unwrap();

    let result_map: FxHashMap<String, (f64, f64, f64)> = final_map
        .into_iter()
        .map(|(city, (count, min, sum, max))| (city, (min, sum / count as f64, max)))
        .collect();

    let total_lines = total_processed.load(Ordering::Relaxed);
    let duration = start.elapsed();

    log_stage(
        &start,
        &format!("✅ Done. Processed {total_lines} lines in {:.2?}", duration),
    );
    log_stage(
        &start,
        &format!("📍 Total unique cities: {}", result_map.len()),
    );

    for (city, (min, mean, max)) in result_map.iter().take(10) {
        log_stage(
            &start,
            &format!("{city}: min={min:.2}, mean={mean:.2}, max={max:.2}"),
        );
    }

    Ok(())
}

// Processing function
fn process_chunk(chunk: &Vec<String>) -> FxHashMap<String, (u64, f64, f64, f64)> {
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

// Channel iterator wrapper
struct ChunkReceiverIter<T> {
    receiver: Receiver<T>,
}

impl<T> ChunkReceiverIter<T> {
    fn new(receiver: Receiver<T>) -> Self {
        ChunkReceiverIter { receiver }
    }
}

impl<T> Iterator for ChunkReceiverIter<T> {
    type Item = T;
    fn next(&mut self) -> Option<T> {
        self.receiver.recv().ok()
    }
}

// Logger
fn log_stage(start: &Instant, msg: &str) {
    let elapsed = start.elapsed().as_secs_f64();
    println!("[{:>6.2}s] {}", elapsed, msg);
}
