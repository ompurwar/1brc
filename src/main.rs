use std::fs::File;
use std::sync::Arc;
use std::time::Instant;
use std::thread;
use std::hash::BuildHasherDefault;

use memmap2::Mmap;
use memchr::memchr_iter;
use crossbeam::channel::bounded;
use hashbrown::HashMap;
use rustc_hash::FxHasher;
use rayon::prelude::*;
use fast_float::parse as fast_parse;
use mimalloc::MiMalloc;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

// Per‐chunk stats: (count, min, sum, max), with owned String keys
type ChunkMap = HashMap<String, (u64, f32, f32, f32), BuildHasherDefault<FxHasher>>;
// Final stats: (min, mean, max)
type FinalMap = HashMap<String, (f32, f32, f32), BuildHasherDefault<FxHasher>>;

fn main() -> std::io::Result<()> {
    let start = Instant::now();
    log_stage(&start, "🚀 Memory-mapping the file");

    // 1. Memory‐map the CSV
    let file = File::open("./data/weather_stations_1000000000.csv")?;
    let mmap = Arc::new(unsafe { Mmap::map(&file)? });

    let chunk_size = 20_000_000;
    let estimated_unique_cities = 50_000;

    // 2. Producer + Consumer + Rayon reduction in a scoped thread
    let combined: ChunkMap = thread::scope(|s| {
        let (sender, receiver) = bounded::<Vec<(usize, usize)>>(100);
        let producer_mmap = Arc::clone(&mmap);

        // Producer: collect byte‐ranges of each line
        s.spawn(move || {
            let bytes: &[u8] = &*producer_mmap;
            let mut buf = Vec::with_capacity(chunk_size);
            let mut line_offset = 0;
            let mut total_lines = 0;
            let mut chunk_lines = 0;

            for nl in memchr_iter(b'\n', bytes) { // memchr is SIMD-accelerated
                if nl > line_offset {
                    buf.push((line_offset, nl));
                    total_lines += 1;
                    chunk_lines += 1;
                    if buf.len() == chunk_size {
                        sender.send(std::mem::take(&mut buf)).unwrap();
                        log_stage(&start, &format!("📤 Sent {} lines", total_lines));
                        buf = Vec::with_capacity(chunk_size);
                        chunk_lines = 0; // Reset local chunk counter
                    }
                }
                line_offset = nl + 1;
            }
            if !buf.is_empty() {
                sender.send(buf).unwrap();
            }
            log_stage(&start, &format!("✅ Finished splitting {} lines", total_lines));
            drop(sender);
        });

        // Consumer + parallel processing + reduction
        let mmap_for_tasks = Arc::clone(&mmap);
        receiver
            .into_iter()
            .par_bridge()
            .map({
                // Capture the Arc<Mmap> by value; each task bumps ref count
                let mmap = Arc::clone(&mmap_for_tasks);
                move |ranges: Vec<(usize, usize)>| {
                    let bytes: &[u8] = &*mmap;
                    // Preallocate chunk map with estimated unique cities
                    let mut map: ChunkMap = HashMap::with_capacity_and_hasher(estimated_unique_cities, BuildHasherDefault::<FxHasher>::default());

                    for (start, end) in ranges {
                        // fast_float is SIMD-accelerated for float parsing
                        if let Ok(line) = std::str::from_utf8(&bytes[start..end]) {
                            if let Some((city, temp_str)) = line.split_once(';') {
                                if let Ok(temp) = fast_parse::<f32, _>(temp_str) {
                                    // Use owned String key
                                    let key = city.to_string();
                                    let entry = map.entry(key).or_insert((0, temp, 0.0, temp));
                                    entry.0 += 1;
                                    entry.1 = entry.1.min(temp);
                                    entry.2 += temp;
                                    entry.3 = entry.3.max(temp);
                                }
                            }
                        }
                    }

                    map
                }
            })
            .reduce(|| HashMap::with_capacity_and_hasher(estimated_unique_cities, BuildHasherDefault::<FxHasher>::default()), |mut acc, chunk_map| {
                for (city, (cnt, mn, sum, mx)) in chunk_map {
                    let e = acc.entry(city).or_insert((0, mn, 0.0, mx));
                    e.0 += cnt;
                    e.1 = e.1.min(mn);
                    e.2 += sum;
                    e.3 = e.3.max(mx);
                }
                acc
            })
    });

    // 3. Final aggregation: compute mean
    log_stage(&start, "🧮 Finalizing averages");
    let final_map: FinalMap = {
        let mut map = HashMap::with_capacity_and_hasher(estimated_unique_cities, BuildHasherDefault::<FxHasher>::default());
        for (city, (cnt, mn, sum, mx)) in combined {
            map.insert(city, (mn, sum / cnt as f32, mx));
        }
        map
    };

    log_stage(&start, &format!("✅ Done in {:.2?}", start.elapsed()));
    log_stage(&start, &format!("📍 Total unique cities: {}", final_map.len()));

    // Print top 10 cities
    for (city, (mn, mean, mx)) in final_map.iter().take(10) {
        println!("{:20} min={:.2}, mean={:.2}, max={:.2}", city, mn, mean, mx);
    }

    Ok(())
}

fn log_stage(start: &Instant, msg: &str) {
    println!("[{:>6.2}s] {}", start.elapsed().as_secs_f64(), msg);
}
