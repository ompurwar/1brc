use std::fs::File;
use std::hash::BuildHasherDefault;
use std::sync::Arc;
use std::thread;
use std::time::Instant;

use clap::Parser;
use crossbeam::channel::bounded;
use hashbrown::HashMap;
use memchr::{memchr, memchr_iter};
use memmap2::Mmap;
use rayon::prelude::*;
use rustc_hash::FxHasher;

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Use memchr-based splitting instead of standard split_once
    #[arg(short, long)]
    memchr: bool,
}

// Per‐chunk stats: (count, min, sum, max), with owned String keys
type ChunkMap = HashMap<String, (u64, f32, f32, f32), BuildHasherDefault<FxHasher>>;
// Final stats: (min, mean, max)
type FinalMap = HashMap<String, (f32, f32, f32), BuildHasherDefault<FxHasher>>;

fn main() -> std::io::Result<()> {
    let args = Args::parse();

    let start = Instant::now();
    let splitting_method = if args.memchr {
        "memchr-based"
    } else {
        "standard split_once"
    };
    log_stage(
        &start,
        &format!(
            "🚀 Memory-mapping the file (using {} splitting)",
            splitting_method
        ),
    );

    // 1. Memory‐map the CSV
    let file = File::open("./data/weather_stations_1000000000.csv")?;
    let mmap = Arc::new(unsafe { Mmap::map(&file)? });

    let chunk_size = 20_000_000;

    // 2. Producer + Consumer + Rayon reduction in a scoped thread
    let use_memchr = args.memchr;
    let combined: ChunkMap = thread::scope(|s| {
        let (sender, receiver) = bounded::<Vec<(usize, usize)>>(100);
        let producer_mmap = Arc::clone(&mmap);

        // Producer: collect byte‐ranges of each line
        s.spawn(move || {
            let bytes: &[u8] = &*producer_mmap;
            let mut buf = Vec::with_capacity(chunk_size);
            let mut line_offset = 0;
            let mut total_lines = 0;

            for nl in memchr_iter(b'\n', bytes) {
                if nl > line_offset {
                    buf.push((line_offset, nl));
                    total_lines += 1;
                    if buf.len() == chunk_size {
                        sender.send(std::mem::take(&mut buf)).unwrap();
                        log_stage(&start, &format!("📤 Sent {} lines", total_lines));
                        buf = Vec::with_capacity(chunk_size);
                    }
                }
                line_offset = nl + 1;
            }
            if !buf.is_empty() {
                sender.send(buf).unwrap();
            }
            log_stage(
                &start,
                &format!("✅ Finished splitting {} lines", total_lines),
            );
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
                    let mut map: ChunkMap = ChunkMap::default();

                    for (start, end) in ranges {
                        if let Ok(line) = std::str::from_utf8(&bytes[start..end]) {
                            let split_result = if use_memchr {
                                split_line_memchr(line)
                            } else {
                                split_line_standard(line)
                            };

                            if let Some((city, temp_str)) = split_result {
                                if let Ok(temp) = temp_str.parse::<f32>() {
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
            .reduce(ChunkMap::default, |mut acc, chunk_map| {
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
    let final_map: FinalMap = combined
        .into_iter()
        .map(|(city, (cnt, mn, sum, mx))| (city, (mn, sum / cnt as f32, mx)))
        .collect();

    log_stage(&start, &format!("✅ Done in {:.2?}", start.elapsed()));
    log_stage(
        &start,
        &format!("📍 Total unique cities: {}", final_map.len()),
    );

    // Print top 10 cities
    for (city, (mn, mean, mx)) in final_map.iter().take(10) {
        println!("{:20} min={:.2}, mean={:.2}, max={:.2}", city, mn, mean, mx);
    }

    Ok(())
}

fn log_stage(start: &Instant, msg: &str) {
    println!("[{:>6.2}s] {}", start.elapsed().as_secs_f64(), msg);
}

/// Fast memchr-based splitter function that finds the first semicolon
/// and returns (city, temperature_str) if found
#[inline]
fn split_line_memchr(line: &str) -> Option<(&str, &str)> {
    let bytes = line.as_bytes();
    memchr(b';', bytes).map(|pos| unsafe {
        let city = line.get_unchecked(..pos);
        let temp = line.get_unchecked(pos + 1..);
        (city, temp)
    })
}

/// Standard split_once-based splitter function for comparison
/// Returns (city, temperature_str) if found
#[inline]
fn split_line_standard(line: &str) -> Option<(&str, &str)> {
    line.split_once(';')
}
