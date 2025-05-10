use criterion::{criterion_group, criterion_main, Criterion};
use fast_float::parse as fast_parse;
use hashbrown::HashMap;
use memmap2::Mmap;
use rayon::prelude::*;
use rustc_hash::FxHasher;
use std::{fs::File, hash::BuildHasherDefault};

type FxHashMap<K, V> = HashMap<K, V, BuildHasherDefault<FxHasher>>;

fn process_std(content: &str) -> FxHashMap<&str, (f64, f64, f64)> {
    let partial_maps: Vec<_> = content
        .par_lines()
        .map(|line| {
            let mut map: FxHashMap<&str, (u64, f64, f64, f64)> = FxHashMap::default();
            if let Some((city, temp_str)) = line.split_once(';') {
                if let Ok(temp) = temp_str.parse::<f64>() {
                    let entry = map.entry(city).or_insert((0, temp, 0.0, temp));
                    entry.0 += 1;
                    entry.1 = entry.1.min(temp);
                    entry.2 += temp;
                    entry.3 = entry.3.max(temp);
                }
            }
            map
        })
        .collect();

    let mut final_map: FxHashMap<&str, (u64, f64, f64, f64)> = FxHashMap::default();
    for partial in partial_maps {
        for (city, (count, min, sum, max)) in partial {
            let entry = final_map.entry(city).or_insert((0, min, 0.0, max));
            entry.0 += count;
            entry.1 = entry.1.min(min);
            entry.2 += sum;
            entry.3 = entry.3.max(max);
        }
    }

    final_map
        .into_iter()
        .map(|(city, (count, min, sum, max))| (city, (min, sum / count as f64, max)))
        .collect()
}

fn process_fastfloat(content: &str) -> FxHashMap<&str, (f64, f64, f64)> {
    let partial_maps: Vec<_> = content
        .par_lines()
        .map(|line| {
            let mut map: FxHashMap<&str, (u64, f64, f64, f64)> = FxHashMap::default();
            if let Some((city, temp_str)) = line.split_once(';') {
                if let Ok(temp) = fast_parse::<f64, _>(temp_str) {
                    let entry = map.entry(city).or_insert((0, temp, 0.0, temp));
                    entry.0 += 1;
                    entry.1 = entry.1.min(temp);
                    entry.2 += temp;
                    entry.3 = entry.3.max(temp);
                }
            }
            map
        })
        .collect();

    let mut final_map: FxHashMap<&str, (u64, f64, f64, f64)> = FxHashMap::default();
    for partial in partial_maps {
        for (city, (count, min, sum, max)) in partial {
            let entry = final_map.entry(city).or_insert((0, min, 0.0, max));
            entry.0 += count;
            entry.1 = entry.1.min(min);
            entry.2 += sum;
            entry.3 = entry.3.max(max);
        }
    }

    final_map
        .into_iter()
        .map(|(city, (count, min, sum, max))| (city, (min, sum / count as f64, max)))
        .collect()
}

fn bench_parsers(c: &mut Criterion) {
    let file = File::open("./data/weather_stations.csv").unwrap();
    let mmap = unsafe { Mmap::map(&file).unwrap() };
    let content = std::str::from_utf8(&mmap).unwrap();

    c.bench_function("parse::<f64>()", |b| {
        b.iter(|| process_std(content));
    });

    c.bench_function("fast_float::parse()", |b| {
        b.iter(|| process_fastfloat(content));
    });
}

criterion_group!(benches, bench_parsers);
criterion_main!(benches);
