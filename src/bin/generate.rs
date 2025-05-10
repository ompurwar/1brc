use rand::{distributions::Uniform, prelude::*};
use std::fs::File;
use std::io::{BufWriter, Write};
use std::time::Instant;

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
