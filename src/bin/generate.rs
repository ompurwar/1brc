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
    use rayon::prelude::*;

    let main_start = Instant::now();
    log_stage(&main_start, "Starting generation process with ring buffer strategy and progress reporting");

    let row_count: usize = 1_000_000_000;
    let unique_cities = 50_000;
    let chunk_size: usize = 1_000_000;

    const NUM_RING_BUFFERS: usize = 12;
    let single_buffer_capacity: usize = chunk_size.saturating_mul(35).max(1024 * 1024);
    log_stage(&main_start, &format!("Config: {} rows, {} unique cities, {} rows/chunk, {} ring buffers, ~{:.2}MB/buffer",
        row_count, unique_cities, chunk_size, NUM_RING_BUFFERS, single_buffer_capacity as f64 / (1024.0 * 1024.0)));

    let file = File::create(format!("./data/weather_stations_{}.csv", row_count))?;
    let shared_writer = Arc::new(Mutex::new(BufWriter::new(file)));
    log_stage(&main_start, &format!("Created output file: ./data/weather_stations_{}.csv", row_count));

    let temp_range = Uniform::new_inclusive(-50.0, 60.0);
    let cities: Arc<Vec<String>> = Arc::new(
        (0..unique_cities)
            .map(|i| format!("City_{}", i))
            .collect()
    );
    log_stage(&main_start, &format!("Generated {} unique city names", unique_cities));

    let progress_stats = Arc::new(ProgressStats::new(row_count));
    let keep_progress_thread_running = Arc::new(AtomicBool::new(true));

    let (empty_buffer_tx, empty_buffer_rx_shared): (mpsc::Sender<Vec<u8>>, mpsc::Receiver<Vec<u8>>) = mpsc::channel();
    let empty_buffer_rx_arc = Arc::new(Mutex::new(empty_buffer_rx_shared));

    let (full_buffer_tx, full_buffer_rx_for_writer): (mpsc::Sender<(Vec<u8>, usize)>, mpsc::Receiver<(Vec<u8>, usize)>) = mpsc::channel();

    for i in 0..NUM_RING_BUFFERS {
        let buffer = Vec::with_capacity(single_buffer_capacity);
        empty_buffer_tx.send(buffer).unwrap();
    }
    log_stage(&main_start, &format!("Initialized and sent {} empty buffers to pool.", NUM_RING_BUFFERS));

    // ProgressBar setup (indicatif)
    let pb = ProgressBar::new(row_count as u64);
    pb.set_style(ProgressStyle::with_template(
        "{bar:40.cyan/blue} {pos:>12}/{len:12} lines [{percent:>3}%] {elapsed_precise} ETA {eta_precise}"
    ).unwrap());

    let progress_thread_stats = Arc::clone(&progress_stats);
    let progress_thread_shutdown_signal = Arc::clone(&keep_progress_thread_running);
    let pb_for_thread = pb.clone();
    let progress_handle = thread::spawn(move || {
        while progress_thread_shutdown_signal.load(Ordering::SeqCst) {
            thread::sleep(Duration::from_millis(500));

            let stats = &progress_thread_stats;
            let mut last_report_time_guard = stats.last_report_time.lock().unwrap();
            let now = Instant::now();
            let elapsed_since_last_report = now.duration_since(*last_report_time_guard).as_secs_f64();

            if elapsed_since_last_report < 0.01 {
                continue;
            }

            let current_generated_lines = stats.generated_lines_count.load(Ordering::Relaxed);
            let current_generated_bytes = stats.generated_bytes_count.load(Ordering::Relaxed);
            let current_written_lines = stats.written_lines_count.load(Ordering::Relaxed);
            let current_written_bytes = stats.written_bytes_count.load(Ordering::Relaxed);

            let prev_gen_lines = stats.prev_generated_lines.swap(current_generated_lines, Ordering::Relaxed);
            let prev_gen_bytes = stats.prev_generated_bytes.swap(current_generated_bytes, Ordering::Relaxed);
            let prev_written_lines = stats.prev_written_lines.swap(current_written_lines, Ordering::Relaxed);
            let prev_written_bytes = stats.prev_written_bytes.swap(current_written_bytes, Ordering::Relaxed);

            *last_report_time_guard = now;
            drop(last_report_time_guard);

            let gen_lines_rate = (current_generated_lines - prev_gen_lines) as f64 / elapsed_since_last_report;
            let gen_bytes_rate = (current_generated_bytes - prev_gen_bytes) as f64 / elapsed_since_last_report;
            let write_lines_rate = (current_written_lines - prev_written_lines) as f64 / elapsed_since_last_report;
            let write_bytes_rate = (current_written_bytes - prev_written_bytes) as f64 / elapsed_since_last_report;

            let gen_progress_pct = if stats.total_lines > 0 { (current_generated_lines as f64 / stats.total_lines as f64) * 100.0 } else { 0.0 };
            let write_progress_pct = if stats.total_lines > 0 { (current_written_lines as f64 / stats.total_lines as f64) * 100.0 } else { 0.0 };

            let gen_lines_rate = if gen_lines_rate < 0.0 { 0.0 } else { gen_lines_rate };
            let gen_bytes_rate = if gen_bytes_rate < 0.0 { 0.0 } else { gen_bytes_rate };
            let write_lines_rate = if write_lines_rate < 0.0 { 0.0 } else { write_lines_rate };
            let write_bytes_rate = if write_bytes_rate < 0.0 { 0.0 } else { write_bytes_rate };

            // Humanize numbers
            let gen_lines_h = humanize_number(current_generated_lines);
            let gen_bytes_h = humanize_number(current_generated_bytes);
            let write_lines_h = humanize_number(current_written_lines);
            let write_bytes_h = humanize_number(current_written_bytes);
            let total_lines_h = humanize_number(stats.total_lines as u64);

            // Ratio as gen:written (simplified)
            let ratio_gcd = gcd(current_generated_lines, current_written_lines.max(1));
            let ratio_gen = current_generated_lines / ratio_gcd;
            let ratio_written = current_written_lines / ratio_gcd;

            println!(
                "\r[Gen: {:>6.2}% | {:>7.0} lines/s | {:>5.1} MB/s] [Write: {:>6.2}% | {:>7.0} lines/s | {:>5.1} MB/s] Ratio: {}:{}  Gen: {} lines/{} MB  Write: {} lines/{} MB  Total: {} lines",
                gen_progress_pct,
                gen_lines_rate,
                gen_bytes_rate / (1024.0 * 1024.0),
                write_progress_pct,
                write_lines_rate,
                write_bytes_rate / (1024.0 * 1024.0),
                ratio_gen, ratio_written,
                gen_lines_h, gen_bytes_h, write_lines_h, write_bytes_h, total_lines_h
            );
            pb_for_thread.set_position(current_written_lines);
        }
        pb_for_thread.finish_and_clear();
        // Clean up the last line
        print!("\r{: <120}\r", "");
        std::io::stdout().flush().unwrap_or_default();
    });

    let writer_thread_writer_arc = Arc::clone(&shared_writer);
    let writer_thread_empty_buffer_tx = empty_buffer_tx.clone();
    let writer_log_start = Instant::now();
    let writer_progress_stats = Arc::clone(&progress_stats);

    let writer_handle = thread::spawn(move || {
        let mut buffers_written_count = 0;
        loop {
            match full_buffer_rx_for_writer.recv() {
                Ok((mut buffer_to_write, num_lines_in_buffer)) => {
                    if buffer_to_write.is_empty() && buffer_to_write.capacity() == 0 {
                        break;
                    }
                    if !buffer_to_write.is_empty() {
                        {
                            let mut writer_guard = writer_thread_writer_arc.lock().unwrap();
                            writer_guard.write_all(&buffer_to_write).unwrap();
                        }

                        writer_progress_stats.written_lines_count.fetch_add(num_lines_in_buffer as u64, Ordering::Relaxed);
                        writer_progress_stats.written_bytes_count.fetch_add(buffer_to_write.len() as u64, Ordering::Relaxed);
                        buffers_written_count += 1;
                    }

                    buffer_to_write.clear();
                    if writer_thread_empty_buffer_tx.send(buffer_to_write).is_err() {
                        break;
                    }
                }
                Err(_) => {
                    break;
                }
            }
        }

        log_stage(&writer_log_start, &format!("Writer thread flushing final data. Total buffers processed: {}.", buffers_written_count));
        let flush_op_start = Instant::now();
        {
            let mut writer_guard = writer_thread_writer_arc.lock().unwrap();
            writer_guard.flush().unwrap();
        }
        log_stage(&flush_op_start, "Writer thread finished flushing.");
        log_stage(&writer_log_start, "Writer thread finished.");
    });

    let data_gen_start = Instant::now();
    log_stage(&data_gen_start, "Starting parallel generation of data chunks into ring buffers.");

    let cities_for_rayon = Arc::clone(&cities);
    let producer_progress_stats = Arc::clone(&progress_stats);

    (0..row_count)
        .into_par_iter()
        .chunks(chunk_size)
        .for_each(|chunk_of_indices| {
            let task_full_buffer_tx = full_buffer_tx.clone();
            let task_empty_buffer_rx_accessor = Arc::clone(&empty_buffer_rx_arc);
            let task_cities = Arc::clone(&cities_for_rayon);
            let task_progress_stats = Arc::clone(&producer_progress_stats);

            let current_chunk_item_count = chunk_of_indices.len();

            let mut buffer_for_writing;
            {
                let empty_rx_guard = task_empty_buffer_rx_accessor.lock().unwrap();
                match empty_rx_guard.recv() {
                    Ok(b) => buffer_for_writing = b,
                    Err(_) => {
                        return;
                    }
                }
            }
            buffer_for_writing.clear();

            let mut local_rng = rand::thread_rng();

            for _idx_in_chunk in chunk_of_indices {
                let city = task_cities.choose(&mut local_rng).unwrap();
                let temp: f64 = local_rng.sample(temp_range);
                writeln!(buffer_for_writing, "{};{:.4}", city, temp).unwrap();
            }

            let bytes_generated_this_chunk = buffer_for_writing.len();
            task_progress_stats.generated_lines_count.fetch_add(current_chunk_item_count as u64, Ordering::Relaxed);
            task_progress_stats.generated_bytes_count.fetch_add(bytes_generated_this_chunk as u64, Ordering::Relaxed);

            if task_full_buffer_tx.send((buffer_for_writing, current_chunk_item_count)).is_err() {
            }
        });

    log_stage(&data_gen_start, "All producer tasks launched and completed by Rayon.");

    log_stage(&main_start, "Main thread: All producers finished. Dropping main full_buffer_tx to signal writer.");
    drop(full_buffer_tx);
    drop(empty_buffer_tx);
    drop(empty_buffer_rx_arc);

    log_stage(&main_start, "Main thread: Waiting for writer thread to finish...");
    writer_handle.join().expect("Writer thread panicked");

    keep_progress_thread_running.store(false, Ordering::SeqCst);
    log_stage(&main_start, "Main thread: Waiting for progress reporting thread to finish...");
    progress_handle.join().expect("Progress reporting thread panicked");

    let duration = main_start.elapsed();
    println!(
        "✅ Done writing {} rows in {:.2?} using ring buffer strategy with progress.",
        row_count, duration
    );

    Ok(())
}
