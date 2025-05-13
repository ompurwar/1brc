use rand::{distributions::Uniform, prelude::*};
use std::fs::File;
use std::io::{BufWriter, Write as IoWrite}; // Renamed std::io::Write
use std::sync::{Arc, Mutex, mpsc}; // Added mpsc
use std::time::Instant;
use std::thread; // Added thread

fn log_stage(start: &Instant, msg: &str) {
    println!("[{:>6.2}s] {}", start.elapsed().as_secs_f64(), msg);
}

fn main() -> std::io::Result<()> {
    use rayon::prelude::*;

    let main_start = Instant::now();
    log_stage(&main_start, "Starting generation process with ring buffer strategy");

    let row_count: usize = 1_000_000_000;
    let unique_cities = 50_000;
    let chunk_size: usize = 1_000_000; // Number of rows processed by each Rayon task into one buffer (was 100_000_000)

    // Ring buffer configuration
    const NUM_RING_BUFFERS: usize = 12; // Number of buffers in the system (was 6)
                                       // Estimate ~30 bytes per line (e.g., "City_12345;-XX.XXXX\\n")
    let single_buffer_capacity: usize = chunk_size.saturating_mul(30).max(1024 * 1024); // Min 1MB capacity
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

    // Channels for buffer management
    let (empty_buffer_tx, empty_buffer_rx_shared): (mpsc::Sender<Vec<u8>>, mpsc::Receiver<Vec<u8>>) = mpsc::channel();
    let empty_buffer_rx_arc = Arc::new(Mutex::new(empty_buffer_rx_shared));

    let (full_buffer_tx, full_buffer_rx_for_writer): (mpsc::Sender<Vec<u8>>, mpsc::Receiver<Vec<u8>>) = mpsc::channel();

    // Populate initial empty buffers
    for i in 0..NUM_RING_BUFFERS {
        let buffer = Vec::with_capacity(single_buffer_capacity);
        log_stage(&main_start, &format!("Initialized and sent empty buffer #{} to pool (capacity {} bytes)", i + 1, single_buffer_capacity));
        empty_buffer_tx.send(buffer).unwrap();
    }

    // --- Writer Thread ---
    let writer_thread_writer_arc = Arc::clone(&shared_writer);
    let writer_thread_empty_buffer_tx = empty_buffer_tx.clone(); // Writer uses this to send empty buffers back
    let writer_log_start = Instant::now();

    let writer_handle = thread::spawn(move || {
        log_stage(&writer_log_start, "Writer thread started. Waiting for full buffers.");
        let mut buffers_written_count = 0;
        loop {
            match full_buffer_rx_for_writer.recv() {
                Ok(mut buffer_to_write) => {
                    if buffer_to_write.is_empty() && buffer_to_write.capacity() == 0 { // Check for explicit poison pill (optional, if used)
                        log_stage(&writer_log_start, "Writer thread received explicit poison pill. Shutting down.");
                        break;
                    }
                    if buffer_to_write.is_empty() { // An empty buffer might mean something else, or just no data for that chunk
                        log_stage(&writer_log_start, "Writer received an empty buffer (non-poison). Sending back to pool.");
                    } else {
                        log_stage(&writer_log_start, &format!("Writer received full buffer ({} bytes). Writing to disk.", buffer_to_write.len()));
                        let write_op_start = Instant::now();
                        {
                            let mut writer_guard = writer_thread_writer_arc.lock().unwrap();
                            writer_guard.write_all(&buffer_to_write).unwrap();
                        } // Mutex guard dropped
                        log_stage(&write_op_start, &format!("Writer finished writing buffer to disk ({} bytes).", buffer_to_write.len()));
                        buffers_written_count += 1;
                    }
                    
                    buffer_to_write.clear(); // Prepare for reuse
                    if writer_thread_empty_buffer_tx.send(buffer_to_write).is_err() {
                        log_stage(&writer_log_start, "Writer: empty_buffer_rx disconnected by producers. Exiting send loop.");
                        break; 
                    }
                }
                Err(_) => { // All full_buffer_tx senders dropped
                    log_stage(&writer_log_start, "Writer: full_buffer_tx disconnected. All data sent. Shutting down.");
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

    // --- Producer Logic (Rayon) ---
    let data_gen_start = Instant::now();
    log_stage(&data_gen_start, "Starting parallel generation of data chunks into ring buffers.");

    let cities_for_rayon = Arc::clone(&cities);
    let indices_iterator = (0..row_count);

    indices_iterator
        .into_par_iter()
        .chunks(chunk_size) // Each chunk of indices is processed by one Rayon task
        .for_each(|chunk_of_indices| {
            let task_full_buffer_tx = full_buffer_tx.clone();
            let task_empty_buffer_rx_accessor = Arc::clone(&empty_buffer_rx_arc);
            let task_cities = Arc::clone(&cities_for_rayon);
            // temp_range is Copy

            let task_start_time = Instant::now();
            let current_chunk_item_count = chunk_of_indices.len();
            // log_stage(&task_start_time, &format!("Producer task started for {} items. Requesting empty buffer.", current_chunk_item_count));

            let mut buffer_for_writing;
            { // Scope for Mutex guard over empty_buffer_rx
                let empty_rx_guard = task_empty_buffer_rx_accessor.lock().unwrap();
                match empty_rx_guard.recv() {
                    Ok(b) => {
                        // log_stage(&task_start_time, &format!("Producer task acquired empty buffer (capacity {}).", b.capacity()));
                        buffer_for_writing = b;
                    },
                    Err(e) => {
                        log_stage(&task_start_time, &format!("Producer task failed to get empty buffer: {}. Terminating task.", e));
                        return; // This Rayon task ends
                    }
                }
            } // Mutex guard dropped
            buffer_for_writing.clear(); // Ensure it's pristine for this chunk

            for _idx_in_chunk in chunk_of_indices { // We only care about the count of items for this chunk
                let city = task_cities.choose(&mut rand::thread_rng()).unwrap();
                let temp: f64 = rand::thread_rng().sample(temp_range);
                let line = format!("{};{:.4}\n", city, temp);
                buffer_for_writing.extend_from_slice(line.as_bytes());
            }
            
            // log_stage(&task_start_time, &format!("Producer task finished generating data ({} bytes) for {} items. Sending to full pool.", buffer_for_writing.len(), current_chunk_item_count));
            if task_full_buffer_tx.send(buffer_for_writing).is_err() {
                log_stage(&task_start_time, "Producer task: Writer thread shut down. Failed to send full buffer.");
            }
        });

    log_stage(&data_gen_start, "All producer tasks launched and completed by Rayon.");

    // Signal writer to shut down by dropping the main sender for full buffers.
    // Rayon tasks also held clones, which are dropped when they finish.
    log_stage(&main_start, "Main thread: All producers finished. Dropping main full_buffer_tx to signal writer.");
    drop(full_buffer_tx);

    // The main thread also drops its clone of empty_buffer_tx.
    // The writer holds one, producers hold Arcs to the receiver.
    drop(empty_buffer_tx);
    // The Arc for the receiver will be dropped when all producer tasks are done and the main thread drops its copy.
    drop(empty_buffer_rx_arc);


    log_stage(&main_start, "Main thread: Waiting for writer thread to finish...");
    writer_handle.join().expect("Writer thread panicked");

    let duration = main_start.elapsed();
    println!(
        "✅ Done writing {} rows in {:.2?} using ring buffer strategy.",
        row_count, duration
    );

    Ok(())
}
