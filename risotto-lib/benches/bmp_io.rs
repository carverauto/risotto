use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;

use anyhow::Result;
use bytes::{BufMut, Bytes, BytesMut};
use risotto_lib::state::new_state;
use risotto_lib::state_store::memory::MemoryStore;

// The original implementation using `Vec<u8>` for comparison.
pub async fn handle_vec<T: risotto_lib::state_store::store::StateStore>(
    stream: &mut TcpStream,
    state: Option<risotto_lib::state::AsyncState<T>>,
    tx: mpsc::Sender<risotto_lib::update::Update>,
) -> Result<()> {
    let socket = stream.peer_addr().unwrap();
    stream.readable().await?;
    loop {
        let mut common_header = [0; 6];
        let n_bytes_peeked = match stream.peek(&mut common_header).await {
            Ok(n) => n,
            Err(_) => break,
        };

        if n_bytes_peeked == 0 {
            break;
        }
        if n_bytes_peeked != 6 {
            continue;
        }

        let packet_length = u32::from_be_bytes(common_header[1..5].try_into().unwrap()) as usize;
        if packet_length < 6 {
            break;
        }

        let mut buffer = vec![0; packet_length];
        if stream.read_exact(&mut buffer).await.is_err() {
            break;
        }
        let mut buffer_bytes = Bytes::from(buffer);

        // The parser might fail on our dummy data, which is fine for this I/O benchmark.
        let _ =
            risotto_lib::process_bmp_message(state.clone(), tx.clone(), socket, &mut buffer_bytes)
                .await;
    }
    Ok(())
}

// proposed new implementation using `BytesMut`.
pub async fn handle_bytesmut<T: risotto_lib::state_store::store::StateStore>(
    stream: &mut TcpStream,
    state: Option<risotto_lib::state::AsyncState<T>>,
    tx: mpsc::Sender<risotto_lib::update::Update>,
) -> Result<()> {
    let socket = stream.peer_addr().unwrap();
    stream.readable().await?;
    loop {
        let mut common_header = [0; 6];
        let n_bytes_peeked = match stream.peek(&mut common_header).await {
            Ok(n) => n,
            Err(_) => break,
        };

        if n_bytes_peeked == 0 {
            break;
        }
        if n_bytes_peeked != 6 {
            continue;
        }

        let packet_length = u32::from_be_bytes(common_header[1..5].try_into().unwrap()) as usize;
        if packet_length < 6 {
            break;
        }

        let mut buffer = BytesMut::with_capacity(packet_length);
        buffer.resize(packet_length, 0);
        if stream.read_exact(&mut buffer).await.is_err() {
            break;
        }
        let mut buffer_bytes = buffer.freeze();
        // The parser might fail on our dummy data, which is fine for this I/O benchmark.
        let _ =
            risotto_lib::process_bmp_message(state.clone(), tx.clone(), socket, &mut buffer_bytes)
                .await;
    }
    Ok(())
}

// Creates a dummy BMP Termination message. This message type is simple
// and does not require a complex body, making it ideal for an I/O benchmark.
fn create_bmp_termination_message(msg_len: u32) -> BytesMut {
    if msg_len < 6 + 4 {
        // common header + tlv header
        panic!("msg_len too small for a valid message");
    }
    let mut buf = BytesMut::with_capacity(msg_len as usize);
    // BMP Common Header
    buf.put_u8(3); // Version
    buf.put_u32(msg_len); // Length
    buf.put_u8(5); // Message Type: Termination

    // Create a single TLV to fill the message body
    let tlv_len = msg_len - 6 - 4;
    buf.put_u16(0); // Type: String
    buf.put_u16(tlv_len as u16); // Length
    // Fill with arbitrary data
    buf.resize(msg_len as usize, 65); // 'A'
    buf
}

// Server runner using `handle_vec` as the request handler
async fn run_server_vec() -> Result<u16> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let port = listener.local_addr()?.port();
    tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let store = MemoryStore::new();
        let state = new_state(store);
        let (tx, mut rx) = mpsc::channel(1);

        // Drain the receiver so the handler doesn't block
        tokio::spawn(async move {
            while let Some(_) = rx.recv().await {}
        });

        let _ = handle_vec(&mut stream, Some(state), tx).await;
    });
    Ok(port)
}

// Server runner using `handle_bytesmut` as the request handler
async fn run_server_bytesmut() -> Result<u16> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let port = listener.local_addr()?.port();
    tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let store = MemoryStore::new();
        let state = new_state(store);
        let (tx, mut rx) = mpsc::channel(1);

        // Drain the receiver so the handler doesn't block
        tokio::spawn(async move {
            while let Some(_) = rx.recv().await {}
        });

        let _ = handle_bytesmut(&mut stream, Some(state), tx).await;
    });
    Ok(port)
}

// Client that sends a fixed number of bytes
async fn run_client(port: u16, message: &[u8], num_bytes: u64) -> Result<()> {
    let mut stream = TcpStream::connect(("127.0.0.1", port)).await?;
    stream.set_nodelay(true)?;
    let num_messages = num_bytes / (message.len() as u64);
    for _ in 0..num_messages {
        stream.write_all(message).await?;
    }
    stream.shutdown().await?; // Close the stream to signal the end
    Ok(())
}

pub fn bmp_io_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("bmp_io_read_buffer");

    // Total bytes to send in one iteration of the benchmark
    let total_bytes_per_iter = 10 * 1024 * 1024; // 10 MiB

    // Test with a variety of message sizes
    for size in [64, 256, 1024, 4096, 16384].iter() {
        let message = create_bmp_termination_message(*size as u32);
        group.throughput(Throughput::Bytes(total_bytes_per_iter));

        // Benchmark the original Vec<u8> version
        group.bench_with_input(BenchmarkId::new("Vec<u8>", *size), size, |b, _| {
            b.to_async(tokio::runtime::Runtime::new().unwrap()).iter_with_setup(
                || {
                    let rt = tokio::runtime::Handle::current();
                    let port = rt.block_on(run_server_vec()).unwrap();
                    (port, message.clone())
                },
                |(port, msg): (u16, BytesMut)| async move {
                    run_client(port, &msg, total_bytes_per_iter)
                        .await
                        .unwrap();
                },
            );
        });

        // Benchmark the new BytesMut version
        group.bench_with_input(BenchmarkId::new("BytesMut", *size), size, |b, _| {
            b.to_async(tokio::runtime::Runtime::new().unwrap()).iter_with_setup(
                || {
                    let rt = tokio::runtime::Handle::current();
                    let port = rt.block_on(run_server_bytesmut()).unwrap();
                    (port, message.clone())
                },
                |(port, msg): (u16, BytesMut)| async move {
                    run_client(port, &msg, total_bytes_per_iter)
                        .await
                        .unwrap();
                },
            );
        });
    }
    group.finish();
}

criterion_group!(benches, bmp_io_benchmark);
criterion_main!(benches);
