//! Reads pending on a socket with data queued never complete on the `polling` driver (kqueue).
//!
//! A single compio runtime accepts connections, splits each with `into_split` and drives a read
//! loop and a write loop on it concurrently. Plain blocking clients write continuously and read
//! slowly, so the server always has data to read and its writes regularly wait for the socket to
//! become writable.
//!
//! A watchdog on the runtime exits with status 1 if any connection has gone `STALL_AFTER` without
//! completing a read, printing how many bytes the kernel has queued for it. Set `READ_ONLY=1` to
//! skip the server writes, which never stalls.

use compio::BufResult;
use compio::io::{AsyncRead, AsyncWrite};
use compio::net::{TcpListener, TcpStream};
use std::cell::{Cell, RefCell};
use std::io::{Read, Write};
use std::os::fd::AsRawFd;
use std::rc::Rc;
use std::time::{Duration, Instant};

const CONNECTIONS: usize = 64;
const BUF_SIZE: usize = 64 * 1024;
const STALL_AFTER: Duration = Duration::from_secs(10);
const RUN_FOR: Duration = Duration::from_secs(60);

struct Conn {
    id: usize,
    fd: i32,
    last_read: Cell<Instant>,
    write_pending: Cell<bool>,
}

fn main() {
    let read_only = std::env::var("READ_ONLY").is_ok();
    let (addr_tx, addr_rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        compio::runtime::Runtime::new()
            .unwrap()
            .block_on(server(addr_tx, read_only))
    });
    let addr = addr_rx.recv().unwrap();

    for _ in 0..CONNECTIONS {
        let mut tx = std::net::TcpStream::connect(addr).unwrap();
        let mut rx = tx.try_clone().unwrap();
        std::thread::spawn(move || {
            let buf = vec![1u8; BUF_SIZE];
            while tx.write_all(&buf).is_ok() {}
        });
        std::thread::spawn(move || {
            let mut buf = vec![0u8; BUF_SIZE];
            while let Ok(1..) = rx.read(&mut buf) {
                std::thread::sleep(Duration::from_millis(1));
            }
        });
    }

    std::thread::sleep(RUN_FOR);
    println!("no stall after {RUN_FOR:?}");
}

async fn server(addr_tx: std::sync::mpsc::Sender<std::net::SocketAddr>, read_only: bool) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    addr_tx.send(listener.local_addr().unwrap()).unwrap();

    let conns: Rc<RefCell<Vec<Rc<Conn>>>> = Rc::default();
    compio::runtime::spawn(watchdog(Rc::clone(&conns))).detach();

    for id in 0..CONNECTIONS {
        let (stream, _) = listener.accept().await.unwrap();
        let conn = Rc::new(Conn {
            id,
            fd: stream.as_raw_fd(),
            last_read: Cell::new(Instant::now()),
            write_pending: Cell::new(false),
        });
        conns.borrow_mut().push(Rc::clone(&conn));
        compio::runtime::spawn(async move {
            let (rx, tx) = stream.into_split();
            if read_only {
                read_loop(rx, &conn).await;
            } else {
                futures_util::future::join(read_loop(rx, &conn), write_loop(tx, &conn)).await;
            }
        })
        .detach();
    }

    futures_util::future::pending::<()>().await;
}

async fn read_loop(mut rx: TcpStream, conn: &Conn) {
    let mut buf = Vec::with_capacity(BUF_SIZE);
    loop {
        buf.clear();
        let BufResult(res, b) = rx.read(buf).await;
        buf = b;
        match res.unwrap() {
            0 => return,
            _ => conn.last_read.set(Instant::now()),
        }
    }
}

async fn write_loop(mut tx: TcpStream, conn: &Conn) {
    let mut buf = vec![2u8; BUF_SIZE];
    loop {
        conn.write_pending.set(true);
        let BufResult(res, b) = tx.write(buf).await;
        conn.write_pending.set(false);
        buf = b;
        res.unwrap();
    }
}

async fn watchdog(conns: Rc<RefCell<Vec<Rc<Conn>>>>) {
    let start = Instant::now();
    loop {
        compio::time::sleep(Duration::from_secs(1)).await;
        let conns = conns.borrow();
        let stalled: Vec<_> = conns
            .iter()
            .filter(|c| c.last_read.get().elapsed() > STALL_AFTER)
            .collect();
        if stalled.is_empty() {
            continue;
        }

        println!(
            "{} of {} connections stalled after {:?}",
            stalled.len(),
            conns.len(),
            start.elapsed()
        );
        for c in stalled {
            println!(
                "  conn {:>2}: no read completed for {:.1?}, {} bytes queued in the kernel, write pending: {}",
                c.id,
                c.last_read.get().elapsed(),
                queued_bytes(c.fd),
                c.write_pending.get(),
            );
        }
        std::process::exit(1);
    }
}

fn queued_bytes(fd: i32) -> i32 {
    let mut n: libc::c_int = 0;
    unsafe { libc::ioctl(fd, libc::FIONREAD, &mut n) };
    n
}
