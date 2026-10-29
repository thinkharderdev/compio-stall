# Poll driver (kqueue): reads and writes stall forever when both are pending on one fd

## Summary

On macOS (`polling` driver over kqueue), when a socket has a read and a write pending at the same
time, for example after `TcpStream::into_split`, operations on that socket and on other sockets can
stop completing permanently. Data stays queued in the kernel (`FIONREAD` shows several MB) and the
pending read is never woken, while the runtime keeps running other tasks and timers.

Reproduces on `compio 0.20.0-beta.1` (`compio-driver 0.13.0-beta.1`) and `compio 0.19.2`
(`compio-driver 0.12.5`). A server that only reads never stalls.

## Reproducer

`Cargo.toml`:

```toml
[package]
name = "compio-stall"
version = "0.1.0"
edition = "2024"

[dependencies]
compio = { version = "=0.20.0-beta.1", features = ["net", "macros", "time", "io"] }
futures-util = "0.3"
libc = "0.2"
```

`src/main.rs`: see attached. It runs one compio runtime that accepts 64 loopback connections and,
for each, splits the stream and runs a read loop and a write loop concurrently. Blocking std clients
write continuously and read slowly, so the server always has data to read and its writes regularly
wait for the socket to become writable. A watchdog exits with status 1 once any connection has gone
10s without completing a read.

```
$ cargo run --release
35 of 37 connections stalled after 11.000180458s
  conn  0: no read completed for 10.9s, 4194828 bytes queued in the kernel, write pending: true
  conn  1: no read completed for 10.9s, 4195852 bytes queued in the kernel, write pending: true
  conn  2: no read completed for 10.1s, 4195736 bytes queued in the kernel, write pending: true
  ...

$ READ_ONLY=1 cargo run --release
no stall after 60s
```

The stall is permanent (watched for over 30s), and only 37 of the 64 connections were accepted
because the listener's accept stalls the same way. With the read and write loops spawned as
separate tasks instead of joined in one, it also stalls.

## Cause

In `src/sys/driver/poll/mod.rs`:

1. `FdQueue::event()` builds one `polling::Event` per fd. When both queues are non-empty, the write
   op's key overwrites the read op's key, so both kqueue filters (`EVFILT_READ` and
   `EVFILT_WRITE`) are registered with the write op's key.
2. kqueue reports the two filters as two separate events, both carrying the write op's key. If the
   writable event is handled first and the write completes, the write op's track is left `ready`.
   The readable event in the same batch then resolves to that write op, and `next_fd()` returns
   `None`.
3. That case returns from the whole `with_events` closure:

   ```rust
   // FIXME: This should not happen
   let Some(fd) = op.extra().as_poll().next_fd() else {
       return Ok(());
   };
   ```

   Every remaining event in the batch is dropped, including events for unrelated fds. They were
   registered oneshot and have already fired, so nothing re-arms them. An fd whose read and write
   are both already pending gets no new submission (and so no `modify`), and waits forever.

Logging that branch confirmed it: it ran about 7,000 times in a run that stalled, always on a
`readable` event.

## Verified fix

Changing `return Ok(());` to `continue;` in that branch stops the stall: no stalls over 60s, with
the branch still hit about 4,000 times per run. The skipped readable event is not lost, because
handling the writable event renews the fd with the read interest still queued, which re-arms the
read filter.

A more robust fix would avoid using an op key to identify a kqueue event, since one key can arrive
on two filters for the same fd. For example, key the registration by fd and look up the
`FdQueue` from the event.

It might also be worth checking whether the op a stale key points to can already have been freed
when the second event arrives.

Epoll delivers one event per fd with both flags set, so the Linux polling driver is probably not
affected by this exact sequence. I haven't tested it.

## Environment

- macOS (Darwin 27.0.0), Apple silicon
- Rust stable, release build
- compio 0.20.0-beta.1 / compio-driver 0.13.0-beta.1, and compio 0.19.2 / compio-driver 0.12.5
