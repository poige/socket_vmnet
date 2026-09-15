# socket_vmnet sliding-queue fork

This fork of [`lima-vm/socket_vmnet`](https://github.com/lima-vm/socket_vmnet) exists to fix two
things: one slow client could stall every other client, and the daemon spent a syscall per packet
where it could spend one per batch. It also adds the datagram endpoint that file-handle network
attachments need.

The original documentation, including installation and standard configuration, is preserved in
[README.upstream.md](README.upstream.md).

## Throughput

Sustained TCP between two clients on one host, summary bandwidth, no retransmits in either case:

| | Gbit/s |
| --- | --- |
| upstream | 2.4 - 2.6 |
| this fork | **~4.5** |
| this fork, via the datagram endpoint | **~5.1 - 5.2** |

The gain is batching: client reads, `vmnet_write()`, and client writes are all amortized over a
batch of frames instead of paying a syscall each. The datagram endpoint is faster still because it
carries one frame per datagram with no length framing to parse.

These numbers justify the design and the defaults. They are not portable benchmark claims -- packet
size, client implementation, virtualization backend, host load, and OS release all move them.

## One slow client no longer stalls the daemon

Upstream floods each frame by writing directly into every other client's socket. A client that
stops draining its socket therefore blocks the flooding path, and with it every other client on the
same daemon -- [issue #173](https://github.com/lima-vm/socket_vmnet/issues/173).

Here each source publishes into its own bounded ring, and every target has a dedicated delivery
thread reading those rings at its own pace. A target that cannot keep up falls outside the
retention window and loses its own oldest batches at a clean batch boundary, which is what a switch
does with a congested port. It cannot delay anyone else. Verified with a deliberately non-draining
client: healthy clients kept running at full rate, and only the stalled target aged out.

## A datagram endpoint for file-handle clients

Clients built on file-handle network attachments speak one raw Ethernet frame per datagram, not the
length-prefixed stream protocol, so they cannot attach to an upstream daemon at all --
[issue #13](https://github.com/lima-vm/socket_vmnet/issues/13). `--dgram-socket` adds that endpoint
alongside the existing one.

It preserves packet order with a serial edge-triggered drain, batches consecutive frames from the
same peer, and delivers over a connected socket per peer. Socket buffers are sized explicitly: the
platform's default datagram receive buffer holds fewer than three Ethernet frames, and leaving it
alone costs almost all of the throughput.

## Using the datagram endpoint

The normal positional `SOCKET` remains required. Add a second listener with:

```console
socket_vmnet --dgram-socket=/path/to/socket_vmnet.dgram /path/to/socket_vmnet
```

The datagram endpoint has no length prefix: every datagram is exactly one Ethernet frame. Clients
must bind their own Unix datagram address before connecting so the daemon can return traffic to
them.

## Tuning

The defaults are the measured conservative settings. These options are primarily useful when
repeating measurements on a different workload or system:

- `--sockbuf-size=BYTES` sets `SO_SNDBUF` and `SO_RCVBUF` on client sockets. The default is 1 MiB;
  zero leaves the operating-system default untouched. Larger buffers absorb bursts but consume
  more kernel memory per client.
- `--delivery-batch-size=BYTES` limits how much a delivery thread coalesces into one stream write.
  The default is 1 MiB. Larger values amortize syscalls but increase head-of-line latency and the
  amount staged per delivery thread.
- `--busy-poll=USEC` spins a delivery thread briefly before parking it. The default is zero. In the
  measured workload, every nonzero setting increased CPU consumption without a repeatable
  throughput improvement, and longer spins reduced throughput. The option remains available to
  make that tradeoff explicit and to support measurements on other systems.

Send `SIGUSR2` to the daemon to dump live publication, delivery, ring-drop, parking, wakeup, and
datagram counters. These counters are maintained per batch rather than per frame on the normal
path.

## Diagnostic build

Build additional diagnostics with:

```console
make DIAG=1
```

This enables datagram section timers and `--skip-vmnet-write`. The latter deliberately prevents
frames from reaching `vmnet.framework`: DHCP, host access, and external networking stop working,
while local client-to-client flooding remains available. It exists only to measure the cost of the
vmnet boundary. In a diagnostic build, `SIGUSR1` toggles suppression at runtime for an in-process
A/B comparison. Release builds neither advertise nor accept the option.

## AI assistance

AI coding assistants were used during code review, experiment design, implementation, and
documentation. The resulting changes, measurements, and publication history were reviewed and
validated by the project maintainer.
