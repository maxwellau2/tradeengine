# Trade Engine + Trade Server (Rust)

TODO
1. Formalise the packets coming in from Hyperliquid (OrderUpdates, PositionUpdates, BalanceUpdate)
2. Test Hyperliquid
3. Test IPC from Engine to TS
4. Decide on architecure of Execution Node of TS
5. Write the methods to post by ws using Tokio Tungstenite (currently for hyperliquid and binance)
6. Can try to figure out how to create a Stream Based HTTP Client? Is it even possible?
7. Core Pinning


## Outline
- This repo is a rust-based Market Data Ingestion Engine + Trade Server. It is designed with thread-per-core in mind.
- *Intra*-Process communication is done using lock-free ring buffers (ringbuf crate)
- *Inter*-Process communication is done using iceoryx2 SHM IPC (iceoryx2 crate)

Ideally, each thread would get it's own core, but in the event we only have 2 cores to work with:

- Core 1: MDEngine, Strategy, TS Central + OS
- Core 2: TS Execution

If we have 4 cores:

- Core 1: MDEngine, Strategy, TS Central
- Core 2: TS Central
- Core 3: TS Execution
- Core 4: Monitoring Services + OS

## Core Architecture

### MD Engine
- Subscribes to all the market data feeds via websocket (tokio tungstenite)
- Sends kline, orderbook, etc to `MDFeedEngine`
- `MDFeedEngine` would have a `Strategy` struct to define what to do when the data comes in
- Any placements of orders, cancels, replacements will be routed to `TradeServer (TS)` via SHM ipc. (`iceorxy2`)

### Trade Server
- When order comes in from MD Engine, it will pass through the state manager first, and from there it will be routed to the appropriate `execution node`.
- `exeuction nodes` will receive responses in an ingress queue (see run_once method)
- It will also be subscribed to the `order updates` channels on the all the `exchanges`.


## Target Benchmarks

1. End-to-end latency of 20 microseconds (excluding network time)
2. p99.9 of about 300 microseconds under relatively high load (not sure how to quantify this)


## Learrning points
- Learn rust lol
- Learn performance tuning methods
- Better software design patterns
