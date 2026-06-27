# QuantPipeline

An event-driven arbitrage research framework that simulates data feeds, detects price anomalies, searches for profitable trade combinations, and routes them through a simulated executor with risk guardrails.

## Overview

QuantPipeline is a single-file prototyping scaffold for alternative-data quantitative arbitrage. It wires together the full loop of a research engine: ingestion, anomaly detection, optimization, and execution. Everything runs on simulated feeds, so the project is meant for experimenting with the architecture and the decision logic, not for trading live funds.

The whole pipeline runs from `QuantPipeline.py`. Background threads generate simulated market data into a queue, a main loop drains the queue into an in-memory event store, and a strategy pass periodically reads recent state, builds candidate opportunities, selects a subset under budget and exposure limits, and sends them to the executor.

## How it works

### Feeds

Two simulated feed generators run on their own daemon threads and push `Event` objects into a shared `Queue`:

- `sim_crypto_pool_feed` models constant-product AMM pools. Each pool holds two token reserves that drift via a random walk, with occasional injected imbalance spikes. It emits `pool_update` events carrying both reserves and the derived pool price.
- `sim_market_price_feed` models parallel marketplace price ticks for a set of SKUs, with slow-moving base prices and occasional temporary underpricing. It emits `price_tick` events.

The main loop drains the queue and writes events into `EventStore`, an in-memory dictionary keyed by source that prunes entries older than the configured history window. `EventStore.tail_df` returns recent events for a source as a pandas DataFrame for the detectors and optimizers to consume.

### Detectors

Two detectors run over the recent price history of each market source:

- `zscore_detector` computes a rolling z-score of the latest price against the window mean and standard deviation, and fires when the absolute z-score crosses the configured threshold.
- `change_point_detector` splits the recent window in half, compares the two means, and normalizes the difference by volatility to produce a change-point score, firing when the score crosses its sensitivity threshold.

### Graph arbitrage search plus knapsack selection

The optimization stage has two parts:

- `multihop_pool_search` builds a directed graph from current pool prices using `networkx`. Each pool contributes two directed edges between its tokens, weighted with the negative log of the exchange rate so that a negative-cycle (Bellman-Ford style) corresponds to a profitable loop. It enumerates 2-hop and 3-hop cycles, computes the product of rates around each cycle, keeps cycles whose profit ratio clears a small threshold, and converts them into candidate opportunities with an estimated profit and cost.
- `simple_knapsack_decision` takes the combined list of pool-cycle and market-signal opportunities and runs a greedy knapsack: it sorts candidates by profit per dollar of cost, then picks them in order until the budget is exhausted, skipping any below the minimum expected profit.

The strategy orchestration in `scoring_and_trade_pipeline` ties these together: it reads pool states, runs the multihop search, runs the z-score and change-point detectors over each market source, maps fired signals into opportunities with heuristic profit and cost estimates scaled by a risk multiplier, combines everything, runs the knapsack under the remaining budget, and submits the chosen picks for execution.

### Simulated executor with risk caps

`ExecutionEngine` is the execution stage. Before each trade it checks two guardrails in `can_execute`: a global exposure cap across all strategies and a per-market exposure cap. In `simulate` and `dryrun` modes it logs the trade, updates tracked exposure, and returns a fake trade id and fill, so no real orders are ever placed. The `real` mode is a placeholder that raises `NotImplementedError`.

## Tech stack

- Python 3
- `numpy` for the numeric detector math
- `pandas` for the event-history DataFrames
- `networkx` for the arbitrage graph and cycle search
- `python-dateutil` for date parsing and timezone helpers
- Standard library `threading` and `queue` for the concurrent feed and ingestion model, plus `argparse`, `logging`, `dataclasses`, `math`, and `random`

## Setup

Requires Python 3.

```
cd QuantPipeline
python -m venv .venv
# Windows
.venv\Scripts\activate
# macOS / Linux
source .venv/bin/activate
pip install -r requirements.txt
```

## Usage

Run the pipeline by selecting a mode with the `--mode` flag. The default is `simulate`.

```
python QuantPipeline.py --mode simulate
python QuantPipeline.py --mode dryrun
python QuantPipeline.py --mode real
```

- `simulate`: run on simulated feeds with the simulated executor.
- `dryrun`: run without real execution.
- `real`: placeholder mode that is not implemented and raises `NotImplementedError` if the executor is invoked.

The loop runs continuously and logs activity to the console. Stop it with Ctrl+C.

## Structure

Everything lives in `QuantPipeline.py`:

- `Config` and `Event`: configuration dataclass and the unified event schema.
- `EventStore`: in-memory, source-keyed event store with pruning and DataFrame access.
- `sim_crypto_pool_feed`, `sim_market_price_feed`, `ingestion_thread_main`: simulated feeds and the ingestion threads.
- `zscore_detector`, `change_point_detector`: anomaly detectors.
- `multihop_pool_search`, `simple_knapsack_decision`: graph arbitrage search and the trade selector.
- `ExecutionEngine`: executor with risk caps.
- `scoring_and_trade_pipeline`, `main_loop`: strategy orchestration and the runner.

## Limitations

- Runs entirely on simulated data. There are no real exchange, AMM, or marketplace connectors.
- The `real` mode is not implemented and will raise `NotImplementedError`.
- Profit and cost figures for opportunities are heuristic placeholders, not modeled returns. There are no fees, slippage, latency, or settlement effects.
- No persistence, backtesting, or performance reporting. State is in-memory only.
- Not production trading software. Treat it as an architecture and logic prototype.
