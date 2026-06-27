#!/usr/bin/env python3
"""
quant_pipeline.py

Purpose:
 - Prototype an Alternative-Data Quantitative Arbitrage engine.
 - Simulates multiple data feeds, detects anomalies, formulates actions with simple optimization,
   and demonstrates an execution + guardrail pipeline.

Usage:
 - python quant_pipeline.py --mode simulate        # run using simulated feeds
 - python quant_pipeline.py --mode dryrun          # switch to dry-run (no real execution)
 - python quant_pipeline.py --mode real            # placeholder: will attempt to use real connectors (not enabled)

Important:
 - This is a prototyping / research scaffold. Replace connector stubs with real APIs as needed.
 - Add robust error handling, key management and legal/compliance checks before using live funds.
"""

import argparse
import time
import threading
import logging
from queue import Queue, Empty
import random
import math
from dataclasses import dataclass, asdict
from typing import Dict, Any, List, Tuple, Callable, Optional
import numpy as np
import pandas as pd
import networkx as nx
from dateutil import tz, parser as dateparser
from datetime import datetime, timedelta

# -------------------------
# Logging setup
# -------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("quant_pipeline")

# -------------------------
# Config
# -------------------------
@dataclass
class Config:
    # General
    mode: str = "simulate"   # simulate | dryrun | real
    loop_interval: float = 1.0  # seconds between main loop iterations
    keep_history_seconds: int = 3600  # how long to keep recent events
    max_position_usd: float = 50000.0  # total exposure cap across strategies
    per_market_exposure: float = 5000.0  # per-market cap
    min_expected_profit_usd: float = 10.0  # threshold to act
    risk_multiplier: float = 0.5  # conservative sizing
    # Simulated feed params
    num_sim_pools: int = 12
    pool_base_liq: float = 20000.0
    pool_volatility: float = 0.01  # per-second random walk
    # Detector params
    zscore_window: int = 40
    zscore_threshold: float = 4.0
    cp_window: int = 80
    cp_sensitivity: float = 5.0

cfg = Config()

# -------------------------
# Unified Event Schema
# -------------------------
@dataclass
class Event:
    source: str            # e.g., "sim_pool:ETH/USDC", "sim_market:marketA"
    type: str              # e.g., "pool_update", "price_tick", "inventory"
    timestamp: datetime
    payload: Dict[str, Any]

# -------------------------
# In-Memory Event Store
# -------------------------
class EventStore:
    def __init__(self):
        # store events per source (recent only)
        self.store: Dict[str, List[Event]] = {}
    def push(self, ev: Event):
        if ev.source not in self.store:
            self.store[ev.source] = []
        self.store[ev.source].append(ev)
        # prune old
        cutoff = datetime.utcnow() - timedelta(seconds=cfg.keep_history_seconds)
        self.store[ev.source] = [e for e in self.store[ev.source] if e.timestamp >= cutoff]
    def tail_df(self, source: str, n: int = 200) -> pd.DataFrame:
        evs = self.store.get(source, [])[-n:]
        if not evs:
            return pd.DataFrame()
        rows = []
        for e in evs:
            r = {"timestamp": e.timestamp}
            r.update(e.payload)
            rows.append(r)
        return pd.DataFrame(rows)

event_store = EventStore()

# -------------------------
# Simulated Data Feed generators
# -------------------------
def sim_crypto_pool_feed(out_q: Queue, pool_count: int = 8, base_liq=20000.0, vol=0.01):
    """
    Simulate 'constant product' AMM pools with two assets.
    Emits pool_update events with reserves and pool-derived price.
    """
    tokens = ["USDC", "DAI", "USDT", "ETH", "WBTC", "TOKENA", "TOKENB", "TOKENC", "STABLEX", "STABLEY"]
    pools = []
    for i in range(pool_count):
        a = random.choice(tokens)
        b = random.choice([t for t in tokens if t != a])
        # base reserves
        ra = base_liq * random.uniform(0.3, 2.0)
        rb = base_liq * random.uniform(0.3, 2.0)
        pools.append({"pair": f"{a}/{b}", "ra": ra, "rb": rb})
    while True:
        for p in pools:
            # random walk reserves to create drift + volatility
            p["ra"] *= 1.0 + random.normalvariate(0, vol)
            p["rb"] *= 1.0 + random.normalvariate(0, vol)
            # sometimes inject big imbalance spikes
            if random.random() < 0.005:
                # create an exploitable imbalance
                factor = random.uniform(0.7, 1.4)
                if random.random() < 0.5:
                    p["ra"] *= factor
                else:
                    p["rb"] *= factor
                logger.debug(f"sim: injected spike in {p['pair']} factor={factor:.3f}")
            price = p["rb"] / p["ra"] if p["ra"] and p["rb"] else 0.0
            ev = Event(
                source=f"sim_pool:{p['pair']}",
                type="pool_update",
                timestamp=datetime.utcnow(),
                payload={"reserve_a": p["ra"], "reserve_b": p["rb"], "price": price, "pair": p["pair"]}
            )
            out_q.put(ev)
        time.sleep(0.5)

def sim_market_price_feed(out_q: Queue, market_count: int = 6):
    """
    Simulate parallel marketplace price ticks (e.g., two markets listing same SKU with temporary gaps).
    """
    items = [f"SKU-{i}" for i in range(market_count)]
    # base prices
    base = {s: random.uniform(20, 500) for s in items}
    while True:
        for s in items:
            # slow-moving base
            base[s] *= 1.0 + random.normalvariate(0, 0.001)
            # random temporary underpricing
            price = base[s] * (1.0 + random.normalvariate(0, 0.03))
            if random.random() < 0.01:
                price *= random.uniform(0.6, 0.9)  # a cheap listing
            ev = Event(
                source=f"sim_market:{s}",
                type="price_tick",
                timestamp=datetime.utcnow(),
                payload={"sku": s, "price": price, "market": "sim_market"}
            )
            out_q.put(ev)
        time.sleep(0.4)

# -------------------------
# Collectors / ingestion threads
# -------------------------
def ingestion_thread_main(out_q: Queue):
    """
    Thread to start multiple simulated collectors. Replace / extend these collectors
    with real connectors (web3, exchange websocket, REST pollers, etc.)
    """
    # start feeders
    t1 = threading.Thread(target=sim_crypto_pool_feed, args=(out_q, cfg.num_sim_pools, cfg.pool_base_liq, cfg.pool_volatility), daemon=True)
    t2 = threading.Thread(target=sim_market_price_feed, args=(out_q, 6), daemon=True)
    t1.start()
    t2.start()
    # Keep running
    while True:
        time.sleep(1.0)

# -------------------------
# Anomaly Detectors
# -------------------------
def zscore_detector(source: str) -> Optional[Dict[str, Any]]:
    """
    Simple rolling z-score detector on 'price' column for the source.
    """
    df = event_store.tail_df(source, n=cfg.zscore_window)
    if df.empty or "price" not in df.columns:
        return None
    prices = df["price"].astype(float).values
    if len(prices) < 10:
        return None
    mean = prices.mean()
    std = prices.std(ddof=0) or 1e-9
    latest = prices[-1]
    z = (latest - mean) / std
    if abs(z) >= cfg.zscore_threshold:
        return {"signal": "zscore", "z": float(z), "price": float(latest), "mean": float(mean), "std": float(std)}
    return None

def change_point_detector(source: str) -> Optional[Dict[str, Any]]:
    """
    Very simple change-point detection using rolling mean differences and sensitivity.
    """
    df = event_store.tail_df(source, n=cfg.cp_window)
    if df.empty or "price" not in df.columns:
        return None
    prices = df["price"].astype(float).values
    if len(prices) < 20:
        return None
    half = len(prices) // 2
    mean1 = prices[:half].mean()
    mean2 = prices[half:].mean()
    diff = mean2 - mean1
    volatility = prices.std() or 1e-9
    score = abs(diff) / volatility
    if score >= cfg.cp_sensitivity:
        return {"signal": "change_point", "score": float(score), "mean_before": float(mean1), "mean_after": float(mean2)}
    return None

# -------------------------
# Optimization & Decision Engine
# -------------------------
def simple_knapsack_decision(opportunities: List[Dict[str, Any]], budget_usd: float) -> List[Dict[str, Any]]:
    """
    Greedy knapsack: sort by profit per USD and pick until budget used.
    Each opportunity must include: expected_profit_usd, cost_usd, market & id
    """
    if not opportunities:
        return []
    ops = sorted(opportunities, key=lambda x: (x["expected_profit_usd"]/max(1e-9, x["cost_usd"])), reverse=True)
    chosen = []
    remaining = budget_usd
    for o in ops:
        if o["cost_usd"] <= remaining and o["expected_profit_usd"] >= cfg.min_expected_profit_usd:
            chosen.append(o)
            remaining -= o["cost_usd"]
    return chosen

def multihop_pool_search(pool_list: List[Dict[str, Any]]) -> List[Dict[str,Any]]:
    """
    Build a graph of token prices from pools and search for profitable cycles or multihop trades.
    Here we create a directed graph with edges log-price so cycles with net negative sum mean arbitrage.
    We return simple 2-3 hop candidate trades with estimated profit (simulation).
    """
    g = nx.DiGraph()
    # Each pool -> create two directed edges between tokenA->tokenB and tokenB->tokenA with current price
    for p in pool_list:
        pair = p["pair"]
        a,b = pair.split("/")
        price_ab = p["price"]  # price b per a
        if price_ab <= 0:
            continue
        # weight as -log(rate) to find negative cycles
        g.add_edge(a, b, rate=price_ab, weight=-math.log(price_ab + 1e-12))
        if price_ab > 0:
            g.add_edge(b, a, rate=1.0/price_ab, weight=-math.log(1.0/(price_ab + 1e-12)))
    # Look for negative cycles using Bellman-Ford style: we attempt all simple 2-3 node cycles
    candidates = []
    nodes = list(g.nodes)
    for i in range(len(nodes)):
        u = nodes[i]
        for v in g.successors(u):
            for w in g.successors(v):
                if w == u:
                    # u -> v -> u cycle (2-hop)
                    weight = g[u][v]['weight'] + g[v][u]['weight']
                    # compute product of rates
                    r = g[u][v]['rate'] * g[v][u]['rate']
                    profit_ratio = r - 1.0
                    candidates.append({"cycle": [u, v, u], "profit_ratio": profit_ratio, "weight": weight})
                else:
                    if g.has_edge(w, u):
                        # u->v->w->u 3-hop
                        r = g[u][v]['rate'] * g[v][w]['rate'] * g[w][u]['rate']
                        profit_ratio = r - 1.0
                        candidates.append({"cycle": [u, v, w, u], "profit_ratio": profit_ratio})
    # filter by profit_ratio threshold
    good = [c for c in candidates if c.get("profit_ratio",0) > 0.001]
    # translate into fake opportunities
    results = []
    for c in good:
        est_profit = 1000.0 * c["profit_ratio"]  # pretend $1k capital
        results.append({"type":"pool_cycle", "cycle": c["cycle"], "expected_profit_usd": est_profit, "cost_usd": 1000.0})
    return results

# -------------------------
# Execution / Broker Stubs
# -------------------------
class ExecutionEngine:
    def __init__(self, mode: str = "dryrun"):
        self.mode = mode
        self.total_exposure = 0.0
        self.positions = {}  # source -> exposure
    def can_execute(self, market_id: str, cost_usd: float) -> bool:
        if self.total_exposure + cost_usd > cfg.max_position_usd:
            logger.warning("global exposure cap reached. cannot execute.")
            return False
        if self.positions.get(market_id, 0.0) + cost_usd > cfg.per_market_exposure:
            logger.warning(f"per-market exposure cap reached for {market_id}. cannot execute.")
            return False
        return True
    def execute(self, action: Dict[str,Any]) -> Dict[str,Any]:
        """
        action: dict with keys 'type', 'market', 'cost_usd', 'expected_profit_usd', ...
        Replace this method with real exchange/web3/marketplace calls.
        """
        market = action.get("market", "unknown")
        cost = float(action.get("cost_usd", 0.0))
        if not self.can_execute(market, cost):
            return {"status":"blocked", "reason":"risk_limits"}
        # simulate execution
        if self.mode in ("dryrun", "simulate"):
            # log and pretend a fill
            logger.info(f"[DRYRUN] Executing action on {market}: cost=${cost:.2f}, exp_profit=${action.get('expected_profit_usd',0):.2f}")
            # update exposure
            self.total_exposure += cost
            self.positions[market] = self.positions.get(market, 0.0) + cost
            # return fake trade id
            return {"status":"ok", "sim_trade_id": f"sim-{random.randint(1000,9999)}", "filled_size": cost}
        elif self.mode == "real":
            # TODO: plug in real connectors with ccxt / web3 / marketplace SDKs
            raise NotImplementedError("Real mode not configured. Plug in exchange/web3 clients here.")
        else:
            raise ValueError("unknown execution mode")

# -------------------------
# Strategy / Orchestration
# -------------------------
def scoring_and_trade_pipeline(exec_engine: ExecutionEngine):
    """
    Periodically examine recent events, detect anomalies, aggregate candidate opportunities,
    run optimizer and execute chosen ones.
    """
    # 1) build list of pool states
    pool_keys = [k for k in event_store.store.keys() if k.startswith("sim_pool:")]
    pool_list = []
    for pk in pool_keys:
        df = event_store.tail_df(pk, n=5)
        if df.empty:
            continue
        last = df.iloc[-1]
        pool_list.append({"pair": last["pair"], "price": float(last["price"]), "reserve_a": float(last.get("reserve_a",0)), "reserve_b": float(last.get("reserve_b",0))})
    # 2) find multihop opportunities
    pool_ops = multihop_pool_search(pool_list)
    # 3) check market price zscore signals
    market_keys = [k for k in event_store.store.keys() if k.startswith("sim_market:")]
    market_ops = []
    for mk in market_keys:
        z = zscore_detector(mk)
        cp = change_point_detector(mk)
        # if either triggers, estimate an expected profit and cost
        if z is not None:
            # map z magnitude to expected profit
            profit = min(1000.0, abs(z["z"]) * 30.0)  # simple heuristic
            cost = 200.0
            market_ops.append({"type":"market_z", "market": mk, "expected_profit_usd": profit*cfg.risk_multiplier, "cost_usd": cost, "signal": z})
        elif cp is not None:
            profit = min(1200.0, cp["score"] * 40.0)
            cost = 250.0
            market_ops.append({"type":"market_cp", "market": mk, "expected_profit_usd": profit*cfg.risk_multiplier, "cost_usd": cost, "signal": cp})
    # 4) combine all opportunities
    opportunities = []
    opportunities.extend(pool_ops)
    opportunities.extend(market_ops)
    if not opportunities:
        logger.debug("no opportunities right now.")
        return
    # 5) run knapsack over opportunities
    budget = min(cfg.max_position_usd - exec_engine.total_exposure, 5000.0)
    picks = simple_knapsack_decision(opportunities, budget)
    # 6) execute picks
    for p in picks:
        action = {"market": p.get("market", p.get("cycle","pool_cycle")), "cost_usd": p["cost_usd"], "expected_profit_usd": p["expected_profit_usd"], "meta": p}
        res = exec_engine.execute(action)
        logger.info(f"Executed pick result: {res}")

# -------------------------
# Event loop & runner
# -------------------------
def main_loop(mode: str = "simulate"):
    q = Queue()
    t = threading.Thread(target=ingestion_thread_main, args=(q,), daemon=True)
    t.start()
    exec_engine = ExecutionEngine(mode=("dryrun" if mode!="real" else "real"))
    logger.info("Starting main loop in mode=%s", mode)
    try:
        while True:
            # ingest available events
            try:
                while True:
                    ev = q.get_nowait()
                    event_store.push(ev)
                    # optional: immediate local detectors
                    if ev.type == "pool_update":
                        # push normalized price event too for unified detectors
                        # create a mirror event in unified source form
                        mirror = Event(source=f"{ev.source}", type="price_unified", timestamp=ev.timestamp, payload={"price": ev.payload["price"]})
                        event_store.push(mirror)
                    elif ev.type == "price_tick":
                        # already normalized
                        event_store.push(ev)
            except Empty:
                pass
            # run strategy engine
            scoring_and_trade_pipeline(exec_engine)
            time.sleep(cfg.loop_interval)
    except KeyboardInterrupt:
        logger.info("Shutting down main loop.")

# -------------------------
# CLI & Entrypoint
# -------------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["simulate", "dryrun", "real"], default="simulate", help="Run mode")
    args = parser.parse_args()
    cfg.mode = args.mode
    main_loop(mode=args.mode)
