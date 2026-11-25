// Pump.fun Sharpe Sprint — Cleanest Paper Bot Wins.
// One-file Node 22 stdlib-only bot for the agent-fun/replay/v2 harness.
// It speaks the NDJSON wire itself (hello / batch / done on stdin, one
// reply per batch on stdout) and trades paper SOL on pump.fun bonding
// curves. No clock, no randomness, no I/O: every decision is a pure
// function of the batches the harness sends, seeded only by hello.seed,
// so the two graded executions produce byte-identical action logs.
//
// Strategy — Sharpe-first momentum, tuned for HOURLY Sharpe over a 6h
// window (mean of hourly equity returns / their sample stdev), not raw
// PnL. That means: many small independent round trips per hour (each
// hour's return is then an average of many trades, so cross-hour
// variance stays low), tight exits, no concentration, and breadth
// (>= 30 distinct mints, each round trip >= 0.01 SOL).
//
// Entry: a mature curve (age >= 400 slots since CreateEvent, real SOL
// 3-100) whose last ~40 slots show accelerating buy pressure: net SOL
// inflow >= max(0.05 SOL, 1.5% of real reserve), >= 4 buys from >= 5
// distinct buyers, price up 4-45% in the window and within 10% of the
// window high, newer-half inflow >= older-half inflow, no single buyer
// over 60% of the window's buy SOL. First buy 0.20 SOL, pyramid adds of
// 0.15 SOL into strength (price >= last buy x1.03, position up) up to
// 0.50 SOL per mint; at most 5 open positions, at most 2.5 SOL at cost.
//
// Exits (whole position, sells go out wide open so they always land):
// stop-loss at -4% vs cost, take-profit at +6% vs cost, trailing stop
// -8% from peak once the peak is +4% over cost, fade exit when net flow
// over the last 20 slots turns < -0.3 SOL, big-sell exit on any single
// recorded sell >= 1.5 SOL, and a 1200-slot (~4 min) time exit unless
// the position is up >= 2%. A drawdown guard pauses new entries while
// equity sits > 10% under its peak. Guardrail pacing keeps the run on
// pace for >= 30 coins / >= 360 fills over 6h with tiny 0.02 SOL probe
// round trips when the real entries lag the pace.
//
// Fees drive the sizing: a round trip costs ~2.5% in curve fees
// (95 bps protocol + 30 bps creator each way) plus 0.00021 SOL of tx
// fees, so a 0.20 SOL trade pays ~2.6% and needs a > ~3% move; the
// 1%-of-real-reserve per-reply buy cap sizes every order, and buys
// carry 800 bps of slippage headroom for the 2-slot fill latency.

import * as readline from 'node:readline';

// ---------------------------------------------------------------- types

type U64 = string;

interface HelloMsg {
  type: 'hello';
  protocol: string;
  seed: U64;
  universe: unknown;
  capital: { lamports: U64 };
  caps: {
    max_open_positions: number;
    max_order_bps_of_real_reserve: number;
    max_orders_per_batch: number;
    shorts: boolean;
    leverage: boolean;
  };
  latency_slots: number;
  fees: {
    protocol_bps_fallback: number;
    creator_bps_fallback: number;
    base_fee_lamports: U64;
    priority_fee_lamports: U64;
    failed_tx_bps: number;
  };
}

interface TradeEvent {
  event: 'TradeEvent';
  mint: string;
  sol_amount: U64;
  token_amount: U64;
  is_buy: boolean;
  user: string;
  timestamp: string;
  virtual_sol_reserves: U64;
  virtual_token_reserves: U64;
  real_sol_reserves: U64;
  real_token_reserves: U64;
  virtual_quote_reserves: U64;
  real_quote_reserves: U64;
  slot: number;
  block_time: number | null;
  [k: string]: unknown;
}

interface CreateEvent {
  event: 'CreateEvent';
  mint: string;
  slot: number;
  block_time: number | null;
  virtual_token_reserves: U64;
  virtual_quote_reserves: U64;
  real_token_reserves: U64;
  [k: string]: unknown;
}

interface CompleteEvent {
  event: 'CompleteEvent';
  mint: string;
  slot: number;
  block_time: number | null;
  [k: string]: unknown;
}

type ChainEvent = TradeEvent | CreateEvent | CompleteEvent;

interface Mark {
  mint: string;
  slot: number;
  virtual_token_reserves: U64;
  virtual_quote_reserves: U64;
  real_token_reserves: U64;
  real_quote_reserves: U64;
}

interface Fill {
  client_id: string;
  mint: string;
  side: 'buy' | 'sell';
  status: 'filled' | 'rejected' | 'failed';
  reason: string | null;
  decided_slot: number;
  slot: number;
  quote_lamports: U64;
  token_amount: U64;
  fee_lamports: U64;
  tx_fee_lamports: U64;
  fee_fallback: boolean;
}

interface Position {
  mint: string;
  token_amount: U64;
  cost_lamports: U64;
}

interface BatchMsg {
  type: 'batch';
  seq: number;
  slot: number;
  events: ChainEvent[];
  marks: Mark[];
  fills: Fill[];
  account: { cash_lamports: U64; positions: Position[] };
}

interface DoneMsg {
  type: 'done';
  seq: number;
  reason: string;
  account: { cash_lamports: U64; positions: Position[] };
}

interface BuyOrder {
  side: 'buy';
  client_id: string;
  mint: string;
  quote_lamports: string;
  max_slippage_bps: number;
}

interface SellOrder {
  side: 'sell';
  client_id: string;
  mint: string;
  token_amount: string;
  max_slippage_bps: number;
}

type Order = BuyOrder | SellOrder;

// ---------------------------------------------------------------- params

const LAMPORTS = 1_000_000_000;
const CURVE_FEE = 0.0125; // 95 bps protocol + 30 bps creator, fallback
const WINDOW = 40; // entry signal window, slots (~8.6 s)
const FADE_WINDOW = 20; // fade-exit flow window, slots
const MIN_AGE = 400; // slots since CreateEvent before an entry (~86 s)
const MIN_RQ_SOL = 3; // min real SOL reserve at entry
const MAX_RQ_SOL = 100; // max real SOL reserve at entry
const MIN_BUYS = 4;
const MIN_BUYERS = 5;
const MIN_UP = 0.04;
const MAX_UP = 0.45;
const NEAR_HIGH = 0.9;
const MAX_BUYER_SHARE = 0.6;
const MIN_NET_SOL = 0.05;
const MIN_NET_FRAC_RQ = 0.015; // net inflow >= 1.5% of real reserve
const INITIAL_SOL = 0.2;
const ADD_SOL = 0.15;
const MAX_POS_SOL = 0.5;
const MAX_EXPOSURE_SOL = 2.5;
const MIN_BUY_SOL = 0.02;
const BUY_SLIP_BPS = 800;
const SELL_SLIP_BPS = 10_000;
const STOP_LOSS = 0.04;
const TAKE_PROFIT = 0.06;
const TRAIL_ARM = 0.04;
const TRAIL_DROP = 0.08;
const FADE_NET_SOL = -0.3;
const BIG_SELL_SOL = 1.5;
const MAX_HOLD = 1200; // slots (~4.3 min)
const HOLD_IF_UP = 0.02;
const COOLDOWN = 150; // slots after an exit before re-entry
const MAX_ENTRIES = 2; // per mint over the run
const ADD_STEP = 0.03; // add only when price >= last buy x 1.03
const ADD_IF_UP = 0.02; // add only while position value >= cost x 1.02
const TARGET_COINS = 30;
const TARGET_FILLS = 360;
const PACE_SECS = 21_600; // 6h final window
const PROBE_SOL = 0.02;
const MAX_PROBES = 1;
const DD_PAUSE = 0.1; // pause entries while equity > 10% under peak
const DD_RESUME = 0.05;

// ---------------------------------------------------------------- state

interface Curve {
  mint: string;
  created: number | null;
  vq: bigint; // virtual quote reserves
  vt: bigint; // virtual token reserves
  rq: bigint; // real quote (SOL) reserves
  rt: bigint; // real token reserves
  reservesSlot: number;
  complete: boolean;
}

interface Trade {
  slot: number;
  buy: boolean;
  sol: number;
  user: string;
  price: number; // vq / vt at the trade
}

interface Tape {
  created: number | null;
  trades: Trade[];
  entries: number;
  lastExit: number;
}

interface Held {
  opened: number;
  peakValue: number; // SOL, best close value seen
  invested: number; // SOL spent on buys for this position
  exiting: boolean;
  filled: boolean;
  lastAddPrice: number;
  lastValue: number;
}

interface Pending {
  mint: string;
  side: 'buy' | 'sell';
  quoteLamports: bigint;
  tokenAmount: bigint;
}

const curves = new Map<string, Curve>();
const tapes = new Map<string, Tape>();
const held = new Map<string, Held>();
const probes = new Map<string, boolean>(); // mint -> selling?
const pending = new Map<string, Pending>();
const positions = new Map<string, { tokens: bigint; cost: bigint }>();
let cash = 0n;
let hello: HelloMsg | null = null;
let nextId = 1;
let myFills = 0;
const myCoins = new Set<string>();
let firstTime: number | null = null;
let lastTime: number | null = null;
let peakEquitySol = 10; // starts at capital; refined on first batch
let batches = 0;

function txFeeSol(): number {
  if (!hello) return 0.000105;
  return (Number(hello.fees.base_fee_lamports) + Number(hello.fees.priority_fee_lamports)) / LAMPORTS;
}

function curveOf(mint: string, slot: number): Curve {
  let c = curves.get(mint);
  if (c === undefined) {
    c = {
      mint,
      created: null,
      vq: 0n,
      vt: 0n,
      rq: 0n,
      rt: 0n,
      reservesSlot: slot,
      complete: false,
    };
    curves.set(mint, c);
  }
  return c;
}

function tapeOf(mint: string): Tape {
  let t = tapes.get(mint);
  if (t === undefined) {
    t = { created: null, trades: [], entries: 0, lastExit: -1e12 };
    tapes.set(mint, t);
  }
  return t;
}

/** Price of one token in SOL from virtual reserves. */
function priceOf(c: Curve): number {
  if (c.vt === 0n || c.vq === 0n) return 0;
  return Number(c.vq) / Number(c.vt);
}

/** SOL a sale of `tokens` would net: constant-product quote less curve fee less one tx fee. */
function sellValueSol(c: Curve, tokens: bigint): number {
  if (tokens <= 0n || c.vt === 0n || c.vq === 0n) return 0;
  const quote = (Number(tokens) * Number(c.vq)) / (Number(c.vt) + Number(tokens));
  return (quote / LAMPORTS) * (1 - CURVE_FEE) - txFeeSol();
}

/** Equity in SOL: cash plus marked sell value of every position. */
function equitySol(): number {
  let eq = Number(cash) / LAMPORTS;
  for (const [mint, p] of positions) {
    const c = curves.get(mint);
    if (c !== undefined && !c.complete) eq += Math.max(0, sellValueSol(c, p.tokens));
  }
  return eq;
}

function maxBuySol(mint: string): number {
  const c = curves.get(mint);
  if (c === undefined || c.complete || !hello) return 0;
  return (Number(c.rq) * hello.caps.max_order_bps_of_real_reserve) / 10_000 / LAMPORTS;
}

function committedCount(): number {
  const mints = new Set<string>(positions.keys());
  for (const p of pending.values()) if (p.side === 'buy') mints.add(p.mint);
  return mints.size;
}

function freeCashSol(): number {
  let free = Number(cash) / LAMPORTS;
  const tx = txFeeSol();
  for (const p of pending.values()) {
    free -= tx;
    if (p.side === 'buy') free -= Number(p.quoteLamports) / LAMPORTS;
  }
  return free;
}

function pendingSellTokens(mint: string): bigint {
  let n = 0n;
  for (const p of pending.values()) if (p.mint === mint && p.side === 'sell') n += p.tokenAmount;
  return n;
}

function hasPending(mint: string): boolean {
  for (const p of pending.values()) if (p.mint === mint) return true;
  return false;
}

/** Net recorded SOL (buys minus sells) over the last `w` slots. */
function flow(t: Tape, now: number, w: number): number {
  let net = 0;
  for (let i = t.trades.length - 1; i >= 0; i--) {
    const tr = t.trades[i]!;
    if (tr.slot <= now - w) break;
    net += tr.buy ? tr.sol : -tr.sol;
  }
  return net;
}

function entrySignal(t: Tape, now: number, rqSol: number): boolean {
  if (rqSol < MIN_RQ_SOL || rqSol > MAX_RQ_SOL) return false;
  if (t.created === null || now - t.created < MIN_AGE) return false;
  const from = now - WINDOW;
  const mid = now - WINDOW / 2;
  let buySol = 0;
  let sellSol = 0;
  let buys = 0;
  let netOld = 0;
  let netNew = 0;
  let startPrice = -1;
  let high = 0;
  let last = 0;
  const byUser = new Map<string, number>();
  for (const tr of t.trades) {
    if (tr.slot <= from) {
      startPrice = tr.price;
      continue;
    }
    if (startPrice < 0) startPrice = tr.price;
    const signed = tr.buy ? tr.sol : -tr.sol;
    if (tr.slot <= mid) netOld += signed;
    else netNew += signed;
    if (tr.buy) {
      buySol += tr.sol;
      buys++;
      byUser.set(tr.user, (byUser.get(tr.user) ?? 0) + tr.sol);
    } else {
      sellSol += tr.sol;
    }
    if (tr.price > high) high = tr.price;
    last = tr.price;
  }
  if (buys < MIN_BUYS || byUser.size < MIN_BUYERS) return false;
  const net = buySol - sellSol;
  if (net < Math.max(MIN_NET_SOL, MIN_NET_FRAC_RQ * rqSol)) return false;
  if (startPrice <= 0) return false;
  const up = last / startPrice - 1;
  if (up < MIN_UP || up > MAX_UP) return false;
  if (last < high * NEAR_HIGH) return false;
  if (netNew <= 0 || netNew < netOld) return false;
  let top = 0;
  for (const v of byUser.values()) if (v > top) top = v;
  if (top > MAX_BUYER_SHARE * buySol) return false;
  return true;
}

// ---------------------------------------------------------------- per batch

function onBatch(batch: BatchMsg): Order[] {
  const now = batch.slot;
  const orders: Order[] = [];
  const cap = hello!.caps.max_orders_per_batch;
  const tx = txFeeSol();
  const bigSell = new Set<string>();
  const traded = new Set<string>();

  for (const f of batch.fills) {
    pending.delete(f.client_id);
    if (f.status === 'filled' && !f.client_id.startsWith('_')) {
      myFills++;
      myCoins.add(f.mint);
    }
  }
  positions.clear();
  cash = BigInt(batch.account.cash_lamports);
  for (const p of batch.account.positions) {
    const tokens = BigInt(p.token_amount);
    if (tokens > 0n) positions.set(p.mint, { tokens, cost: BigInt(p.cost_lamports) });
  }
  for (const m of batch.marks) {
    const c = curves.get(m.mint);
    if (c !== undefined && m.slot < c.reservesSlot) continue;
    const curve = c ?? curveOf(m.mint, m.slot);
    curve.vq = BigInt(m.virtual_quote_reserves);
    curve.vt = BigInt(m.virtual_token_reserves);
    curve.rq = BigInt(m.real_quote_reserves);
    curve.rt = BigInt(m.real_token_reserves);
    curve.reservesSlot = m.slot;
  }
  for (const e of batch.events) {
    if (e.block_time !== null) {
      if (firstTime === null) firstTime = e.block_time;
      if (lastTime === null || e.block_time > lastTime) lastTime = e.block_time;
    }
    if (e.event === 'CreateEvent') {
      tapeOf(e.mint).created = e.slot;
      const c = curveOf(e.mint, e.slot);
      c.created = e.slot;
      c.vt = BigInt(e.virtual_token_reserves);
      c.vq = BigInt(e.virtual_quote_reserves);
      c.rt = BigInt(e.real_token_reserves);
      c.rq = 0n;
      c.reservesSlot = e.slot;
    } else if (e.event === 'TradeEvent') {
      const t = tapeOf(e.mint);
      const sol = Number(e.sol_amount) / LAMPORTS;
      const c = curveOf(e.mint, e.slot);
      c.vq = BigInt(e.virtual_quote_reserves);
      c.vt = BigInt(e.virtual_token_reserves);
      c.rq = BigInt(e.real_quote_reserves);
      c.rt = BigInt(e.real_token_reserves);
      c.reservesSlot = e.slot;
      t.trades.push({
        slot: e.slot,
        buy: e.is_buy,
        sol,
        user: e.user,
        price: Number(e.virtual_quote_reserves) / Number(e.virtual_token_reserves),
      });
      traded.add(e.mint);
      if (!e.is_buy && sol >= BIG_SELL_SOL) bigSell.add(e.mint);
    } else {
      const c = curveOf(e.mint, e.slot);
      c.complete = true;
    }
  }
  // Prune tapes to the lookback we use, plus a margin.
  const keep = Math.max(WINDOW, FADE_WINDOW) + 5;
  for (const mint of traded) {
    const t = tapes.get(mint)!;
    let cut = 0;
    while (cut < t.trades.length && t.trades[cut]!.slot <= now - keep) cut++;
    if (cut > 0) t.trades.splice(0, cut);
  }

  const pendingByMint = new Set<string>();
  for (const p of pending.values()) pendingByMint.add(p.mint);

  // Drawdown guard: pause new entries while equity is far under its peak.
  const eq = equitySol();
  if (eq > peakEquitySol) peakEquitySol = eq;
  const dd = peakEquitySol > 0 ? (peakEquitySol - eq) / peakEquitySol : 0;
  const entriesPaused = dd > DD_PAUSE;

  // --- Exits first. ---
  for (const [mint, h] of held) {
    const pos = positions.get(mint);
    const curve = curves.get(mint);
    if (pos === undefined) {
      if (!pendingByMint.has(mint)) {
        held.delete(mint);
        const t = tapeOf(mint);
        if (h.filled) t.lastExit = now;
        else t.entries--;
      }
      continue;
    }
    h.filled = true;
    if (curve === undefined || curve.complete) continue;
    const value = sellValueSol(curve, pos.tokens);
    const cost = Number(pos.cost) / LAMPORTS;
    if (value > h.peakValue) h.peakValue = value;
    h.lastValue = value;
    let exit = h.exiting;
    const t = tapeOf(mint);
    if (!exit && cost > 0 && value < cost * (1 - STOP_LOSS)) exit = true;
    if (!exit && cost > 0 && value >= cost * (1 + TAKE_PROFIT)) exit = true;
    if (!exit && h.peakValue >= cost * (1 + TRAIL_ARM) && value < h.peakValue * (1 - TRAIL_DROP)) exit = true;
    if (!exit && flow(t, now, FADE_WINDOW) < FADE_NET_SOL) exit = true;
    if (!exit && bigSell.has(mint)) exit = true;
    if (!exit && now - h.opened > MAX_HOLD && value < cost * (1 + HOLD_IF_UP)) exit = true;
    if (!exit) continue;
    h.exiting = true;
    let sellPending = false;
    for (const p of pending.values()) if (p.mint === mint && p.side === 'sell') sellPending = true;
    if (sellPending) continue;
    if (orders.length >= cap) break;
    const avail = pos.tokens - pendingSellTokens(mint);
    if (avail <= 0n) continue;
    orders.push({
      side: 'sell',
      client_id: `o${nextId++}`,
      mint,
      token_amount: avail.toString(),
      max_slippage_bps: SELL_SLIP_BPS,
    });
  }

  // --- Probe round trips: sell the whole probe as soon as its buy lands. ---
  for (const [mint, selling] of probes) {
    const pos = positions.get(mint);
    const curve = curves.get(mint);
    if (pos === undefined) {
      if (!pendingByMint.has(mint)) probes.delete(mint);
      continue;
    }
    if (curve === undefined || curve.complete || pendingByMint.has(mint)) continue;
    if (orders.length >= cap) break;
    orders.push({
      side: 'sell',
      client_id: `o${nextId++}`,
      mint,
      token_amount: pos.token_amount,
      max_slippage_bps: SELL_SLIP_BPS,
    });
    if (!selling) probes.set(mint, true);
  }

  // --- Entries and pyramid adds. ---
  if (!entriesPaused) {
    let free = freeCashSol();
    let slotsLeft = hello!.caps.max_open_positions - committedCount();
    let mainLeft = hello!.caps.max_open_positions - held.size;
    let exposure = 0;
    for (const [mint, h] of held) {
      exposure += Math.max(h.invested, Number(positions.get(mint)?.cost ?? 0n) / LAMPORTS);
    }
    for (const mint of traded) {
      if (orders.length >= cap) break;
      const curve = curves.get(mint);
      if (curve === undefined || curve.complete) continue;
      if (pendingByMint.has(mint) || probes.has(mint)) continue;
      const t = tapes.get(mint)!;
      const h = held.get(mint);
      const price = priceOf(curve);
      if (price <= 0) continue;
      const rqSol = Number(curve.rq) / LAMPORTS;
      let want: number;
      if (h === undefined) {
        if (slotsLeft <= 0 || mainLeft <= 0) continue;
        if (t.entries >= MAX_ENTRIES || now - t.lastExit < COOLDOWN) continue;
        if (!entrySignal(t, now, rqSol)) continue;
        want = INITIAL_SOL;
      } else {
        if (h.exiting || !h.filled || h.invested >= MAX_POS_SOL - MIN_BUY_SOL) continue;
        const pos = positions.get(mint);
        if (pos === undefined) continue;
        if (price < h.lastAddPrice * (1 + ADD_STEP)) continue;
        if (h.lastValue < (Number(pos.cost) / LAMPORTS) * (1 + ADD_IF_UP)) continue;
        if (flow(t, now, WINDOW) < Math.max(MIN_NET_SOL, MIN_NET_FRAC_RQ * rqSol)) continue;
        if (rqSol > MAX_RQ_SOL) continue;
        want = Math.min(ADD_SOL, MAX_POS_SOL - h.invested);
      }
      const capSol = Math.floor(maxBuySol(mint) * LAMPORTS) / LAMPORTS;
      let amount = Math.min(capSol, want, MAX_EXPOSURE_SOL - exposure);
      if (amount > free - tx) amount = free - tx;
      if (amount < MIN_BUY_SOL) continue;
      const lamports = BigInt(Math.floor(amount * LAMPORTS));
      if (lamports <= 0n) continue;
      orders.push({
        side: 'buy',
        client_id: `o${nextId++}`,
        mint,
        quote_lamports: lamports.toString(),
        max_slippage_bps: BUY_SLIP_BPS,
      });
      free -= amount + tx;
      exposure += amount;
      if (h === undefined) {
        held.set(mint, {
          opened: now,
          peakValue: 0,
          invested: amount,
          exiting: false,
          filled: false,
          lastAddPrice: price,
          lastValue: 0,
        });
        t.entries++;
        slotsLeft--;
        mainLeft--;
      } else {
        h.invested += amount;
        h.lastAddPrice = price;
      }
    }

    // --- Guardrail pacing: open a probe when fills or coins lag the pace. ---
    if (probes.size < MAX_PROBES && slotsLeft > 0 && firstTime !== null && lastTime !== null) {
      const pace = Math.min(1, (lastTime - firstTime) / PACE_SECS);
      const needCoins = myCoins.size < Math.ceil(TARGET_COINS * pace);
      const needFills = myFills < Math.ceil(TARGET_FILLS * pace);
      if (needCoins || needFills) {
        const minRq = (PROBE_SOL * 10_000) / hello!.caps.max_order_bps_of_real_reserve;
        for (const mint of traded) {
          if (orders.length >= cap || probes.size >= MAX_PROBES) break;
          if (held.has(mint) || probes.has(mint) || pendingByMint.has(mint) || positions.has(mint)) continue;
          if (needCoins && !needFills && myCoins.has(mint)) continue;
          const curve = curves.get(mint);
          if (curve === undefined || curve.complete) continue;
          if (Number(curve.rq) / LAMPORTS < minRq * 1.5) continue;
          if (free < PROBE_SOL + 2 * tx) break;
          orders.push({
            side: 'buy',
            client_id: `o${nextId++}`,
            mint,
            quote_lamports: BigInt(Math.floor(PROBE_SOL * LAMPORTS)).toString(),
            max_slippage_bps: BUY_SLIP_BPS,
          });
          probes.set(mint, false);
          free -= PROBE_SOL + tx;
        }
      }
    }
  }

  // (Pending is recorded by the caller after filtering, so only orders
  // actually sent are tracked.)
  return orders;
}

// ---------------------------------------------------------------- wire loop

const PUBKEY_RE = /^[1-9A-HJ-NP-Za-km-z]{32,44}$/;

function validOrder(o: Order): boolean {
  if (!PUBKEY_RE.test(o.mint)) return false;
  if (o.side === 'buy') {
    if (!/^(0|[1-9][0-9]{0,19})$/.test(o.quote_lamports)) return false;
  } else if (!/^(0|[1-9][0-9]{0,19})$/.test(o.token_amount)) return false;
  return o.max_slippage_bps >= 0 && o.max_slippage_bps <= 10_000;
}

async function main(): Promise<void> {
  const rl = readline.createInterface({ input: process.stdin, crlfDelay: Infinity });
  for await (const line of rl) {
    if (line.trim() === '') continue;
    let msg: unknown;
    try {
      msg = JSON.parse(line);
    } catch {
      console.error('bad JSON line, exiting');
      process.exit(1);
    }
    const type = (msg as { type?: unknown }).type;
    if (type === 'hello') {
      hello = msg as HelloMsg;
      if (hello.protocol !== 'agent-fun/replay/v2') {
        console.error(`unsupported protocol ${hello.protocol}`);
        process.exit(1);
      }
      cash = BigInt(hello.capital.lamports);
      peakEquitySol = Number(hello.capital.lamports) / LAMPORTS;
    } else if (type === 'batch') {
      const batch = msg as BatchMsg;
      if (hello === null) {
        console.error('batch before hello');
        process.exit(1);
      }
      batches++;
      let orders: Order[] = [];
      try {
        orders = onBatch(batch);
      } catch (err) {
        console.error(`batch ${batch.seq}: strategy error, sending no orders: ${err instanceof Error ? err.message : String(err)}`);
        orders = [];
      }
      // A protocol violation fails the run; drop anything malformed instead.
      orders = orders.filter(validOrder).slice(0, hello.caps.max_orders_per_batch);
      for (const o of orders) {
        pending.set(o.client_id, {
          mint: o.mint,
          side: o.side,
          quoteLamports: o.side === 'buy' ? BigInt(o.quote_lamports) : 0n,
          tokenAmount: o.side === 'sell' ? BigInt(o.token_amount) : 0n,
        });
      }
      process.stdout.write(`${JSON.stringify({ type: 'reply', seq: batch.seq, orders })}\n`);
    } else if (type === 'done') {
      const done = msg as DoneMsg;
      console.error(
        `done after ${batches} batches: cash ${done.account.cash_lamports} lamports, ` +
          `${done.account.positions.length} positions, fills ${myFills}, coins ${myCoins.size}`,
      );
      return;
    } else {
      console.error(`unknown message type ${String(type)}`);
      process.exit(1);
    }
  }
  console.error('input ended before done');
  process.exit(1);
}

main().catch((err) => {
  console.error(err instanceof Error ? err.stack ?? err.message : String(err));
  process.exit(1);
});
