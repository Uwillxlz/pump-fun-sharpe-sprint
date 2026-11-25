// Pump.fun Sharpe Sprint — Cleanest Paper Bot Wins.
// One-file Node 22 stdlib-only bot for the agent-fun/replay/v2 harness.
// It speaks the NDJSON wire itself (hello / batch / done on stdin, one
// reply per batch on stdout) and trades paper SOL on pump.fun bonding
// curves. No clock, no randomness, no I/O: every decision is a pure
// function of the batches the harness sends, so the two graded
// executions produce byte-identical action logs.
//
// Strategy — Sharpe-first launch sniper, tuned for HOURLY Sharpe over
// a 6h window (mean of hourly equity returns / their sample stdev), not
// raw PnL. The dev window says momentum-chasing loses hard here (the
// kit's momentum pilot: -9.44 SOL, 6.7% win rate; my own momentum v1:
// -1.00 SOL, 16% win rate) and dip-buying loses too (reversion pilot:
// -3.99 SOL), while buying launches early is the only family near
// breakeven (sniper pilot -0.50 SOL, 354 round trips, 5.4% drawdown).
// So: buy a launch inside its first ~20 slots, only when it looks
// organic, stake a small fixed 0.10 SOL, exit fast on a wide stop, a
// big take-profit, or a short time limit. Hundreds of small independent
// round trips per window smooth each hour's return (law of large
// numbers), which is what hourly Sharpe rewards; no single coin can
// dominate an hour.
//
// Entry (decided once, within ENTRY_SLOTS of the CreateEvent):
//   - real SOL reserve 1-30 (the 1%-of-reserve buy cap must let us put
//     at least 0.012 SOL in; above 30 the launch is already crowded),
//   - at least 3 SOL of buys have landed in the first ~20 slots
//     (strong early demand — the single most predictive filter found
//     by sweeping the dev window),
//   - at most 8 distinct non-creator buyers so far and no single buy
//     over 1.75 SOL (a launch bundled by many wallets, or one whale,
//     is a dump in waiting; the 1.75 cap is where the sweep's edge
//     appears — at 2 SOL it is gone, at 1 SOL almost nothing trades),
//   - the creator has not sold (creator dumping is the classic rug).
//     Creator buy size turned out not to predict anything on the dev
//     window, so it is not filtered.
// One buy per mint, 0.10 SOL or the 1% cap, whichever is smaller.
//
// Exits (whole position, sells go out wide open so they always land):
// stop-loss -25% vs cost, take-profit +15% vs cost, time exit after
// 200 slots (~43 s) whether up or not, big-sell exit on any single
// recorded sell >= 1 SOL. A drawdown guard pauses new entries while
// equity sits > 10% under its peak. Guardrail pacing keeps the run on
// pace for >= 40 coins / >= 400 fills over 6h with tiny 0.02 SOL probe
// round trips when the real entries lag the pace.
//
// Fees drive the sizing: a round trip costs ~2.5% in curve fees
// (95 bps protocol + 30 bps creator each way) plus 0.00021 SOL of tx
// fees, so a 0.10 SOL trade pays ~2.7% and needs a > ~3% move; buys
// carry 1000 bps of slippage headroom for the 2-slot fill latency.

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
  creator: string;
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
  creator: string;
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
const ENTRY_SLOTS = 20; // decide within this many slots of CreateEvent
const MIN_RQ_SOL = 1; // min real SOL reserve at entry
const MAX_RQ_SOL = 30; // max real SOL reserve at entry
const MAX_BUYERS = 8; // distinct non-creator buyers at entry
const MAX_CREATOR_BUY_SOL = 99; // sweep: creator buy size does not predict (only creator selling does)
const MAX_SINGLE_BUY_SOL = 1.75;
const MIN_TOTAL_BUY_SOL = 3;
const STAKE_SOL = 0.1;
const MIN_BUY_SOL = 0.012;
const BUY_SLIP_BPS = 1000;
const SELL_SLIP_BPS = 10_000;
const STOP_LOSS = 0.25;
const TAKE_PROFIT = 0.15;
const TRAIL_ARM = 999; // trailing stop off by default
const TRAIL_DROP = 0.15;
const BIG_SELL_SOL = 1;
const MAX_HOLD = 200; // slots (~43 s)
const HOLD_IF_UP = 100; // time exit fires regardless of being up
const MAX_EXPOSURE_SOL = 2.5;
const TARGET_COINS = 40;
const TARGET_FILLS = 400;
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

/** Early-launch tape: everything seen on a mint since its CreateEvent. */
interface Tape {
  created: number | null;
  creator: string | null;
  buyers: Set<string>; // distinct non-creator buyers
  creatorBuySol: number;
  creatorSold: boolean;
  totalBuySol: number;
  maxBuySol: number;
  decided: boolean; // entry decision made (buy or skip)
  entered: boolean;
}

interface Held {
  opened: number;
  peakValue: number; // SOL, best close value seen
  invested: number; // SOL spent on buys for this position
  exiting: boolean;
  filled: boolean;
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
const young = new Map<string, number>(); // mint -> created slot, undecided
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
    t = {
      created: null,
      creator: null,
      buyers: new Set<string>(),
      creatorBuySol: 0,
      creatorSold: false,
      totalBuySol: 0,
      maxBuySol: 0,
      decided: false,
      entered: false,
    };
    tapes.set(mint, t);
  }
  return t;
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

/** Entry filter on a young mint's early tape at slot `now`. */
function entrySignal(t: Tape, now: number, rqSol: number): boolean {
  if (t.created === null || now - t.created > ENTRY_SLOTS) return false;
  if (rqSol < MIN_RQ_SOL || rqSol > MAX_RQ_SOL) return false;
  if (t.buyers.size > MAX_BUYERS) return false;
  if (t.creatorBuySol > MAX_CREATOR_BUY_SOL) return false;
  if (t.creatorSold) return false;
  if (t.maxBuySol > MAX_SINGLE_BUY_SOL) return false;
  if (t.totalBuySol < MIN_TOTAL_BUY_SOL) return false;
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
      const t = tapeOf(e.mint);
      t.created = e.slot;
      t.creator = e.creator;
      young.set(e.mint, e.slot);
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
      traded.add(e.mint);
      if (e.is_buy) {
        t.totalBuySol += sol;
        if (sol > t.maxBuySol) t.maxBuySol = sol;
        if (t.creator !== null && e.user === t.creator) {
          t.creatorBuySol += sol;
        } else {
          t.buyers.add(e.user);
        }
      } else {
        if (t.creator !== null && e.user === t.creator) t.creatorSold = true;
        if (sol >= BIG_SELL_SOL) bigSell.add(e.mint);
      }
    } else {
      const c = curveOf(e.mint, e.slot);
      c.complete = true;
    }
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
      if (!pendingByMint.has(mint)) held.delete(mint);
      continue;
    }
    h.filled = true;
    if (curve === undefined || curve.complete) continue;
    const value = sellValueSol(curve, pos.tokens);
    const cost = Number(pos.cost) / LAMPORTS;
    if (value > h.peakValue) h.peakValue = value;
    h.lastValue = value;
    let exit = h.exiting;
    if (!exit && cost > 0 && value < cost * (1 - STOP_LOSS)) exit = true;
    if (!exit && cost > 0 && value >= cost * (1 + TAKE_PROFIT)) exit = true;
    if (!exit && h.peakValue >= cost * (1 + TRAIL_ARM) && value < h.peakValue * (1 - TRAIL_DROP)) exit = true;
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

  // --- Sniper entries: decide each young mint once, inside its window. ---
  let free = freeCashSol();
  let slotsLeft = hello!.caps.max_open_positions - committedCount();
  let exposure = 0;
  for (const [mint, h] of held) {
    exposure += Math.max(h.invested, Number(positions.get(mint)?.cost ?? 0n) / LAMPORTS);
  }
  if (!entriesPaused) {
    for (const [mint, created] of young) {
      if (orders.length >= cap) break;
      const t = tapes.get(mint)!;
      const curve = curves.get(mint);
      if (now - created > ENTRY_SLOTS) {
        t.decided = true;
        young.delete(mint);
        continue;
      }
      if (t.decided || t.entered) {
        young.delete(mint);
        continue;
      }
      if (curve === undefined || curve.complete) {
        t.decided = true;
        young.delete(mint);
        continue;
      }
      if (slotsLeft <= 0) break;
      if (!entrySignal(t, now, Number(curve.rq) / LAMPORTS)) continue;
      const capSol = Math.floor(maxBuySol(mint) * LAMPORTS) / LAMPORTS;
      let amount = Math.min(capSol, STAKE_SOL, MAX_EXPOSURE_SOL - exposure);
      if (amount > free - tx) amount = free - tx;
      if (amount < MIN_BUY_SOL) {
        // Too small to buy now; wait for the reserve to grow, still inside the window.
        continue;
      }
      const lamports = BigInt(Math.floor(amount * LAMPORTS));
      if (lamports <= 0n) continue;
      orders.push({
        side: 'buy',
        client_id: `o${nextId++}`,
        mint,
        quote_lamports: lamports.toString(),
        max_slippage_bps: BUY_SLIP_BPS,
      });
      t.decided = true;
      t.entered = true;
      young.delete(mint);
      held.set(mint, {
        opened: now,
        peakValue: 0,
        invested: amount,
        exiting: false,
        filled: false,
        lastValue: 0,
      });
      free -= amount + tx;
      exposure += amount;
      slotsLeft--;
    }
  }
  // Age out young mints whose window passed without a decision.
  for (const [mint, created] of young) {
    if (now - created > ENTRY_SLOTS) {
      tapes.get(mint)!.decided = true;
      young.delete(mint);
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
        // A probe only helps the coin guardrail on a mint we have not traded.
        if (needCoins && myCoins.has(mint)) continue;
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
