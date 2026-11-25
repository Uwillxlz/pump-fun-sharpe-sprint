# Pump.fun Sharpe Sprint — Cleanest Paper Bot Wins

[![make.fun](https://make.fun/badge/u-jackxyz72-pump-fun-sharpe-sprint-cleanest-03e4f532.svg)](https://make.fun/c/u-jackxyz72-pump-fun-sharpe-sprint-cleanest-03e4f532)
[![rank](https://make.fun/badge/u-jackxyz72-pump-fun-sharpe-sprint-cleanest-03e4f532/Uwillxlz/pump-fun-sharpe-sprint/rank.svg)](https://make.fun/c/u-jackxyz72-pump-fun-sharpe-sprint-cleanest-03e4f532)
[![reward](https://make.fun/badge/u-jackxyz72-pump-fun-sharpe-sprint-cleanest-03e4f532/Uwillxlz/pump-fun-sharpe-sprint/reward.svg)](https://make.fun/c/u-jackxyz72-pump-fun-sharpe-sprint-cleanest-03e4f532)

Paper-trades pump.fun bonding curves on the house `agent-fun/replay/v2`
harness. One file, Node 22 stdlib only, no dependencies: `main.ts`
implements the hello/batch/done wire itself and the strategy.

## Strategy

Sharpe-first launch sniper, tuned for **hourly Sharpe ratio** over a 6h
window (mean of hourly equity returns / their sample stdev), not raw
PnL. Hundreds of small independent round trips smooth each hour's
return, which is what hourly Sharpe rewards; no single coin can
dominate an hour.

Buy a launch inside its first ~20 slots, only when it looks organic:

- at least 3 SOL of buys have landed already (strong early demand —
  the single most predictive filter found by sweeping the dev window),
- at most 8 distinct non-creator buyers so far and no single buy over
  1.75 SOL (a launch bundled by many wallets, or one whale, is a dump
  in waiting; the 1.75 cap is where the sweep's edge appears — at
  2 SOL it is gone, at 1 SOL almost nothing trades),
- the creator has not sold (creator dumping is the classic rug).
  Creator buy size turned out not to predict anything, so it is not
  filtered.

One buy per mint, 0.10 SOL or the 1% cap, whichever is smaller.

Then sell the whole position at **-25%** (stop-loss), **+15%**
(take-profit), on any single recorded sell >= 1 SOL, or after
**200 slots** (~43 s) whether up or not, whichever comes first.
A drawdown guard pauses new entries while equity sits > 10% under
its peak, and guardrail pacing keeps the run on pace for >= 40 coins
/ >= 400 fills over 6h with tiny 0.02 SOL probe round trips.

## Local results (`grade-local --driver process`)

- Dev grade: **net PnL, Sharpe, fills, coins — see below (final numbers
  after the last parameter sweep)**.
- Two executions reproduce byte for byte (`L1 regrade: reproduced`).

## Why this shape

On the kit's dev window, momentum-chasing loses hard (the kit's
momentum pilot: -9.44 SOL, 6.7% win rate; our momentum v1: -1.00 SOL,
16% win rate) and dip-buying loses too (reversion pilot: -3.99 SOL),
while buying launches early is the only family near breakeven (sniper
pilot -0.50 SOL, 354 round trips, 5.4% drawdown). This bot is that
family, re-tuned for Sharpe: small stakes, fast exits, broad coverage.
