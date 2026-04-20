"""
Capital Management — Tier System
T0: paper trading only
T1: $2 live, <$0.30/bet
T2: $10 live, unlocks after 30 resolved bets + Brier < 0.20 + win_rate > 52%
T3: $50 live, unlocks after T2 criteria × 2 (60+ bets, Brier < 0.18, wr > 55%)
T4: $200 live
T5: $1000+ live
"""

import json, os
from polymarket_core import CALIBRATION_STATS_JSON, STARTING_BANKROLL_JSON

# ─── TIER DEFINITIONS ─────────────────────────────────────────────────────────

TIERS = {
    0: {"name": "T0_PAPER",  "max_bankroll": 0,    "max_bet": 0,     "max_daily_pct": 0.0,  "live": False},
    1: {"name": "T1_SEED",   "max_bankroll": 2,    "max_bet": 0.30,  "max_daily_pct": 0.30, "live": True},
    2: {"name": "T2_TEST",   "max_bankroll": 10,   "max_bet": 1.50,  "max_daily_pct": 0.25, "live": True},
    3: {"name": "T3_SCALE",  "max_bankroll": 50,   "max_bet": 5.00,  "max_daily_pct": 0.20, "live": True},
    4: {"name": "T4_GROW",   "max_bankroll": 200,  "max_bet": 15.00, "max_daily_pct": 0.15, "live": True},
    5: {"name": "T5_FULL",   "max_bankroll": 1000, "max_bet": 50.00, "max_daily_pct": 0.12, "live": True},
}

UNLOCK_CRITERIA = {
    2: {"min_resolved": 30,  "max_brier": 0.20, "min_win_rate": 0.52, "min_roi": 0.05},
    3: {"min_resolved": 60,  "max_brier": 0.18, "min_win_rate": 0.55, "min_roi": 0.10},
    4: {"min_resolved": 150, "max_brier": 0.16, "min_win_rate": 0.57, "min_roi": 0.15},
    5: {"min_resolved": 300, "max_brier": 0.15, "min_win_rate": 0.60, "min_roi": 0.20},
}

_STATS_FILE    = CALIBRATION_STATS_JSON
_BANKROLL_FILE = STARTING_BANKROLL_JSON

# ─── STATS LOADING ────────────────────────────────────────────────────────────

def load_stats(stats_file=None):
    """Load calibration stats. Returns empty defaults if file missing."""
    path = stats_file or _STATS_FILE
    defaults = {
        "resolved_bets": 0,
        "brier_score":   1.0,
        "win_rate":      0.0,
        "roi":           0.0,
    }
    if not path or not os.path.exists(path):
        return defaults
    try:
        with open(path) as f:
            data = json.load(f)
        # Fill in any missing keys with defaults
        for k, v in defaults.items():
            data.setdefault(k, v)
        return data
    except Exception:
        return defaults

# ─── TIER UNLOCK CHECK ────────────────────────────────────────────────────────

def check_tier_unlock(current_tier, stats):
    """
    Check if criteria are met to advance to current_tier + 1.

    Returns (new_tier, reason_str).
    If criteria met   → (current_tier + 1, "unlocked: ...")
    If criteria unmet → (current_tier,     "locked: ...")
    If already at T5  → (5, "already at maximum tier T5")
    """
    next_tier = current_tier + 1
    if next_tier not in UNLOCK_CRITERIA:
        return (current_tier, f"already at maximum tier T{current_tier}")

    crit     = UNLOCK_CRITERIA[next_tier]
    resolved = stats.get("resolved_bets", 0)
    brier    = stats.get("brier_score",   1.0)
    win_rate = stats.get("win_rate",      0.0)
    roi      = stats.get("roi",           0.0)

    gaps = []
    if resolved < crit["min_resolved"]:
        gaps.append(f"{crit['min_resolved'] - resolved} more resolved bets "
                    f"({resolved}/{crit['min_resolved']})")
    if brier > crit["max_brier"]:
        gaps.append(f"Brier too high: {brier:.3f} (need <{crit['max_brier']})")
    if win_rate < crit["min_win_rate"]:
        gaps.append(f"win_rate too low: {win_rate:.1%} (need >{crit['min_win_rate']:.0%})")
    if roi < crit["min_roi"]:
        gaps.append(f"ROI too low: {roi:.1%} (need >{crit['min_roi']:.0%})")

    if not gaps:
        reason = (
            f"unlocked: {resolved} resolved bets, "
            f"Brier={brier:.3f}, win_rate={win_rate:.1%}, ROI={roi:.1%}"
        )
        return (next_tier, reason)

    return (current_tier, "locked: needs " + " / ".join(gaps))

# ─── KELLY SIZING ─────────────────────────────────────────────────────────────

def kelly_bet(prob, price, bankroll, tier):
    """
    Half-Kelly bet sizing, clamped to tier max.

    prob  : model probability of YES (0–1)
    price : current market price / implied prob (0–1)
    bankroll : available USDC
    tier  : current tier int

    Returns USDC amount to bet (0.0 if Kelly is negative).
    """
    if price <= 0 or price >= 1:
        return 0.0

    # Kelly fraction for a binary bet at market price `price`
    # f = (prob * (1 - price) - (1 - prob) * price) / (1 - price)
    edge   = prob * (1 - price) - (1 - prob) * price
    f      = edge / (1 - price)

    if f <= 0:
        return 0.0

    f_half  = f * 0.5
    raw_bet = f_half * bankroll
    max_bet = TIERS[tier]["max_bet"]

    return max(0.10, min(raw_bet, max_bet))


def _city_region(city):
    city = (city or "").lower()
    region_map = {
        "east_asia": {
            "hong kong", "shenzhen", "guangzhou", "taipei", "shanghai",
            "beijing", "wuhan", "chengdu", "chongqing", "seoul", "busan", "tokyo",
        },
        "southeast_asia": {"singapore", "kuala lumpur", "jakarta", "manila"},
        "europe": {
            "london", "paris", "madrid", "milan", "amsterdam", "warsaw",
            "helsinki", "moscow", "istanbul", "ankara", "munich",
        },
        "north_america": {
            "new york city", "new york", "nyc", "toronto", "seattle", "dallas",
            "atlanta", "miami", "chicago", "houston", "austin", "denver",
            "los angeles", "san francisco", "mexico city", "panama city",
        },
    }
    for region, cities in region_map.items():
        if city in cities:
            return region
    return "other"


def estimate_correlation(signal_a, signal_b):
    city_a = (signal_a.get("city") or "").lower()
    city_b = (signal_b.get("city") or "").lower()
    end_a = signal_a.get("end_date") or signal_a.get("target_date") or ""
    end_b = signal_b.get("end_date") or signal_b.get("target_date") or ""
    hedge_a = signal_a.get("hedge_group")
    hedge_b = signal_b.get("hedge_group")

    if hedge_a and hedge_b and hedge_a == hedge_b:
        return 0.15

    if city_a == city_b and end_a == end_b:
        return 0.85
    if city_a == city_b:
        return 0.40
    if _city_region(city_a) == _city_region(city_b) and end_a == end_b:
        return 0.60
    if _city_region(city_a) == _city_region(city_b):
        return 0.25
    return 0.10


def build_correlation_matrix(signals):
    matrix = []
    for i, sig_a in enumerate(signals):
        row = []
        for j, sig_b in enumerate(signals):
            row.append(1.0 if i == j else estimate_correlation(sig_a, sig_b))
        matrix.append(row)
    return matrix


def portfolio_kelly(signals, bankroll, tier):
    """
    Correlation-aware Kelly shrinkage.

    Starts from per-bet sizes already computed by each signal engine and
    dampens overlapping exposure before enforcing the tier daily cap.
    """
    if bankroll <= 0 or not signals:
        return [0.0 for _ in signals]

    max_daily_frac = TIERS[tier]["max_daily_pct"]
    max_bet = TIERS[tier]["max_bet"]
    base_fracs = [
        max(0.0, min(float(sig.get("bet", 0.0)) / bankroll, max_daily_frac))
        for sig in signals
    ]
    matrix = build_correlation_matrix(signals)
    order = sorted(
        range(len(signals)),
        key=lambda i: (
            abs(float(signals[i].get("edge", 0.0))),
            float(signals[i].get("ev", 0.0)),
        ),
        reverse=True,
    )

    adjusted = [0.0] * len(signals)
    for idx in order:
        corr_load = 0.0
        for jdx, weight in enumerate(adjusted):
            if weight <= 0:
                continue
            corr_load += max(matrix[idx][jdx], 0.0) * weight
        penalty = 1.0 + 2.0 * corr_load
        adjusted[idx] = base_fracs[idx] / penalty

    total = sum(adjusted)
    if total > max_daily_frac and total > 0:
        scale = max_daily_frac / total
        adjusted = [w * scale for w in adjusted]

    return [round(min(max_bet, max(0.0, frac * bankroll)), 2) for frac in adjusted]

# ─── DAILY BUDGET ─────────────────────────────────────────────────────────────

def daily_budget(bankroll, tier, already_deployed):
    """Return remaining USDC budget for today."""
    max_deploy = bankroll * TIERS[tier]["max_daily_pct"]
    remaining  = max_deploy - already_deployed
    return max(0.0, remaining)

# ─── STOP-LOSS ────────────────────────────────────────────────────────────────

def stop_loss_hit(bankroll, starting_bankroll, tier):
    """
    Returns True if drawdown limit for the tier has been breached.

    Also writes starting_bankroll to disk if the file is missing.
    The `starting_bankroll` argument is used as the reference if the file
    doesn't exist yet — the caller typically passes the current bankroll on
    first run (which writes it), then subsequent calls compare against it.
    """
    # Try to load persisted starting bankroll
    ref = starting_bankroll
    if os.path.exists(_BANKROLL_FILE):
        try:
            with open(_BANKROLL_FILE) as f:
                ref = float(json.load(f).get("starting_bankroll", starting_bankroll))
        except Exception:
            ref = starting_bankroll
    else:
        # First run — persist current bankroll as the baseline
        try:
            os.makedirs(os.path.dirname(_BANKROLL_FILE), exist_ok=True)
            with open(_BANKROLL_FILE, "w") as f:
                json.dump({"starting_bankroll": starting_bankroll}, f)
        except Exception:
            pass

    if ref <= 0:
        return False

    drawdown_ratio = bankroll / ref

    if tier in (1, 2):
        return drawdown_ratio < 0.50   # 50% drawdown
    elif tier in (3, 4):
        return drawdown_ratio < 0.70   # 30% drawdown
    elif tier >= 5:
        return drawdown_ratio < 0.80   # 20% drawdown

    return False  # T0 paper — no stop-loss

# ─── TIER DETECTION ───────────────────────────────────────────────────────────

def get_current_tier(env_dict):
    """Read TIER env var (default '1'). Returns int."""
    return int(env_dict.get("TIER", "1"))

# ─── STATUS PRINTER ───────────────────────────────────────────────────────────

def print_tier_status(bankroll, tier, stats):
    """Print current tier, unlock progress, and next unlock requirements."""
    cfg = TIERS[tier]
    print(f"\n{'─'*60}", flush=True)
    print(f"CAPITAL TIER: {cfg['name']}", flush=True)
    print(f"  Bankroll : ${bankroll:.2f}  |  Max bet: ${cfg['max_bet']:.2f}  "
          f"|  Max daily: {cfg['max_daily_pct']:.0%}", flush=True)

    resolved = stats.get("resolved_bets", 0)
    brier    = stats.get("brier_score",   1.0)
    win_rate = stats.get("win_rate",      0.0)
    roi      = stats.get("roi",           0.0)

    print(f"  Stats    : {resolved} resolved | Brier={brier:.3f} | "
          f"WR={win_rate:.1%} | ROI={roi:.1%}", flush=True)

    next_tier = tier + 1
    if next_tier in UNLOCK_CRITERIA:
        crit = UNLOCK_CRITERIA[next_tier]
        next_name = TIERS[next_tier]["name"]
        print(f"  Next     : {next_name} requires "
              f"{crit['min_resolved']} bets / Brier<{crit['max_brier']} / "
              f"WR>{crit['min_win_rate']:.0%} / ROI>{crit['min_roi']:.0%}", flush=True)
    else:
        print("  Next     : Max tier reached.", flush=True)

    print(f"{'─'*60}", flush=True)
