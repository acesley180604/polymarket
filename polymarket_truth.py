"""
Truth Resolution & Calibration Tracker
Fetches actual HK temps from HKO after market resolution,
scores Brier/CRPS against our predictions, tracks tier unlock criteria.

Run daily (after HKO reanalysis lag, ~1 day):
  python3 polymarket_truth.py

Tier unlock criteria (Tier 1):
  brier_score < 0.20  AND  win_rate > 0.52  over ≥ 20 resolved trades
"""

import os, json, time, re
import requests
from datetime import date, datetime, timedelta
from polymarket_core import ENV as _env, f_to_c, c_to_f, TRADES_JSONL as TRADES_JSONL_DEFAULT, TRUTH_CACHE_JSON, CALIBRATION_STATS_JSON, CITY_CALIBRATION_JSON, EMOS_RETRAIN_QUEUE_JSON

# ─── ENV / CONFIG ─────────────────────────────────────────
TRUTH_CACHE_DEFAULT   = TRUTH_CACHE_JSON
CALIB_STATS_DEFAULT   = CALIBRATION_STATS_JSON

HKO_API  = "https://data.weather.gov.hk/weatherAPI/opendata/climate.php"
ERA5_API = "https://archive-api.open-meteo.com/v1/archive"
HK_LAT   = 22.3020
HK_LON   = 114.1739

# ─── 1. FETCH ACTUAL HKO TEMPERATURE ─────────────────────

def fetch_hko_actual(date_str: str) -> float | None:
    """
    Fetch actual HK daily max temperature for date_str (YYYY-MM-DD).
    Primary: HKO climate API.
    Fallback: Open-Meteo ERA5 archive.
    Returns Celsius float, or None if unavailable.
    """
    # ── Primary: HKO climate API ─────────────────────────
    try:
        r = requests.get(
            HKO_API,
            params={"dataType": "CLMTEMP", "lang": "en", "rformat": "json"},
            timeout=15,
        )
        r.raise_for_status()
        data = r.json()

        # HKO returns structured data; walk to find daily max for our date
        target_year  = date_str[:4]
        target_month = date_str[5:7]
        target_day   = int(date_str[8:10])

        # Try "ClimateMaxTemp" or similar key structures
        for section in data.get("ClimateMaxTemp", []):
            if str(section.get("year", "")) == target_year and \
               str(section.get("month", "")).zfill(2) == target_month:
                daily = section.get("daily", [])
                if target_day <= len(daily):
                    val = daily[target_day - 1]
                    if val not in (None, "", "N/A"):
                        return float(val)

        # Alternative HKO JSON layout: list of {"date":..., "maxTemp":...}
        records = data.get("data", data.get("records", []))
        for rec in records:
            rec_date = str(rec.get("date", rec.get("Date", "")))
            # Support YYYYMMDD or YYYY-MM-DD
            normalised = rec_date[:4] + "-" + rec_date[4:6] + "-" + rec_date[6:] \
                if len(rec_date) == 8 else rec_date
            if normalised == date_str:
                for key in ("maxTemp", "MaxTemp", "max", "Max", "temperature_max"):
                    if key in rec and rec[key] not in (None, "", "N/A"):
                        return float(rec[key])

    except Exception as e:
        print(f"  [HKO] API error for {date_str}: {e} — falling back to ERA5", flush=True)

    # ── Fallback: Open-Meteo ERA5 archive ────────────────
    try:
        r = requests.get(
            ERA5_API,
            params={
                "latitude":         HK_LAT,
                "longitude":        HK_LON,
                "daily":            "temperature_2m_max",
                "temperature_unit": "celsius",
                "start_date":       date_str,
                "end_date":         date_str,
                "timezone":         "auto",
            },
            timeout=20,
        )
        r.raise_for_status()
        vals = r.json().get("daily", {}).get("temperature_2m_max", [])
        if vals and vals[0] is not None:
            return float(vals[0])
    except Exception as e:
        print(f"  [ERA5] Fallback also failed for {date_str}: {e}", flush=True)

    return None


# ─── QUESTION PARSER ──────────────────────────────────────

def parse_question_bucket(question: str):
    """
    Extract (low, high, unit) from a Polymarket temperature question.
    Handles both C and F, range/above/below formats.
    Returns (low_c, high_c) in Celsius, or None if parse fails.
    """
    q = question

    # "between X and Y °F/°C" or "between X–Y °F"
    m = re.search(r'between\s+(\d+\.?\d*)\s*(?:°\s*)?([CF])?\s*(?:and|[-–])\s*(\d+\.?\d*)\s*°?\s*([CF])?', q, re.I)
    if m:
        lo   = float(m.group(1))
        hi   = float(m.group(3))
        unit = (m.group(4) or m.group(2) or "F").upper()
        if unit == "F":
            return f_to_c(lo), f_to_c(hi)
        return lo, hi

    # "exceed / above / higher than X °C/°F"
    m = re.search(r'(?:exceed|above|higher than)\s+(\d+\.?\d*)\s*°?\s*([CF])', q, re.I)
    if m:
        val  = float(m.group(1))
        unit = m.group(2).upper()
        hi_c = 999.0
        lo_c = f_to_c(val) if unit == "F" else val
        return lo_c, hi_c

    # "below / not exceed X °C/°F"
    m = re.search(r'(?:below|not exceed|at most)\s+(\d+\.?\d*)\s*°?\s*([CF])', q, re.I)
    if m:
        val  = float(m.group(1))
        unit = m.group(2).upper()
        hi_c = f_to_c(val) if unit == "F" else val
        return -999.0, hi_c

    # "be X °F or below"
    m = re.search(r'be\s+(-?\d+\.?\d*)\s*°?\s*([CF])\s+or\s+below', q, re.I)
    if m:
        val  = float(m.group(1))
        unit = m.group(2).upper()
        hi_c = f_to_c(val) if unit == "F" else val
        return -999.0, hi_c

    # "be X °F or above/higher"
    m = re.search(r'be\s+(\d+\.?\d*)\s*°?\s*([CF])\s+or\s+(?:above|higher)', q, re.I)
    if m:
        val  = float(m.group(1))
        unit = m.group(2).upper()
        lo_c = f_to_c(val) if unit == "F" else val
        return lo_c, 999.0

    # Single value "be X°C on" (exact)
    m = re.search(r'be\s+(-?\d+\.?\d*)\s*°([CF])\s+on', q, re.I)
    if m:
        val  = float(m.group(1))
        unit = m.group(2).upper()
        v_c  = f_to_c(val) if unit == "F" else val
        return v_c - 0.5, v_c + 0.5   # ±0.5° tolerance for exact bucket

    return None


def outcome_for_temp(actual_c: float, low_c: float, high_c: float) -> bool:
    """
    True if actual_c falls in [low_c, high_c].
    Handles open-ended buckets (low=-999 or high=999).
    """
    if low_c <= -999.0:
        return actual_c <= high_c
    if high_c >= 999.0:
        return actual_c >= low_c
    return low_c <= actual_c <= high_c


# ─── 2. RESOLVE TRADES ────────────────────────────────────

def resolve_trades(trades_jsonl_path: str, truth_cache_path: str) -> list:
    """
    Read trades.jsonl, resolve any unresolved HK trades whose end_date has passed.
    Writes outcome + brier_score back to trades.jsonl (in-place update).
    Returns list of newly resolved trade dicts.
    """
    if not os.path.exists(trades_jsonl_path):
        print(f"  [resolve] Trades file not found: {trades_jsonl_path}", flush=True)
        return []

    # Load truth cache
    truth_cache = {}
    if os.path.exists(truth_cache_path):
        try:
            with open(truth_cache_path) as f:
                truth_cache = json.load(f)
        except Exception:
            truth_cache = {}

    today = date.today()

    # Read all trades
    trades = []
    with open(trades_jsonl_path) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                trades.append(json.loads(line))
            except json.JSONDecodeError:
                continue

    newly_resolved = []

    for trade in trades:
        # Skip already resolved
        if trade.get("outcome") is not None:
            continue

        end_date_str = trade.get("end_date", "")
        if not end_date_str:
            continue

        try:
            end_date = date.fromisoformat(end_date_str[:10])
        except ValueError:
            continue

        # Must be at least 1 full day in the past (HKO/ERA5 reanalysis lag)
        if (today - end_date).days < 1:
            continue

        question = trade.get("question", "")
        city     = trade.get("city", "")

        # Only auto-resolve HK markets (truth source is HKO)
        if "hong kong" not in city.lower() and "hk" not in city.lower():
            # For non-HK cities, mark as needs_manual_resolution
            trade["outcome_note"] = "non-hk: manual resolution needed"
            continue

        # Parse bucket from question
        bucket = parse_question_bucket(question)
        if not bucket:
            print(f"  [resolve] Could not parse bucket: {question[:60]}", flush=True)
            trade["outcome_note"] = "parse_failed"
            continue

        low_c, high_c = bucket

        # Fetch actual temp (with cache)
        date_key = end_date_str[:10]
        if date_key not in truth_cache:
            actual_c = fetch_hko_actual(date_key)
            if actual_c is not None:
                truth_cache[date_key] = actual_c
                print(f"  [resolve] {date_key}: HKO actual = {actual_c:.1f}°C", flush=True)
            else:
                print(f"  [resolve] {date_key}: could not fetch actual — skipping", flush=True)
                continue
        else:
            actual_c = truth_cache[date_key]

        # Determine outcome
        direction = trade.get("direction", "BUY YES")
        raw_outcome = outcome_for_temp(actual_c, low_c, high_c)

        # If we BUY NO, win when bucket is False
        if "NO" in direction.upper():
            win = not raw_outcome
        else:
            win = raw_outcome

        model_prob = float(trade.get("model_prob", trade.get("f_prob", 0.5)))
        # For Brier score: probability assigned to the YES outcome
        if "NO" in direction.upper():
            # We bet NO at trade_price, so our YES-equivalent prob is (1 - model_prob)
            brier_prob = 1.0 - model_prob
        else:
            brier_prob = model_prob

        actual_outcome_int = 1 if raw_outcome else 0
        brier = round((brier_prob - actual_outcome_int) ** 2, 6)

        trade["outcome"]     = win
        trade["outcome_raw"] = raw_outcome
        trade["actual_c"]    = actual_c
        trade["brier_score"] = brier
        trade["resolved_on"] = today.isoformat()

        newly_resolved.append(trade)
        print(
            f"  [resolve] {city} | {date_key} | bucket [{low_c:.1f},{high_c:.1f}]°C "
            f"actual={actual_c:.1f}°C → {'YES' if raw_outcome else 'NO'} "
            f"(trade={'WIN' if win else 'LOSS'}) brier={brier:.4f}",
            flush=True,
        )
        time.sleep(0.3)  # polite rate-limit for HKO/ERA5

    # Write updated trades back
    with open(trades_jsonl_path, "w") as f:
        for trade in trades:
            f.write(json.dumps(trade) + "\n")

    # Save truth cache
    os.makedirs(os.path.dirname(truth_cache_path), exist_ok=True)
    with open(truth_cache_path, "w") as f:
        json.dump(truth_cache, f, indent=2)

    return newly_resolved


# ─── 3. COMPUTE CALIBRATION ───────────────────────────────

def compute_calibration(trades_jsonl_path: str) -> dict:
    """
    Read all resolved trades, compute Brier score + calibration bins.
    Returns structured stats dict.
    """
    if not os.path.exists(trades_jsonl_path):
        return {"error": "trades file not found", "n_resolved": 0, "n_total": 0}

    resolved = []
    total    = 0

    with open(trades_jsonl_path) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                t = json.loads(line)
            except json.JSONDecodeError:
                continue
            total += 1
            if t.get("outcome") is not None and t.get("brier_score") is not None:
                resolved.append(t)

    if not resolved:
        return {
            "brier_score":      None,
            "n_resolved":       0,
            "n_total":          total,
            "win_rate":         None,
            "calibration_bins": [],
            "by_source":        {},
            "by_setup":         {},
            "by_strategy_arm":  {},
        }

    # ── Brier score & win rate ────────────────────────────
    brier_scores = [t["brier_score"] for t in resolved]
    wins         = [t for t in resolved if t.get("outcome") is True]
    mean_brier   = round(sum(brier_scores) / len(brier_scores), 6)
    win_rate     = round(len(wins) / len(resolved), 4)

    # ── Calibration bins (10 bins, 0-10% through 90-100%) ─
    bins = [{"range": f"{i*10}-{(i+1)*10}%", "n_trades": 0, "predicted_sum": 0.0,
             "actual_sum": 0.0, "brier_sum": 0.0}
            for i in range(10)]

    for t in resolved:
        direction  = t.get("direction", "BUY YES")
        model_prob = float(t.get("model_prob", t.get("f_prob", 0.5)))
        if "NO" in direction.upper():
            pred_yes = 1.0 - model_prob
        else:
            pred_yes = model_prob

        actual_yes = 1 if t.get("outcome_raw", t.get("outcome")) else 0

        bin_idx = min(int(pred_yes * 10), 9)
        b = bins[bin_idx]
        b["n_trades"]      += 1
        b["predicted_sum"] += pred_yes
        b["actual_sum"]    += actual_yes
        b["brier_sum"]     += t["brier_score"]

    calibration_bins = []
    for b in bins:
        if b["n_trades"] == 0:
            calibration_bins.append({
                "range":            b["range"],
                "n_trades":         0,
                "predicted_avg":    None,
                "actual_freq":      None,
                "brier_contribution": 0.0,
            })
        else:
            calibration_bins.append({
                "range":              b["range"],
                "n_trades":           b["n_trades"],
                "predicted_avg":      round(b["predicted_sum"] / b["n_trades"], 4),
                "actual_freq":        round(b["actual_sum"]    / b["n_trades"], 4),
                "brier_contribution": round(b["brier_sum"]     / len(resolved), 6),
            })

    # ── By source (delta vs conviction vs ladder) ─────────
    by_source = {}
    for t in resolved:
        source = t.get("urgency", t.get("source", "conviction"))
        if source not in by_source:
            by_source[source] = {"n": 0, "wins": 0, "brier_sum": 0.0}
        by_source[source]["n"]         += 1
        by_source[source]["wins"]      += 1 if t.get("outcome") else 0
        by_source[source]["brier_sum"] += t["brier_score"]

    by_source_stats = {}
    for src, s in by_source.items():
        by_source_stats[src] = {
            "n":          s["n"],
            "win_rate":   round(s["wins"] / s["n"], 4),
            "brier_score": round(s["brier_sum"] / s["n"], 6),
        }

    by_setup = {}
    by_arm = {}
    for t in resolved:
        setup = t.get("setup_type") or "unknown"
        arm = t.get("strategy_arm") or setup
        for bucket, key in [(by_setup, setup), (by_arm, arm)]:
            if key not in bucket:
                bucket[key] = {"n": 0, "wins": 0, "brier_sum": 0.0}
            bucket[key]["n"] += 1
            bucket[key]["wins"] += 1 if t.get("outcome") else 0
            bucket[key]["brier_sum"] += t["brier_score"]

    by_setup_stats = {
        key: {
            "n": s["n"],
            "win_rate": round(s["wins"] / s["n"], 4),
            "brier_score": round(s["brier_sum"] / s["n"], 6),
        }
        for key, s in by_setup.items()
    }
    by_arm_stats = {
        key: {
            "n": s["n"],
            "win_rate": round(s["wins"] / s["n"], 4),
            "brier_score": round(s["brier_sum"] / s["n"], 6),
        }
        for key, s in by_arm.items()
    }

    return {
        "brier_score":      mean_brier,
        "n_resolved":       len(resolved),
        "n_total":          total,
        "win_rate":         win_rate,
        "calibration_bins": calibration_bins,
        "by_source":        by_source_stats,
        "by_setup":         by_setup_stats,
        "by_strategy_arm":  by_arm_stats,
        "computed_at":      date.today().isoformat(),
    }


def update_city_calibration(trades_jsonl_path: str, calib_file: str = CITY_CALIBRATION_JSON) -> dict:
    """
    Fit per-city isotonic calibration on resolved trades.
    Saves breakpoint maps for later lookup in the live scanner.
    """
    if not os.path.exists(trades_jsonl_path):
        return {}

    try:
        from sklearn.isotonic import IsotonicRegression
    except Exception as e:
        print(f"[truth] isotonic regression unavailable: {e}", flush=True)
        return {}

    city_rows = {}
    with open(trades_jsonl_path) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                t = json.loads(line)
            except json.JSONDecodeError:
                continue
            if t.get("outcome_raw") is None:
                continue
            city = (t.get("city") or "").lower()
            if not city:
                continue
            direction = (t.get("direction") or "BUY YES").upper()
            model_prob = float(t.get("model_prob", t.get("f_prob", 0.5)) or 0.5)
            pred_yes = 1.0 - model_prob if "NO" in direction else model_prob
            resolved_on = t.get("resolved_on") or t.get("end_date") or ""
            city_rows.setdefault(city, []).append({
                "pred_yes": max(0.01, min(0.99, pred_yes)),
                "actual_yes": 1.0 if t.get("outcome_raw") else 0.0,
                "brier": float(t.get("brier_score", 0.0) or 0.0),
                "resolved_on": resolved_on,
            })

    updated = {}
    cutoff = date.today() - timedelta(days=7)
    for city, rows in city_rows.items():
        if len(rows) < 10:
            continue
        x = [r["pred_yes"] for r in rows]
        y = [r["actual_yes"] for r in rows]
        try:
            ir = IsotonicRegression(out_of_bounds="clip")
            ir.fit(x, y)
            pts_x = getattr(ir, "X_thresholds_", x)
            pts_y = getattr(ir, "y_thresholds_", ir.predict(pts_x))
            breakpoints = [
                [round(float(px), 6), round(float(py), 6)]
                for px, py in zip(pts_x, pts_y)
            ]
        except Exception:
            continue

        recent_briers = []
        for row in rows:
            try:
                resolved_day = date.fromisoformat(str(row["resolved_on"])[:10])
            except Exception:
                resolved_day = None
            if resolved_day is not None and resolved_day >= cutoff:
                recent_briers.append(row["brier"])

        updated[city] = {
            "n": len(rows),
            "updated": date.today().isoformat(),
            "breakpoints": breakpoints,
            "brier": round(sum(r["brier"] for r in rows) / len(rows), 6),
            "rolling_7d_brier": round(sum(recent_briers) / len(recent_briers), 6) if recent_briers else None,
        }

    if updated:
        with open(calib_file, "w") as f:
            json.dump(updated, f, indent=2)
        print(f"[truth] City calibration saved → {calib_file}", flush=True)
    return updated


def check_emos_retrain_needed(city_calibration: dict, emos_coeffs: dict) -> list[str]:
    flagged = []
    for city, calib in city_calibration.items():
        rolling_brier = calib.get("rolling_7d_brier")
        if rolling_brier is None:
            continue
        city_coeffs = emos_coeffs.get(city, {})
        crps_vals = []
        if isinstance(city_coeffs, dict):
            if city_coeffs.get("crps") is not None:
                crps_vals.append(float(city_coeffs["crps"]))
            for bucket_coeffs in city_coeffs.values():
                if isinstance(bucket_coeffs, dict) and bucket_coeffs.get("crps") is not None:
                    crps_vals.append(float(bucket_coeffs["crps"]))
        if not crps_vals:
            continue
        baseline_crps = sum(crps_vals) / len(crps_vals)
        if rolling_brier > baseline_crps * 1.5:
            flagged.append(city)

    with open(EMOS_RETRAIN_QUEUE_JSON, "w") as f:
        json.dump({"updated": date.today().isoformat(), "cities": flagged}, f, indent=2)
    print(f"[truth] EMOS retrain queue saved → {EMOS_RETRAIN_QUEUE_JSON}", flush=True)
    return flagged


# ─── 4. PRINT CALIBRATION REPORT ─────────────────────────

def print_calibration_report(stats: dict) -> None:
    """Pretty-print calibration stats. Flags Tier 1 unlock criteria."""
    print("\n" + "=" * 70, flush=True)
    print("POLYMARKET WEATHER  —  CALIBRATION REPORT", flush=True)
    print("=" * 70, flush=True)

    if stats.get("error") or stats.get("n_resolved", 0) == 0:
        print(f"  No resolved trades yet. "
              f"({stats.get('n_total', 0)} total trades logged)", flush=True)
        return

    n_res  = stats["n_resolved"]
    n_tot  = stats["n_total"]
    brier  = stats["brier_score"]
    wr     = stats["win_rate"]

    print(f"  Resolved:     {n_res} / {n_tot} trades", flush=True)
    print(f"  Brier score:  {brier:.4f}  (0=perfect, 0.25=random)", flush=True)
    print(f"  Win rate:     {wr:.1%}", flush=True)
    print(flush=True)

    # ── Calibration bins ─────────────────────────────────
    print("  CALIBRATION BINS (predicted prob → actual frequency):", flush=True)
    print(f"  {'Bin':>12s}  {'N':>5s}  {'Pred%':>7s}  {'Actual%':>8s}  {'Brier':>8s}  {'Quality':>10s}", flush=True)
    print("  " + "-" * 65, flush=True)
    for b in stats.get("calibration_bins", []):
        if b["n_trades"] == 0:
            continue
        pred   = b["predicted_avg"]
        actual = b["actual_freq"]
        diff   = abs(pred - actual)
        quality = "WELL CAL" if diff < 0.05 else ("OK" if diff < 0.10 else "OVER/UNDER")
        print(
            f"  {b['range']:>12s}  {b['n_trades']:>5d}  "
            f"{pred:>6.1%}  {actual:>7.1%}  "
            f"{b['brier_contribution']:>8.5f}  {quality:>10s}",
            flush=True,
        )

    # ── By source ─────────────────────────────────────────
    by_src = stats.get("by_source", {})
    if by_src:
        print(flush=True)
        print("  BY SIGNAL SOURCE:", flush=True)
        print(f"  {'Source':>12s}  {'N':>5s}  {'WinRate':>8s}  {'Brier':>8s}", flush=True)
        print("  " + "-" * 40, flush=True)
        for src, s in sorted(by_src.items()):
            print(f"  {src:>12s}  {s['n']:>5d}  {s['win_rate']:>7.1%}  {s['brier_score']:>8.4f}", flush=True)

    by_setup = stats.get("by_setup", {})
    if by_setup:
        print(flush=True)
        print("  BY SETUP:", flush=True)
        print(f"  {'Setup':>18s}  {'N':>5s}  {'WinRate':>8s}  {'Brier':>8s}", flush=True)
        print("  " + "-" * 48, flush=True)
        for setup, s in sorted(by_setup.items()):
            print(f"  {setup:>18s}  {s['n']:>5d}  {s['win_rate']:>7.1%}  {s['brier_score']:>8.4f}", flush=True)

    by_arm = stats.get("by_strategy_arm", {})
    if by_arm:
        print(flush=True)
        print("  BY STRATEGY ARM:", flush=True)
        print(f"  {'Arm':>18s}  {'N':>5s}  {'WinRate':>8s}  {'Brier':>8s}", flush=True)
        print("  " + "-" * 48, flush=True)
        for arm, s in sorted(by_arm.items()):
            print(f"  {arm:>18s}  {s['n']:>5d}  {s['win_rate']:>7.1%}  {s['brier_score']:>8.4f}", flush=True)

    # ── Tier 1 unlock check ───────────────────────────────
    MIN_TRADES_FOR_UNLOCK = 20
    T1_BRIER_THRESH       = 0.20
    T1_WINRATE_THRESH     = 0.52

    print(flush=True)
    print("  TIER 1 UNLOCK CRITERIA:", flush=True)

    brier_ok = brier is not None and brier < T1_BRIER_THRESH
    wr_ok    = wr is not None    and wr    > T1_WINRATE_THRESH
    n_ok     = n_res >= MIN_TRADES_FOR_UNLOCK

    print(f"    Brier < {T1_BRIER_THRESH}:   {'PASS' if brier_ok else 'FAIL':>4s}  (current: {brier:.4f})", flush=True)
    print(f"    Win rate > {T1_WINRATE_THRESH:.0%}: {'PASS' if wr_ok  else 'FAIL':>4s}  (current: {wr:.1%})", flush=True)
    print(f"    N >= {MIN_TRADES_FOR_UNLOCK}:          {'PASS' if n_ok   else 'FAIL':>4s}  (current: {n_res})", flush=True)

    if brier_ok and wr_ok and n_ok:
        print(flush=True)
        print("  *** TIER 1 UNLOCK: ALL CRITERIA MET — READY TO SCALE SIZING ***", flush=True)
    else:
        missing = []
        if not brier_ok: missing.append(f"need brier < {T1_BRIER_THRESH} (gap: {brier - T1_BRIER_THRESH:+.4f})")
        if not wr_ok:    missing.append(f"need win rate > {T1_WINRATE_THRESH:.0%} (gap: {wr - T1_WINRATE_THRESH:+.1%})")
        if not n_ok:     missing.append(f"need {MIN_TRADES_FOR_UNLOCK - n_res} more resolved trades")
        print(f"  Tier 1 not yet unlocked: {'; '.join(missing)}", flush=True)

    print("=" * 70, flush=True)


# ─── 5. MAIN ──────────────────────────────────────────────

def main():
    trades_path = _env.get("TRADES_JSONL", TRADES_JSONL_DEFAULT)
    truth_cache = TRUTH_CACHE_DEFAULT
    calib_path  = CALIB_STATS_DEFAULT

    # Ensure directories exist
    for path in [trades_path, truth_cache, calib_path]:
        os.makedirs(os.path.dirname(path), exist_ok=True)

    print(f"[truth] Trades file:  {trades_path}", flush=True)
    print(f"[truth] Truth cache:  {truth_cache}", flush=True)
    print(flush=True)

    print("[1] Resolving unresolved trades...", flush=True)
    newly = resolve_trades(trades_path, truth_cache)
    print(f"    Resolved {len(newly)} new trade(s) this run.", flush=True)

    print("\n[2] Computing calibration stats...", flush=True)
    stats = compute_calibration(trades_path)

    print_calibration_report(stats)

    # Save stats
    with open(calib_path, "w") as f:
        json.dump(stats, f, indent=2)
    print(f"\n[truth] Calibration stats saved → {calib_path}", flush=True)

    city_calibration = update_city_calibration(trades_path)
    if city_calibration:
        try:
            import polymarket_emos as emos_mod
            flagged = check_emos_retrain_needed(city_calibration, emos_mod.load_coefficients())
            if flagged:
                print(f"[truth] EMOS retrain queued for: {', '.join(flagged)}", flush=True)
            else:
                print("[truth] No EMOS retrain needed.", flush=True)
        except Exception as e:
            print(f"[truth] EMOS queue step failed: {e}", flush=True)


if __name__ == "__main__":
    main()
