"""
ts-EMOS + QRF blended calibration for weather forecasts.

ts-EMOS (temporal-seasonal EMOS):
  Reference: Gneiting et al. (2005) Eq. 10, extended with Fourier seasonal terms.

  μ  = a + b1·X1 + b2·X2 + b3·X3 + γ1·sin(2πt/365) + γ2·cos(2πt/365)
  σ² = c + d·S²  + γ3·sin(2πt/365) + γ4·cos(2πt/365)

  X1/X2/X3     = GFS / ECMWF / ICON deterministic forecast means
  S²           = inter-model variance (spread-skill signal)
  t            = day-of-year (0-364)
  γ1-γ4        = seasonal amplitude terms (critical for HK monsoon: Jun-Sep hot, Nov-Feb cold)
  a,b1..b3     = bias + per-model weights (learned from 40-day window)
  c,d          = predictive variance intercept + spread coefficient

QRF (Quantile Regression Forest):
  GradientBoostingRegressor (sklearn) trained at α=0.1, 0.5, 0.9 for q10/q50/q90.
  Features: [gfs, ecmwf, icon, var, day_sin, day_cos]

Blending: 60% EMOS + 40% QRF for final prob_bucket.

Training: 40-day sliding window, tail-weighted CRPS (upweights >1.5σ extremes by up to 3×).
EMOS+: b weights ≥ 0 → auto-drops redundant/collinear models.
Lead-time buckets: ["0-12", "12-24", "24-36", "36-48"] — separate coefficients per bucket per city.
"""

import os, json, time, pickle
import numpy as np
from datetime import date, timedelta
from scipy.stats import norm, skewnorm
from scipy.optimize import minimize
import requests
from polymarket_core import EMOS_COEFFICIENTS_JSON, EMOS_RETRAIN_QUEUE_JSON

# ─── PATHS ────────────────────────────────────────────────
COEFF_FILE  = EMOS_COEFFICIENTS_JSON
QRF_DIR     = os.path.dirname(COEFF_FILE)

TRAINING_DAYS = 40  # sweet spot: 30-45 per paper

LEAD_BUCKETS = ["0-12", "12-24", "24-36", "36-48"]

def _lead_bucket(lead_hours: int) -> str:
    if lead_hours <= 12:
        return "0-12"
    elif lead_hours <= 24:
        return "12-24"
    elif lead_hours <= 36:
        return "24-36"
    else:
        return "36-48"

def _city_slug(city: str) -> str:
    return city.lower().replace(" ", "_").replace("/", "_")

# ─── CRPS closed-form for Gaussian ────────────────────────
def _crps(mu, sigma, y):
    """CRPS for N(mu, sigma²) vs observation y. Lower = better."""
    if sigma <= 0:
        return abs(y - mu)
    z = (y - mu) / sigma
    return sigma * (z * (2.0 * norm.cdf(z) - 1.0) + 2.0 * norm.pdf(z) - 1.0 / np.sqrt(np.pi))

# ─── DATA FETCHERS ────────────────────────────────────────
def _fetch_hindcast(lat, lon, start, end, model):
    """Deterministic hindcast for a model via historical-forecast-api."""
    r = requests.get(
        "https://historical-forecast-api.open-meteo.com/v1/forecast",
        params={
            "latitude": lat, "longitude": lon,
            "daily": "temperature_2m_max",
            "temperature_unit": "celsius",
            "models": model,
            "start_date": start, "end_date": end,
            "timezone": "auto",
        },
        timeout=25,
    )
    r.raise_for_status()
    vals = r.json().get("daily", {}).get("temperature_2m_max", [])
    return [float(v) if v is not None else None for v in vals]

def _fetch_actuals(lat, lon, start, end):
    """ERA5 reanalysis actual max temps (archive-api)."""
    r = requests.get(
        "https://archive-api.open-meteo.com/v1/archive",
        params={
            "latitude": lat, "longitude": lon,
            "daily": "temperature_2m_max",
            "temperature_unit": "celsius",
            "start_date": start, "end_date": end,
            "timezone": "auto",
        },
        timeout=25,
    )
    r.raise_for_status()
    vals = r.json().get("daily", {}).get("temperature_2m_max", [])
    return [float(v) if v is not None else None for v in vals]

def fetch_training_data(lat, lon, n_days=TRAINING_DAYS, lead_hours=24):
    """
    Fetch n_days of hindcast + actuals for one location.
    Skip last 6 days (ERA5 reanalysis ~5-day lag).
    Returns list of dicts: {gfs, ecmwf, icon, var, actual, day_of_year, day_sin, day_cos}.
    lead_hours is stored in rows for potential downstream use.
    """
    end_date   = date.today() - timedelta(days=6)
    start_date = date.today() - timedelta(days=n_days + 6)
    end   = end_date.isoformat()
    start = start_date.isoformat()

    try:
        actuals = _fetch_actuals(lat, lon, start, end)
    except Exception:
        return []

    n = len(actuals)
    raw = {}
    for model in ["gfs_seamless", "ecmwf_ifs025", "icon_seamless"]:
        try:
            raw[model] = _fetch_hindcast(lat, lon, start, end, model)
            time.sleep(0.25)
        except Exception:
            raw[model] = [None] * n

    rows = []
    for i in range(n):
        y    = actuals[i]
        gfs  = raw["gfs_seamless"][i]  if i < len(raw["gfs_seamless"])  else None
        ecm  = raw["ecmwf_ifs025"][i]  if i < len(raw["ecmwf_ifs025"])  else None
        icon = raw["icon_seamless"][i] if i < len(raw["icon_seamless"]) else None
        if y is None:
            continue
        vals = [v for v in [gfs, ecm, icon] if v is not None]
        if not vals:
            continue
        mean_fill = float(np.mean(vals))
        # day-of-year for the i-th training date
        row_date    = start_date + timedelta(days=i)
        doy         = row_date.timetuple().tm_yday - 1  # 0-364
        day_sin     = np.sin(2 * np.pi * doy / 365.0)
        day_cos     = np.cos(2 * np.pi * doy / 365.0)
        rows.append({
            "gfs":        gfs   if gfs   is not None else mean_fill,
            "ecmwf":      ecm   if ecm   is not None else mean_fill,
            "icon":       icon  if icon  is not None else mean_fill,
            "var":        float(np.var(vals)) if len(vals) > 1 else 1.0,
            "actual":     y,
            "day_of_year": doy,
            "day_sin":    float(day_sin),
            "day_cos":    float(day_cos),
            "lead_hours": lead_hours,
        })
    return rows

# ─── FIT ts-EMOS+ ─────────────────────────────────────────
def fit_emos(rows):
    """
    Fit ts-EMOS+ by minimizing tail-weighted mean CRPS over training rows.

    Parameters: [a, b1, b2, b3, c, d, g1, g2, g3, g4]
      μ  = a + b1·X1 + b2·X2 + b3·X3 + g1·sin(2πt/365) + g2·cos(2πt/365)
      σ² = c + d·S² + g3·sin(2πt/365) + g4·cos(2πt/365)

    Tail weight: upweights events >1.5σ from naive mean by up to 3×.
    EMOS+: all b weights ≥ 0.

    Returns coefficient dict or None if fit fails / too little data.
    """
    if len(rows) < 15:
        return None

    X1  = np.array([r["gfs"]        for r in rows])
    X2  = np.array([r["ecmwf"]      for r in rows])
    X3  = np.array([r["icon"]       for r in rows])
    S2  = np.array([r["var"]        for r in rows])
    Y   = np.array([r["actual"]     for r in rows])
    DS  = np.array([r["day_sin"]    for r in rows])
    DC  = np.array([r["day_cos"]    for r in rows])

    mu_baseline    = float(np.mean(Y))
    sigma_baseline = max(float(np.std(Y)), 0.01)

    # Tail weights: up-weight extremes >1.5σ above naive mean by up to 3×
    tail_weights = 1.0 + 2.0 * np.maximum(0.0, (Y - mu_baseline) / sigma_baseline - 1.5)

    def objective(p):
        a, b1, b2, b3, c, d, g1, g2, g3, g4 = p
        mu     = a + b1*X1 + b2*X2 + b3*X3 + g1*DS + g2*DC
        var    = c + d*S2 + g3*DS + g4*DC
        sigma  = np.sqrt(np.maximum(var, 0.01))
        crps_v = np.array([_crps(float(mu[i]), float(sigma[i]), float(Y[i])) for i in range(len(Y))])
        return float(np.mean(tail_weights * crps_v))

    x0     = [0.0, 0.33, 0.33, 0.34, 1.0, 0.5, 0.0, 0.0, 0.0, 0.0]
    bounds = [
        (None, None),   # a:  bias intercept
        (0.0, None),    # b1: GFS weight ≥ 0
        (0.0, None),    # b2: ECMWF weight ≥ 0
        (0.0, None),    # b3: ICON weight ≥ 0
        (0.01, None),   # c:  variance floor
        (0.0, None),    # d:  spread coefficient ≥ 0
        (None, None),   # g1: μ seasonal sin (free)
        (None, None),   # g2: μ seasonal cos (free)
        (None, None),   # g3: σ² seasonal sin (free)
        (None, None),   # g4: σ² seasonal cos (free)
    ]

    try:
        res = minimize(objective, x0, bounds=bounds, method="L-BFGS-B",
                       options={"maxiter": 3000, "ftol": 1e-10})
        if res.fun < 6.0:  # CRPS > 6°C → useless fit
            a, b1, b2, b3, c, d, g1, g2, g3, g4 = [round(float(x), 5) for x in res.x]
            return {
                "a": a, "b1": b1, "b2": b2, "b3": b3,
                "c": c, "d": d,
                "g1": g1, "g2": g2, "g3": g3, "g4": g4,
                "crps": round(res.fun, 4),
                "n":    len(rows),
                "updated": date.today().isoformat(),
            }
    except Exception:
        pass
    return None

# ─── FIT QRF ──────────────────────────────────────────────
def fit_qrf(rows):
    """
    Fit quantile regression forest (GradientBoostingRegressor) at q10/q50/q90.
    Features: [gfs, ecmwf, icon, var, day_sin, day_cos]
    Returns dict with keys 'q10', 'q50', 'q90' each holding a fitted estimator,
    or None if not enough data.
    """
    from sklearn.ensemble import GradientBoostingRegressor

    if len(rows) < 15:
        return None

    X = np.array([
        [r["gfs"], r["ecmwf"], r["icon"], r["var"], r["day_sin"], r["day_cos"]]
        for r in rows
    ])
    y = np.array([r["actual"] for r in rows])

    models = {}
    for q, alpha in [("q10", 0.1), ("q50", 0.5), ("q90", 0.9)]:
        gbr = GradientBoostingRegressor(
            loss="quantile",
            alpha=alpha,
            n_estimators=200,
            max_depth=4,
            learning_rate=0.05,
            subsample=0.8,
            random_state=42,
        )
        try:
            gbr.fit(X, y)
            models[q] = gbr
        except Exception:
            return None

    return models  # dict: {"q10": estimator, "q50": estimator, "q90": estimator}

def predict_qrf(features_vec, models_dict):
    """
    Predict q10/q50/q90 from a QRF models dict.
    features_vec: [gfs, ecmwf, icon, var, day_sin, day_cos]
    Returns (q10, q50, q90) in Celsius.
    """
    X = np.array(features_vec).reshape(1, -1)
    q10 = float(models_dict["q10"].predict(X)[0])
    q50 = float(models_dict["q50"].predict(X)[0])
    q90 = float(models_dict["q90"].predict(X)[0])
    # Enforce monotonicity
    q10, q50, q90 = sorted([q10, q50, q90])
    return q10, q50, q90

def _save_qrf(city, models_dict):
    slug = _city_slug(city)
    path = os.path.join(QRF_DIR, f"emos_qrf_{slug}.pkl")
    os.makedirs(QRF_DIR, exist_ok=True)
    with open(path, "wb") as f:
        pickle.dump(models_dict, f, protocol=4)
    return path

def _load_qrf(city):
    slug = _city_slug(city)
    path = os.path.join(QRF_DIR, f"emos_qrf_{slug}.pkl")
    if not os.path.exists(path):
        return None
    try:
        with open(path, "rb") as f:
            return pickle.load(f)
    except Exception:
        return None

def prob_bucket_qrf(q10, q50, q90, low_c, high_c):
    """
    Fit a skew-normal distribution to (q10, q50, q90) then compute
    P(low_c ≤ T ≤ high_c).  Clips to [0.01, 0.99].
    """
    # Estimate location/scale/skew from quantile triplet via moment matching
    iqr = max(q90 - q10, 0.01)
    loc   = q50
    scale = max(iqr / (2 * 1.2816), 0.1)  # 2 * Φ⁻¹(0.9) ≈ 2.5631
    skew_est = (q90 + q10 - 2 * q50) / max(iqr, 0.01)  # Bowley's skewness proxy
    a_param  = np.clip(skew_est * 3.0, -5.0, 5.0)

    if high_c >= 999.0:
        p = 1.0 - skewnorm.cdf(low_c, a_param, loc=loc, scale=scale)
    elif low_c <= -999.0:
        p = skewnorm.cdf(high_c, a_param, loc=loc, scale=scale)
    else:
        p = skewnorm.cdf(high_c, a_param, loc=loc, scale=scale) - \
            skewnorm.cdf(low_c, a_param, loc=loc, scale=scale)
    return float(max(0.01, min(0.99, p)))

# ─── TRAIN ALL CITIES ─────────────────────────────────────
def train_all(city_coords, verbose=True):
    """
    Train ts-EMOS and QRF for every city × lead-time bucket in city_coords dict.
    COEFF_FILE structure: {"hong kong": {"12-24": {...coeffs...}, "24-36": {...}}, ...}
    QRF models saved as pickle files: emos_qrf_{city_slug}.pkl per city.
    Merges into existing COEFF_FILE (keeps cities/buckets not in this run).
    """
    os.makedirs(QRF_DIR, exist_ok=True)
    try:
        with open(COEFF_FILE) as f:
            all_coeffs = json.load(f)
    except Exception:
        all_coeffs = {}

    for city, (lat, lon) in city_coords.items():
        city_key = city.lower()
        if city_key not in all_coeffs:
            all_coeffs[city_key] = {}

        # Collect rows for QRF once (model-agnostic, use 24h as reference)
        qrf_rows_all = []

        for bucket in LEAD_BUCKETS:
            # Representative lead hours for each bucket
            lead_map = {"0-12": 6, "12-24": 18, "24-36": 30, "36-48": 42}
            lead_h   = lead_map[bucket]

            if verbose:
                print(f"  ts-EMOS {city.title():22s} [{bucket}h] ...", end=" ", flush=True)
            try:
                rows = fetch_training_data(lat, lon, lead_hours=lead_h)
                if not rows:
                    if verbose: print("no data")
                    continue

                c = fit_emos(rows)
                if c:
                    all_coeffs[city_key][bucket] = c
                    if verbose:
                        print(
                            f"CRPS={c['crps']:.3f}°C  "
                            f"b=[GFS:{c['b1']:.2f} ECMWF:{c['b2']:.2f} ICON:{c['b3']:.2f}]  "
                            f"bias={c['a']:+.2f}°C  "
                            f"seasonal(μ)=[{c['g1']:+.2f},{c['g2']:+.2f}]  "
                            f"n={c['n']}",
                            flush=True,
                        )
                    # Accumulate rows for QRF (de-dupe by day_of_year is fine here)
                    qrf_rows_all.extend(rows)
                else:
                    if verbose: print(f"fit failed ({len(rows)} rows)")
            except Exception as e:
                if verbose: print(f"ERROR: {e}")
            time.sleep(0.5)

        # Train QRF once per city using all accumulated rows
        if qrf_rows_all:
            if verbose:
                print(f"  QRF     {city.title():22s} [{len(qrf_rows_all)} rows] ...", end=" ", flush=True)
            try:
                qrf_models = fit_qrf(qrf_rows_all)
                if qrf_models:
                    _save_qrf(city, qrf_models)
                    if verbose: print("saved", flush=True)
                else:
                    if verbose: print("fit failed", flush=True)
            except Exception as e:
                if verbose: print(f"QRF ERROR: {e}", flush=True)

    with open(COEFF_FILE, "w") as f:
        json.dump(all_coeffs, f, indent=2)

    trained_cities = sum(1 for v in all_coeffs.values() if isinstance(v, dict) and v)
    if verbose:
        print(f"\nts-EMOS: {trained_cities}/{len(city_coords)} cities trained. Saved → {COEFF_FILE}", flush=True)
    return all_coeffs

def load_coefficients():
    """Load coefficient file. Returns nested dict {city: {bucket: coeffs}}."""
    try:
        with open(COEFF_FILE) as f:
            return json.load(f)
    except Exception:
        return {}


def retrain_flagged_cities(queue_file=EMOS_RETRAIN_QUEUE_JSON):
    try:
        with open(queue_file) as f:
            queue = json.load(f)
    except Exception:
        return []

    flagged = [c.lower() for c in queue.get("cities", []) if c]
    if not flagged:
        return []

    from polymarket_core import CITY_COORDS
    coords = {city: coords for city, coords in CITY_COORDS.items() if city.lower() in flagged}
    if not coords:
        return []

    print(f"Retraining flagged EMOS cities: {', '.join(sorted(coords))}", flush=True)
    train_all(coords)
    with open(queue_file, "w") as f:
        json.dump({"updated": date.today().isoformat(), "cities": []}, f, indent=2)
    return list(coords)

# ─── PREDICT (BLENDED ts-EMOS + QRF) ─────────────────────
def predict_blended(city, gfs_members, ecmwf_members, icon_members,
                    coeffs, coeffs_qrf=None, lead_hours=24, day_of_year=None):
    """
    Blended ts-EMOS (60%) + QRF (40%) prediction.

    Args:
        city:           City name string.
        gfs/ecmwf/icon_members: Lists of ensemble member values (Celsius).
        coeffs:         Full coefficient dict from load_coefficients().
        coeffs_qrf:     Optional pre-loaded QRF models dict (from _load_qrf).
                        If None, will attempt to load from disk.
        lead_hours:     Forecast lead time in hours (default 24).
        day_of_year:    0-364. If None, uses today.

    Returns:
        (mu, sigma, blend_info_dict)
        blend_info_dict keys: emos_prob, qrf_prob, blended_prob,
                               mu_emos, sigma_emos, q10, q50, q90
    """
    city_key = city.lower()
    bucket   = _lead_bucket(lead_hours)

    if day_of_year is None:
        day_of_year = date.today().timetuple().tm_yday - 1  # 0-364
    day_sin = np.sin(2 * np.pi * day_of_year / 365.0)
    day_cos = np.cos(2 * np.pi * day_of_year / 365.0)

    gfs_mean   = float(np.mean(gfs_members))   if gfs_members   else None
    ecmwf_mean = float(np.mean(ecmwf_members)) if ecmwf_members else None
    icon_mean  = float(np.mean(icon_members))  if icon_members  else None

    all_vals = (gfs_members or []) + (ecmwf_members or []) + (icon_members or [])
    raw_mean = float(np.mean(all_vals)) if all_vals else 20.0
    raw_std  = float(np.std(all_vals))  if len(all_vals) > 1 else 2.0

    # Fallback if no data at all
    if not all_vals:
        info = dict(emos_prob=None, qrf_prob=None, blended_prob=None,
                    mu_emos=raw_mean, sigma_emos=raw_std, q10=None, q50=None, q90=None)
        return raw_mean, max(raw_std, 0.5), info

    model_vals = [v for v in [gfs_mean, ecmwf_mean, icon_mean] if v is not None]
    inter_var  = float(np.var(model_vals)) if len(model_vals) > 1 else 1.0

    # --- ts-EMOS ---
    city_coeffs = coeffs.get(city_key, {})
    # Accept both new nested format and legacy flat format
    if isinstance(city_coeffs, dict) and bucket in city_coeffs:
        c = city_coeffs[bucket]
    elif isinstance(city_coeffs, dict) and "a" in city_coeffs:
        c = city_coeffs  # legacy flat
    else:
        c = None

    if c:
        g1 = c.get("g1", 0.0)
        g2 = c.get("g2", 0.0)
        g3 = c.get("g3", 0.0)
        g4 = c.get("g4", 0.0)
        mu_emos = (c["a"]
                   + c["b1"] * (gfs_mean   if gfs_mean   is not None else raw_mean)
                   + c["b2"] * (ecmwf_mean if ecmwf_mean is not None else raw_mean)
                   + c["b3"] * (icon_mean  if icon_mean  is not None else raw_mean)
                   + g1 * day_sin + g2 * day_cos)
        var_emos   = max(c["c"] + c["d"] * inter_var + g3 * day_sin + g4 * day_cos, 0.01)
        sigma_emos = float(np.sqrt(var_emos))
    else:
        mu_emos    = raw_mean
        sigma_emos = max(raw_std, 0.5)

    # --- QRF ---
    if coeffs_qrf is None:
        coeffs_qrf = _load_qrf(city)

    qrf_q10 = qrf_q50 = qrf_q90 = None
    if coeffs_qrf:
        try:
            features = [
                gfs_mean   if gfs_mean   is not None else raw_mean,
                ecmwf_mean if ecmwf_mean is not None else raw_mean,
                icon_mean  if icon_mean  is not None else raw_mean,
                inter_var,
                float(day_sin),
                float(day_cos),
            ]
            qrf_q10, qrf_q50, qrf_q90 = predict_qrf(features, coeffs_qrf)
        except Exception:
            qrf_q10 = qrf_q50 = qrf_q90 = None

    blend_info = dict(
        mu_emos=float(mu_emos),
        sigma_emos=float(sigma_emos),
        q10=qrf_q10,
        q50=qrf_q50,
        q90=qrf_q90,
        emos_prob=None,
        qrf_prob=None,
        blended_prob=None,
    )
    return float(mu_emos), float(sigma_emos), blend_info

def predict(city, gfs_members, ecmwf_members, icon_members, coeffs,
            lead_hours=24, day_of_year=None):
    """
    Apply ts-EMOS to today's live ensemble members.
    Returns (mu_celsius, sigma_celsius).
    Falls back to raw ensemble mean/std if no coefficients for city.

    Internally calls predict_blended(). Use predict_blended() directly
    for full blend_info (QRF quantiles, blended probs).
    """
    mu, sigma, _ = predict_blended(
        city, gfs_members, ecmwf_members, icon_members,
        coeffs, lead_hours=lead_hours, day_of_year=day_of_year,
    )
    return mu, sigma

def prob_bucket(mu, sigma, low_c, high_c):
    """
    P(low_c ≤ max_temp ≤ high_c) from Gaussian N(mu, sigma²).
    Clips to [0.01, 0.99].
    """
    if high_c >= 999.0:
        p = 1.0 - norm.cdf(low_c, mu, sigma)
    elif low_c <= -999.0:
        p = norm.cdf(high_c, mu, sigma)
    else:
        p = norm.cdf(high_c, mu, sigma) - norm.cdf(low_c, mu, sigma)
    return float(max(0.01, min(0.99, p)))

def prob_bucket_blended(city, gfs_members, ecmwf_members, icon_members,
                        coeffs, low_c, high_c,
                        lead_hours=24, day_of_year=None):
    """
    Convenience wrapper: compute 60% EMOS + 40% QRF blended P(low_c ≤ T ≤ high_c).

    Returns blend_info_dict with keys:
        emos_prob, qrf_prob, blended_prob, mu_emos, sigma_emos, q10, q50, q90
    """
    mu, sigma, info = predict_blended(
        city, gfs_members, ecmwf_members, icon_members,
        coeffs, lead_hours=lead_hours, day_of_year=day_of_year,
    )
    emos_p = prob_bucket(mu, sigma, low_c, high_c)
    info["emos_prob"] = emos_p

    if info["q10"] is not None and info["q50"] is not None and info["q90"] is not None:
        qrf_p = prob_bucket_qrf(info["q10"], info["q50"], info["q90"], low_c, high_c)
        info["qrf_prob"] = qrf_p
        blended_p = 0.6 * emos_p + 0.4 * qrf_p
        info["blended_prob"] = float(max(0.01, min(0.99, blended_p)))
    else:
        info["qrf_prob"]     = None
        info["blended_prob"] = emos_p  # fall back to EMOS only

    return info

# ─── STANDALONE TRAINING ──────────────────────────────────
if __name__ == "__main__":
    from polymarket_core import CITY_COORDS
    import sys
    city_filter = sys.argv[1].lower() if len(sys.argv) > 1 else None
    coords = {k: v for k, v in CITY_COORDS.items() if not city_filter or city_filter in k}
    if city_filter:
        print(
            f"Training ts-EMOS + QRF for {len(coords)} cities "
            f"(40-day window, tail-weighted CRPS, {len(LEAD_BUCKETS)} lead buckets)...\n"
        )
        train_all(coords)
    else:
        retrained = retrain_flagged_cities()
        if not retrained:
            print(
                f"Training ts-EMOS + QRF for {len(coords)} cities "
                f"(40-day window, tail-weighted CRPS, {len(LEAD_BUCKETS)} lead buckets)...\n"
            )
            train_all(coords)
