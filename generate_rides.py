#!/usr/bin/env python3
import os
import uuid
import math
import argparse
from dataclasses import dataclass
from datetime import date, datetime, timedelta, time, timezone
import numpy as np
import pandas as pd
import holidays
from typing import Dict

try:
    from azure.storage.blob import BlobServiceClient, ContentSettings
    AZURE_AVAILABLE = True
except Exception:
    AZURE_AVAILABLE = False


# -----------------------------
# Simple .env-style state file
# -----------------------------
def read_env_file(path: str) -> Dict[str, str]:
    """Read a simple KEY=VALUE file. Ignores blank lines and comments (#...)."""
    env: Dict[str, str] = {}
    if not path or not os.path.exists(path):
        return env
    with open(path, "r") as f:
        for line in f:
            s = line.strip()
            if not s or s.startswith("#") or "=" not in s:
                continue
            k, v = s.split("=", 1)
            env[k.strip()] = v.strip()
    return env

def write_env_file(path: str, mapping: Dict[str, str]) -> None:
    """Write a simple KEY=VALUE file (overwrites)."""
    with open(path, "w") as f:
        for k, v in mapping.items():
            f.write(f"{k}={v}\n")


# -----------------------------
# Configurable city catalog
# -----------------------------
@dataclass
class City:
    name: str
    state: str
    tz: str
    lat_min: float
    lat_max: float
    lon_min: float
    lon_max: float
    base_fare: float
    per_mile: float
    per_minute: float
    zones: list
    weight: float  # share of total rides

CITIES = [
    City("New York", "NY", "America/New_York", 40.49, 40.92, -74.27, -73.68,
         base_fare=2.75, per_mile=3.00, per_minute=0.55,
         zones=["Manhattan", "Brooklyn", "Queens", "Bronx", "Staten Island"], weight=0.32),
    City("Chicago", "IL", "America/Chicago", 41.64, 42.02, -87.94, -87.52,
         base_fare=2.25, per_mile=2.30, per_minute=0.40,
         zones=["Loop", "North Side", "South Side", "West Side"], weight=0.18),
    City("Los Angeles", "CA", "America/Los_Angeles", 33.70, 34.34, -118.67, -118.15,
         base_fare=2.20, per_mile=2.40, per_minute=0.38,
         zones=["DTLA", "Hollywood", "Santa Monica", "Pasadena", "Venice"], weight=0.20),
    City("San Francisco", "CA", "America/Los_Angeles", 37.70, 37.84, -122.52, -122.35,
         base_fare=2.50, per_mile=3.10, per_minute=0.60,
         zones=["SOMA", "Mission", "Sunset", "Richmond", "FiDi"], weight=0.12),
    City("Phoenix", "AZ", "America/Phoenix", 33.26, 33.80, -112.32, -111.90,
         base_fare=2.00, per_mile=2.05, per_minute=0.32,
         zones=["Downtown", "Tempe", "Scottsdale", "Mesa", "Glendale"], weight=0.18),
]

# Payment/device/type distributions
PAYMENT_TYPES = ["card", "wallet", "cash"]
PAYMENT_P = [0.92, 0.06, 0.02]
DEVICE_TYPES = ["ios", "android"]
DEVICE_P = [0.48, 0.52]
STATUSES = ["completed", "cancelled", "no_show"]
# base cancellation/no_show rates; adjusted by rain/traffic
STATUS_P = [0.92, 0.06, 0.02]

# Time-of-day demand shape (per hour, before weekend/holiday/weather multipliers)
# Morning commute, evening commute, late night weekend bumps.
HOUR_WEIGHTS_WEEKDAY = np.array(
    [0.8, 0.5, 0.4, 0.4, 0.5, 0.7, 1.6, 2.0, 1.4, 1.0, 0.9, 0.9,
     0.9, 1.0, 1.1, 1.3, 1.8, 2.3, 1.9, 1.6, 1.4, 1.2, 1.0, 0.9]
)
HOUR_WEIGHTS_WEEKEND = np.array(
    [1.0, 0.9, 0.8, 0.8, 0.9, 1.2, 1.6, 1.8, 1.7, 1.5, 1.3, 1.2,
     1.2, 1.3, 1.4, 1.7, 2.1, 2.7, 2.4, 2.2, 2.0, 2.1, 2.3, 2.2]
)

# Weather model (very simple seasonal)
WEATHERS = ["clear", "rain", "snow", "fog"]
def weather_probs(month: int, city: City):
    # crude seasonality: snow only in winter for non-Phoenix/LA
    rain = 0.13 if month in (1,2,3,11,12) else 0.10
    if city.name in ("Phoenix", "Los Angeles"):
        snow = 0.0
    else:
        snow = 0.12 if month in (12,1,2) else 0.02
    fog = 0.05 if city.name in ("San Francisco",) else 0.03
    clear = max(0.0, 1.0 - (rain + snow + fog))
    return [clear, rain, snow, fog]

def choose_weather(rng, month, city):
    p = weather_probs(month, city)
    return rng.choice(WEATHERS, p=p)

def traffic_level_for(hour, is_weekend, weather):
    base = 1 if hour in (0,1,2,3,4,5) else (2 if hour in (6,7,8,16,17,18) else 1)
    if is_weekend:
        base = max(1, base - 1)  # slightly less commuter traffic
        if hour in (22,23,0,1):  # nightlife
            base += 1
    if weather in ("rain", "snow", "fog"):
        base += 1
    return min(base, 3)  # 1=low, 2=med, 3=high

def surge_for(traffic_level, is_weekend, hour, weather, rng):
    surge = 1.0
    if traffic_level == 2: surge += 0.2
    if traffic_level == 3: surge += 0.6
    if is_weekend and hour in (22,23,0,1): surge += 0.3
    if weather in ("rain","snow"): surge += 0.25
    # small random jitter
    surge += rng.normal(0, 0.05)
    return round(max(1.0, min(surge, 3.0)), 2)

def bounded_normal(rng, mean, sd, lo, hi):
    x = rng.normal(mean, sd)
    return float(max(lo, min(hi, x)))

def lognormal_miles(rng, city, is_weekend):
    # City-specific central tendency; weekends skew slightly longer leisure trips
    mu = math.log(4.0 if city.name in ("New York","San Francisco") else 5.0)
    sigma = 0.6
    miles = float(rng.lognormal(mu, sigma))
    if is_weekend:
        miles *= 1.08
    return max(0.3, min(60.0, miles))

def speed_mph(city, traffic_level, weather, hour, rng):
    base = 18 if city.name in ("New York","San Francisco") else 24
    if traffic_level == 2: base -= 3
    if traffic_level == 3: base -= 7
    if weather in ("rain","snow","fog"): base -= 2
    if hour in (22,23,0,1,2,3): base += 3
    return max(6.0, float(base + rng.normal(0, 2.0)))

def random_point_in_box(rng, city):
    lat = rng.uniform(city.lat_min, city.lat_max)
    lon = rng.uniform(city.lon_min, city.lon_max)
    return round(lat, 6), round(lon, 6)

def pick_zone(rng, city):
    return rng.choice(city.zones)

def is_us_holiday(d, us_h):
    return d in us_h

def hour_weights(is_weekend):
    w = HOUR_WEIGHTS_WEEKEND if is_weekend else HOUR_WEIGHTS_WEEKDAY
    return w / w.sum()

def choose_hour(rng, is_weekend):
    p = hour_weights(is_weekend)
    return int(rng.choice(np.arange(24), p=p))

def season_multiplier(day_of_year):
    # mild yearly seasonality (summer uptick)
    return 1.0 + 0.15 * math.sin(2 * math.pi * (day_of_year / 365.0))

def daily_count(rng, d, is_weekend, is_holiday, target_mean):
    lam = target_mean * season_multiplier(d.timetuple().tm_yday)
    if is_weekend: lam *= 1.8
    if is_holiday: lam *= 2.2
    lam = max(1.0, lam)
    return int(rng.poisson(lam))

def split_across_cities(rng, total):
    weights = np.array([c.weight for c in CITIES])
    weights = weights / weights.sum()
    # Multinomial split
    return rng.multinomial(total, weights)

def money_round(x):
    return round(float(x), 2)

def build_day_df(rng, d, per_day_target_mean):
    is_weekend = d.weekday() >= 5
    us_h = holidays.UnitedStates(years=range(d.year-1, d.year+2))
    is_holiday = is_us_holiday(d, us_h)
    total = daily_count(rng, d, is_weekend, is_holiday, per_day_target_mean)
    if total == 0:
        return pd.DataFrame()

    counts = split_across_cities(rng, total)
    rows = []
    for city, city_n in zip(CITIES, counts):
        if city_n == 0:
            continue
        # Precompute hour distribution
        for _ in range(city_n):
            hr = choose_hour(rng, is_weekend)
            minute = int(rng.integers(0, 60))
            second = int(rng.integers(0, 60))

            # Treat timestamps as UTC to simplify; you can localize later if you prefer
            start_dt = datetime.combine(d, time(hr, minute, second, tzinfo=timezone.utc))

            weather = choose_weather(rng, d.month, city)
            traffic = traffic_level_for(hr, is_weekend, weather)
            surge = surge_for(traffic, is_weekend, hr, weather, rng)

            status = rng.choice(STATUSES, p=_status_probs(weather, traffic, is_weekend))

            pickup_lat, pickup_lon = random_point_in_box(rng, city)
            drop_lat, drop_lon = random_point_in_box(rng, city)

            # IDs
            ride_id = str(uuid.uuid4())
            rider_id = f"r_{int(rng.integers(10000000, 99999999))}"
            driver_id = f"d_{int(rng.integers(100000, 999999))}"

            # Distance & duration
            if status == "completed":
                miles = lognormal_miles(rng, city, is_weekend)
                mph = speed_mph(city, traffic, weather, hr, rng)
                drive_min = (miles / mph) * 60.0
                wait_min = max(0.0, rng.normal(4.0 if traffic >= 2 else 3.0, 1.0))
                duration_min = max(1.0, drive_min + wait_min + rng.normal(0, 1.2))
                end_dt = start_dt + timedelta(minutes=duration_min)
            else:
                miles = 0.0
                mph = 0.0
                drive_min = 0.0
                wait_min = max(0.0, rng.normal(5.0, 1.5))
                duration_min = wait_min
                end_dt = start_dt  # no trip

            # Pricing
            base = city.base_fare
            fare_distance = city.per_mile * miles
            fare_time = city.per_minute * (max(0.0, duration_min - wait_min))
            tolls = max(0.0, rng.normal(1.2 if city.name in ("New York","San Francisco") else 0.5, 0.8))
            tolls = 0.0 if status != "completed" else max(0.0, tolls)
            subtotal = (base + fare_distance + fare_time + tolls) * surge if status == "completed" else 0.0
            taxes = 0.08 * subtotal
            platform_fee = 0.25 * subtotal
            coupon = max(0.0, rng.normal(0.0, 0.6))
            # small chance of promo:
            promo_code = None
            if rng.random() < 0.05 and status == "completed":
                promo_code = rng.choice(["WELCOME5","WEEKEND10","RAINRIDE"])
                coupon += rng.choice([1.0, 2.0, 3.0])

            fare_total = max(0.0, subtotal + taxes - coupon)
            tip = 0.0 if status != "completed" else max(0.0, rng.normal(2.5, 1.8))
            driver_earnings = max(0.0, subtotal - platform_fee + tip)

            # Ratings
            rider_rating = bounded_normal(rng, 4.75, 0.18, 3.0, 5.0)
            driver_rating = bounded_normal(rng, 4.80, 0.15, 3.0, 5.0)

            # Categorical
            payment_type = str(rng.choice(PAYMENT_TYPES, p=PAYMENT_P))
            device_type = str(rng.choice(DEVICE_TYPES, p=DEVICE_P))
            pickup_zone = pick_zone(rng, city)
            drop_zone = pick_zone(rng, city)

            # Assemble row
            rows.append({
                "ride_id": ride_id,
                "rider_id": rider_id,
                "driver_id": driver_id,
                "city": city.name,
                "state": city.state,
                "pickup_zone": pickup_zone,
                "dropoff_zone": drop_zone,
                "pickup_lat": pickup_lat,
                "pickup_lon": pickup_lon,
                "dropoff_lat": drop_lat,
                "dropoff_lon": drop_lon,
                "start_time_utc": start_dt.isoformat(),
                "end_time_utc": end_dt.isoformat(),
                "status": status,
                "distance_miles": round(miles, 3),
                "duration_minutes": round(duration_min, 2),
                "wait_time_minutes": round(wait_min, 2),
                "avg_speed_mph": round(mph, 2),
                "traffic_level": {1:"low",2:"medium",3:"high"}[traffic],
                "weather": weather,
                "surge_multiplier": surge,
                "base_fare": money_round(base),
                "per_mile_rate": money_round(city.per_mile),
                "per_minute_rate": money_round(city.per_minute),
                "tolls": money_round(tolls),
                "taxes": money_round(taxes),
                "coupon_discount": money_round(coupon),
                "fare_total": money_round(fare_total),
                "tip": money_round(tip),
                "platform_fee": money_round(platform_fee),
                "driver_earnings": money_round(driver_earnings),
                "payment_type": payment_type,
                "device_type": device_type,
                "rider_rating": round(rider_rating, 2),
                "driver_rating": round(driver_rating, 2),
                "is_weekend": bool(d.weekday() >= 5),
                "is_holiday": bool(is_holiday),
                "promo_code": promo_code
            })

    df = pd.DataFrame(rows)
    # Stable column order for COPY INTO later
    COLS = [
        "ride_id","rider_id","driver_id","city","state","pickup_zone","dropoff_zone",
        "pickup_lat","pickup_lon","dropoff_lat","dropoff_lon","start_time_utc","end_time_utc",
        "status","distance_miles","duration_minutes","wait_time_minutes","avg_speed_mph",
        "traffic_level","weather","surge_multiplier","base_fare","per_mile_rate","per_minute_rate",
        "tolls","taxes","coupon_discount","fare_total","tip","platform_fee","driver_earnings",
        "payment_type","device_type","rider_rating","driver_rating","is_weekend","is_holiday","promo_code"
    ]
    return df[COLS]


def _status_probs(weather, traffic_level, is_weekend):
    # start from base and perturb by context
    p_completed, p_cancel, p_noshow = STATUS_P
    if weather in ("rain", "snow"):  # more cancels/no-shows
        p_cancel += 0.01
        p_noshow += 0.01
        p_completed -= 0.02
    if traffic_level == 3:
        p_cancel += 0.01
        p_completed -= 0.01
    if is_weekend and weather == "clear":
        p_completed += 0.01
        p_cancel -= 0.01
    # normalize
    s = p_completed + p_cancel + p_noshow
    return [p_completed/s, p_cancel/s, p_noshow/s]

def write_local(df, root, d):
    path = os.path.join(root, f"{d.year:04d}", f"{d:%m}", f"{d:%d}")
    os.makedirs(path, exist_ok=True)
    p = os.path.join(path, "rides.parquet")
    df.to_parquet(p, index=False)
    return p

def write_azure(df, container_client, d):
    blob_path = f"{d.year:04d}/{d:%m}/{d:%d}/rides.parquet"
    parquet_bytes = df.to_parquet(index=False, engine="pyarrow")
    # Azure upload expects bytes → Parquet writer returns a file, so wrap:
    import io
    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False, engine="pyarrow")
    buffer.seek(0)
    blob_client = container_client.get_blob_client(blob_path)
    blob_client.upload_blob(buffer.getvalue(), overwrite=True,
                            content_settings=ContentSettings(content_type="application/octet-stream"))
    return f"azure://{container_client.container_name}/{blob_path}"

def main():
    parser = argparse.ArgumentParser(description="Generate synthetic Uber-like rides data.")
    parser.add_argument("--days", type=int, default=365, help="Number of days back from yesterday.")
    parser.add_argument("--seed", type=int, default=42, help="Random seed.")
    parser.add_argument("--out", choices=["local","azure"], default="local", help="Output destination.")
    parser.add_argument("--root", type=str, default="./lake/raw/uberides", help="Local root when --out local.")
    parser.add_argument("--mean", type=float, default=1200.0,
                        help="Weekday mean rides/day across all cities BEFORE weekend/holiday multipliers.")
    # Azure config
    parser.add_argument("--azure-container", type=str, default="raw")
    parser.add_argument("--state-file", type=str, default=".uberides.env",
                        help="Path to .env-like file storing LAST_DATE (YYYY-MM-DD).")
    parser.add_argument("--ignore-state", action="store_true",
                        help="If set, do not read or update the state file; fall back to --days.")
    args = parser.parse_args()

    rng = np.random.default_rng(args.seed)

    today = date.today()
    # If state is enabled and a LAST_DATE exists, resume from the day after it.
    if not args.ignore_state:
        state = read_env_file(args.state_file)
        last_str = state.get("LAST_DATE")
    else:
        state = {}
        last_str = None

    if last_str:
        try:
            last_date = datetime.strptime(last_str, "%Y-%m-%d").date()
        except ValueError:
            raise RuntimeError(f"Invalid LAST_DATE in {args.state_file}: {last_str!r}. Expected YYYY-MM-DD.")
        start_date = last_date + timedelta(days=1)
    else:
        # Fall back to historical backfill of --days ending today
        start_date = today - timedelta(days=args.days-1)

    end_date = today  # inclusive through today

    if args.out == "azure":
        if not AZURE_AVAILABLE:
            raise RuntimeError("azure-storage-blob not available. Install it and try again.")
        conn = os.environ.get("AZURE_UBERRIDES_STORAGE_CONNECTION_STRING")
        if not conn:
            raise RuntimeError("Set AZURE_UBERRIDES_STORAGE_CONNECTION_STRING for Azure output.")
        bsc = BlobServiceClient.from_connection_string(conn)
        container_client = bsc.get_container_client(args.azure_container)
        try:
            container_client.create_container()  # idempotent
        except Exception:
            pass
    else:
        container_client = None

    total_rows = 0
    last_processed = None
    d = start_date
    while d <= end_date:
        df = build_day_df(rng, d, args.mean)
        if df.empty:
            d += timedelta(days=1)
            continue
        if args.out == "local":
            outpath = write_local(df, args.root, d)
        else:
            outpath = write_azure(df, container_client, d)
        print(f"{d.isoformat()} -> {outpath} [{len(df)} rows]")
        total_rows += len(df)
        last_processed = d
        d += timedelta(days=1)

    # Update the state file to remember the most recent date we attempted/generated.
    if not args.ignore_state:
        # If nothing ran (e.g., start_date > end_date), keep existing LAST_DATE if present.
        new_last = (last_processed or (datetime.strptime(state["LAST_DATE"], "%Y-%m-%d").date() if state.get("LAST_DATE") else None))
        if new_last:
            state["LAST_DATE"] = new_last.isoformat()
            write_env_file(args.state_file, state)

    print(f"Done. Wrote ~{total_rows:,} rows across {args.days} days.")

if __name__ == "__main__":
    main()