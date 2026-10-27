import time
from pathlib import Path

import pandas as pd
import requests

URL = "https://archive-api.open-meteo.com/v1/archive"
VARIABLES = "shortwave_radiation,temperature_2m,cloud_cover,wind_speed_10m"
YEARS = range(2015, 2026)  # 2015 à 2025 inclus

BASE_DIR = Path(__file__).resolve().parent.parent
RAW_DIR = BASE_DIR / "data" / "raw" / "open_meteo"

GOUVERNORATS = {
    "Tunis": (36.8065, 10.1815), "Ariana": (36.8625, 10.1956),
    "Ben_Arous": (36.7533, 10.2282), "Manouba": (36.8101, 10.0863),
    "Nabeul": (36.4513, 10.7357), "Zaghouan": (36.4029, 10.1429),
    "Bizerte": (37.2744, 9.8739), "Beja": (36.7256, 9.1817),
    "Jendouba": (36.5011, 8.7803), "Le_Kef": (36.1826, 8.7148),
    "Siliana": (36.0850, 9.3708), "Sousse": (35.8256, 10.6369),
    "Monastir": (35.7643, 10.8113), "Mahdia": (35.5047, 11.0622),
    "Sfax": (34.7406, 10.7603), "Kairouan": (35.6781, 10.0963),
    "Kasserine": (35.1676, 8.8365), "Sidi_Bouzid": (35.0382, 9.4849),
    "Gabes": (33.8815, 10.0982), "Medenine": (33.3549, 10.5055),
    "Tataouine": (32.9297, 10.4518), "Gafsa": (34.4250, 8.7842),
    "Tozeur": (33.9197, 8.1335), "Kebili": (33.7050, 8.9690),
}


def fetch_year(name, lat, lon, year):
    params = {
        "latitude": lat,
        "longitude": lon,
        "start_date": f"{year}-01-01",
        "end_date": f"{year}-12-31",
        "hourly": VARIABLES,
        "timezone": "Africa/Tunis",
    }
    for attempt in range(1, 6):
        response = requests.get(URL, params=params, timeout=120)
        if response.status_code == 200:
            break
        wait = 30 * attempt
        print(f"  Erreur {response.status_code}, nouvelle tentative dans {wait}s")
        time.sleep(wait)
    else:
        raise RuntimeError(f"Échec pour {name} {year}")

    df = pd.DataFrame(response.json()["hourly"])
    df["time"] = pd.to_datetime(df["time"])
    df.insert(0, "gouvernorat", name)
    df.insert(1, "latitude", lat)
    df.insert(2, "longitude", lon)
    return df


def main():
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    for name, (lat, lon) in GOUVERNORATS.items():
        for year in YEARS:
            out = RAW_DIR / f"{name}_{year}.parquet"
            if out.exists():
                continue  
            df = fetch_year(name, lat, lon, year)
            df.to_parquet(out, index=False)
            print(f"{name} {year} : {len(df)} lignes")
            time.sleep(1)  


if __name__ == "__main__":
    main()