"""
RhB Punctuality Analytics — Phase 2: ETL nach Supabase
=======================================================
Lädt die tagesaktuellen Ist-Daten von opentransportdata.swiss,
filtert auf Rhätische Bahn (RhB), berechnet Verspätungen
und schreibt die Daten in Supabase.

Verwendung:
    pip install requests pandas python-dotenv supabase
    python fetch_istdaten.py

Umgebungsvariablen (.env):
    OPENTRANSPORT_API_KEY=...
    SUPABASE_URL=https://xxxx.supabase.co
    SUPABASE_SERVICE_KEY=sb_secret_...
"""

import os
import io
import gzip
import requests
import pandas as pd
from datetime import date
from pathlib import Path
from dotenv import load_dotenv
from supabase import create_client, Client

# ── Konfiguration ────────────────────────────────────────────────────────────

load_dotenv()

API_KEY      = os.getenv("OPENTRANSPORT_API_KEY")
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_KEY")

for name, val in [("OPENTRANSPORT_API_KEY", API_KEY),
                  ("SUPABASE_URL", SUPABASE_URL),
                  ("SUPABASE_SERVICE_KEY", SUPABASE_KEY)]:
    if not val:
        raise EnvironmentError(f"{name} nicht gesetzt. Bitte .env-Datei prüfen.")

CKAN_API_BASE = "https://api.opentransportdata.swiss/ckan-api"
DATASET_ID    = "ist-daten-v2"
BETREIBER     = "RhB"
BATCH_SIZE    = 500

OUTPUT_DIR = Path("data/raw")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

HEADERS = {"Authorization": API_KEY}


# ── Schritt 1: Download-URL via CKAN-API ermitteln ───────────────────────────

def get_latest_download_url() -> str:
    url = f"{CKAN_API_BASE}/package_show?id={DATASET_ID}"
    print(f"[1/5] CKAN-Metadaten abrufen...")
    resp = requests.get(url, headers=HEADERS, timeout=30)
    resp.raise_for_status()
    resources = resp.json()["result"]["resources"]
    resources_sorted = sorted(
        resources,
        key=lambda r: r.get("last_modified") or r.get("created", ""),
        reverse=True,
    )
    latest = resources_sorted[0]
    print(f"    → Neueste Ressource: {latest.get('last_modified', 'unbekannt')}")
    return latest["url"]


# ── Schritt 2: CSV herunterladen ─────────────────────────────────────────────

def download_and_read_csv(url: str) -> pd.DataFrame:
    print(f"[2/5] Datei herunterladen (kann einige Minuten dauern)...")
    resp = requests.get(url, headers=HEADERS, timeout=300, stream=True)
    resp.raise_for_status()
    content = resp.content
    if url.endswith(".gz") or resp.headers.get("Content-Type", "").startswith("application/x-gzip"):
        content = gzip.decompress(content)
    df = pd.read_csv(io.BytesIO(content), sep=";", dtype=str, low_memory=False)
    print(f"    → {len(df):,} Zeilen geladen")
    return df


# ── Schritt 3: Filtern & Verspätung berechnen ────────────────────────────────

def filter_and_transform(df: pd.DataFrame) -> pd.DataFrame:
    print(f"[3/5] Filtern und transformieren...")
    df_rhb = df[df["BETREIBER_ABK"] == BETREIBER].copy()
    print(f"    → {len(df_rhb):,} RhB-Zeilen gefunden")

    dt_format     = "%d.%m.%Y %H:%M"
    dt_format_sec = "%d.%m.%Y %H:%M:%S"

    def parse_dt(series):
        parsed = pd.to_datetime(series, format=dt_format, errors="coerce")
        mask = parsed.isna() & series.notna()
        parsed[mask] = pd.to_datetime(series[mask], format=dt_format_sec, errors="coerce")
        return parsed

    df_rhb["ANKUNFTSZEIT_DT"] = parse_dt(df_rhb["ANKUNFTSZEIT"])
    df_rhb["AN_PROGNOSE_DT"]  = parse_dt(df_rhb["AN_PROGNOSE"])
    df_rhb["ABFAHRTSZEIT_DT"] = parse_dt(df_rhb["ABFAHRTSZEIT"])
    df_rhb["AB_PROGNOSE_DT"]  = parse_dt(df_rhb["AB_PROGNOSE"])

    df_rhb["ANKUNFT_VERSPAETUNG_MIN"] = (
        (df_rhb["AN_PROGNOSE_DT"] - df_rhb["ANKUNFTSZEIT_DT"]).dt.total_seconds() / 60
    ).round(1)
    df_rhb["ABFAHRT_VERSPAETUNG_MIN"] = (
        (df_rhb["AB_PROGNOSE_DT"] - df_rhb["ABFAHRTSZEIT_DT"]).dt.total_seconds() / 60
    ).round(1)
    df_rhb["PUENKTLICH"] = df_rhb["ABFAHRT_VERSPAETUNG_MIN"].abs() <= 3

    df_final = df_rhb[df_rhb["AB_PROGNOSE_STATUS"].isin(["REAL", "GESCHAETZT"])].copy()
    print(f"    → {len(df_final):,} Zeilen mit echten Ist-Daten")
    return df_final


# ── Schritt 4: Backup als CSV speichern ──────────────────────────────────────

def save_csv(df: pd.DataFrame, betriebstag: str) -> None:
    filename = OUTPUT_DIR / f"rhb_istdaten_{betriebstag}.csv"
    df.to_csv(filename, index=False, sep=";")
    print(f"    → CSV-Backup gespeichert: {filename}")


# ── Schritt 5: In Supabase laden ─────────────────────────────────────────────

def to_supabase(df: pd.DataFrame, supabase: Client) -> None:
    print(f"[5/5] Daten in Supabase laden ({len(df):,} Zeilen)...")

    def safe_ts(val):
        try:
            return None if pd.isna(val) else str(val)
        except (TypeError, ValueError):
            return None

    def safe_float(val):
        try:
            f = float(val)
            import math
            return None if math.isnan(f) or math.isinf(f) else round(f, 1)
        except (TypeError, ValueError):
            return None

    def safe_bool(val):
        try:
            return None if pd.isna(val) else bool(val)
        except (TypeError, ValueError):
            return None

    def safe_str(val):
        try:
            return None if pd.isna(val) else str(val)
        except (TypeError, ValueError):
            return None

    records = []
    for _, row in df.iterrows():
        try:
            btag = pd.to_datetime(row["BETRIEBSTAG"], format="%d.%m.%Y").date().isoformat()
        except Exception:
            btag = date.today().isoformat()

        records.append({
            "betriebstag":             btag,
            "fahrt_bezeichner":        safe_str(row.get("FAHRT_BEZEICHNER")),
            "betreiber_abk":           safe_str(row.get("BETREIBER_ABK")),
            "produkt_id":              safe_str(row.get("PRODUKT_ID")),
            "linien_text":             safe_str(row.get("LINIEN_TEXT")),
            "haltestellen_name":       safe_str(row.get("HALTESTELLEN_NAME")),
            "ankunftszeit":            safe_ts(row.get("ANKUNFTSZEIT_DT")),
            "an_prognose":             safe_ts(row.get("AN_PROGNOSE_DT")),
            "an_prognose_status":      safe_str(row.get("AN_PROGNOSE_STATUS")),
            "abfahrtszeit":            safe_ts(row.get("ABFAHRTSZEIT_DT")),
            "ab_prognose":             safe_ts(row.get("AB_PROGNOSE_DT")),
            "ab_prognose_status":      safe_str(row.get("AB_PROGNOSE_STATUS")),
            "ankunft_verspaetung_min": safe_float(row.get("ANKUNFT_VERSPAETUNG_MIN")),
            "abfahrt_verspaetung_min": safe_float(row.get("ABFAHRT_VERSPAETUNG_MIN")),
            "puenktlich":              safe_bool(row.get("PUENKTLICH")),
            "faellt_aus_tf":           safe_bool(row.get("FAELLT_AUS_TF") == "true"),
        })

    inserted = 0
    for i in range(0, len(records), BATCH_SIZE):
        batch = records[i:i + BATCH_SIZE]
        try:
            supabase.table("rhb_istdaten").upsert(
                batch,
                on_conflict="betriebstag,fahrt_bezeichner,haltestellen_name"
            ).execute()
            inserted += len(batch)
            print(f"    → {inserted:,}/{len(records):,} Zeilen eingefügt", end="\r")
        except Exception as e:
            print(f"\n    ⚠ Batch {i//BATCH_SIZE + 1} Fehler: {e}")

    print(f"\n    ✓ Fertig")


# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    print("=" * 55)
    print("  RhB Punctuality Analytics — ETL nach Supabase")
    print("=" * 55)

    supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

    download_url = get_latest_download_url()
    df_raw       = download_and_read_csv(download_url)
    df_final     = filter_and_transform(df_raw)

    try:
        btag = pd.to_datetime(df_final["BETRIEBSTAG"].iloc[0], format="%d.%m.%Y").date().isoformat()
    except Exception:
        btag = date.today().isoformat()

    print(f"[4/5] CSV-Backup speichern...")
    save_csv(df_final, btag)

    to_supabase(df_final, supabase)

    puenktlichkeit = df_final["PUENKTLICH"].mean() * 100
    print(f"\n  ┌─ Kennzahlen {btag} ─────────────┐")
    print(f"  │  Pünktlichkeit (≤3 Min):  {puenktlichkeit:>6.1f}%       │")
    print(f"  │  Haltestopps total:       {len(df_final):>6,}         │")
    print(f"  └─────────────────────────────────────────┘")
    print("=" * 55)


if __name__ == "__main__":
    main()