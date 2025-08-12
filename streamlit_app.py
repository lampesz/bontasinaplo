# streamlit_app.py
# Bontásinapló – vizuális, érintőbarát MVP
# Biztonság: PostgreSQL hozzáférés titkos kezelése – prioritás:
#   1) st.secrets["pg"]  (Streamlit Cloud / secrets.toml)
#   2) környezeti változók (PGHOST, PGPORT, PGDATABASE, PGUSER, PGPASSWORD)
#   3) lokális keyring (macOS Keychain / Windows Credential Manager / stb.)
#
# Indítás:
#   pip install -r requirements.txt
#   streamlit run streamlit_app.py
#
# Secrets példa (.streamlit/secrets.toml vagy Cloud Secrets):
# [pg]
# host = "192.168.1.155"
# port = 5432
# db = "cegirnyitas"
# user = "Test"
# password = "124578"
# sslmode = "prefer"   # ha kell

import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime, timedelta
from io import BytesIO
import os
import uuid
import json
import socket
import requests
import hmac, hashlib
import threading, time
from urllib.parse import quote_plus

st.set_page_config(page_title="Bontásinapló", page_icon="🥩", layout="wide")

# Opcionális: szebb felső menü
try:
    from streamlit_option_menu import option_menu
    HAS_OPT_MENU = True
except Exception:
    HAS_OPT_MENU = False

# Keyring (lokális gépen – Cloudon nem kötelező)
try:
    import keyring
    HAS_KEYRING = True
except Exception:
    HAS_KEYRING = False

# --- Alapértelmezett állatok + részek (seed) ---
SEED_ANIMALS = {
    "Sertés": [
        "Comb","Lapocka","Karaj","Tarja","Csülök (első)","Csülök (hátsó)",
        "Oldalas","Szalonna","Fej","Bőr","Csont","Belsőség","Zsiradék","Húsnyesedék"
    ],
    "Marha": [
        "Hátszín","Bélszín","Lapos hátszín","Comb","Lábszár","Lapocka",
        "Nyak","Szegy","Fej","Csont","Zsiradék","Húsnyesedék"
    ],
    "Szárnyas": [
        "Mellfilé","Felsőcomb","Alsócomb","Szárny","Hát-farhát","Aprólék",
        "Bőr","Csont","Húsnyesedék"
    ],
}

# --- DB (helyi) ---
engine = create_engine("sqlite:///bontas.db", future=True)

# táblák
with engine.begin() as conn:
    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS batches (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            datum TEXT NOT NULL,
            allat TEXT NOT NULL,
            tetel_azon TEXT,
            beszallito TEXT,
            eredet TEXT,
            ellenorzo TEXT,
            ossztomeg REAL NOT NULL,
            megjegyzes TEXT,
            status TEXT DEFAULT 'open',
            closed_at TEXT,
            pg_pushed_at TEXT,
            vir_pushed_at TEXT
        );
    """))
    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS parts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            batch_id INTEGER NOT NULL,
            resz TEXT NOT NULL,
            tomeg REAL NOT NULL,
            megjegyzes TEXT,
            FOREIGN KEY(batch_id) REFERENCES batches(id)
        );
    """))
    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS attachments (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            batch_id INTEGER NOT NULL,
            kind TEXT,
            path TEXT NOT NULL,
            mime TEXT,
            created_at TEXT NOT NULL,
            note TEXT,
            FOREIGN KEY(batch_id) REFERENCES batches(id)
        );
    """))
    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS settings (
            key TEXT PRIMARY KEY,
            value TEXT
        );
    """))
    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS sync_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            batch_id INTEGER,
            event TEXT,
            endpoint TEXT,
            payload TEXT,
            status TEXT,
            http_status INTEGER,
            response TEXT,
            created_at TEXT NOT NULL
        );
    """))
    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS pg_outbox (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            batch_id INTEGER,
            event TEXT,
            payload TEXT NOT NULL,
            status TEXT DEFAULT 'pending',    -- pending|processing|sent
            tries INTEGER DEFAULT 0,
            last_error TEXT,
            next_try_at TEXT,
            created_at TEXT NOT NULL
        );
    """))
    # Törzsadat táblák
    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS animals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL UNIQUE,
            active INTEGER NOT NULL DEFAULT 1
        );
    """))
    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS custom_parts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            animal TEXT NOT NULL,
            name TEXT NOT NULL,
            active INTEGER NOT NULL DEFAULT 1,
            UNIQUE(animal, name)
        );
    """))
    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS part_mappings (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            animal TEXT NOT NULL,
            part_name TEXT NOT NULL,
            erp_id TEXT NOT NULL,
            erp_name TEXT,
            erp_code TEXT,
            created_at TEXT NOT NULL,
            UNIQUE(animal, part_name)
        );
    """))

# --- Seed (idempotens) ---
def seed_defaults():
    with engine.begin() as conn:
        for a in SEED_ANIMALS.keys():
            conn.execute(text("INSERT OR IGNORE INTO animals(name, active) VALUES (:n, 1)"), {"n": a})
        batch_animals = [r[0] for r in conn.execute(text("SELECT DISTINCT allat FROM batches")).fetchall()]
        for a in batch_animals:
            if a:
                conn.execute(text("INSERT OR IGNORE INTO animals(name, active) VALUES (:n, 1)"), {"n": a})
        for a, parts in SEED_ANIMALS.items():
            for p in parts:
                conn.execute(text("""
                    INSERT INTO custom_parts(animal, name, active)
                    SELECT :a, :p, 1
                    WHERE NOT EXISTS (SELECT 1 FROM custom_parts WHERE animal=:a AND name=:p)
                """), {"a": a, "p": p})
seed_defaults()

# --- Settings ---
def get_setting(key: str, default: str = ""):
    try:
        with engine.begin() as conn:
            row = conn.execute(text("SELECT value FROM settings WHERE key=:k"), {"k": key}).fetchone()
            return row[0] if row and row[0] is not None else default
    except Exception:
        return default

def set_setting(key: str, value: str):
    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO settings(key, value) VALUES (:k, :v)
            ON CONFLICT(key) DO UPDATE SET value = excluded.value
        """), {"k": key, "v": str(value)})

# --- Állatok kezelése ---
def get_animals(only_active: bool = True) -> pd.DataFrame:
    q = "SELECT name, active FROM animals"
    if only_active:
        q += " WHERE active = 1"
    q += " ORDER BY name"
    try:
        return pd.read_sql(q, engine)
    except Exception:
        return pd.DataFrame(columns=["name", "active"])

def add_animal(name: str):
    name = (name or "").strip()
    if not name:
        return False, "Üres név"
    with engine.begin() as conn:
        try:
            res = conn.execute(text("INSERT OR IGNORE INTO animals(name, active) VALUES (:n, 1)"), {"n": name})
            if getattr(res, "rowcount", 0) == 0:
                return True, "Már létezett (kihagyva)"
            return True, "Hozzáadva"
        except Exception as e:
            return False, str(e)

def set_animal_active(name: str, active: bool):
    with engine.begin() as conn:
        conn.execute(text("UPDATE animals SET active=:a WHERE name=:n"), {"a": 1 if active else 0, "n": name})

def rename_animal(old_name: str, new_name: str, propagate_batches: bool = False):
    new_name = (new_name or "").strip()
    if not new_name:
        return False, "Üres új név"
    with engine.begin() as conn:
        try:
            conn.execute(text("UPDATE animals SET name=:new WHERE name=:old"), {"new": new_name, "old": old_name})
            conn.execute(text("UPDATE custom_parts SET animal=:new WHERE animal=:old"), {"new": new_name, "old": old_name})
            conn.execute(text("UPDATE part_mappings SET animal=:new WHERE animal=:old"), {"new": new_name, "old": old_name})
            if propagate_batches:
                conn.execute(text("UPDATE batches SET allat=:new WHERE allat=:old"), {"new": new_name, "old": old_name})
            return True, "Átnevezve"
        except Exception as e:
            return False, str(e)

def copy_parts_from_animal(src_animal: str, dst_animal: str, include_inactive: bool = False) -> int:
    filt = "" if include_inactive else " AND active = 1"
    rows = pd.read_sql("SELECT name, active FROM custom_parts WHERE animal = :a" + filt, engine, params={"a": src_animal})
    inserted = 0
    with engine.begin() as conn:
        for _, r in rows.iterrows():
            try:
                conn.execute(text("""
                    INSERT INTO custom_parts(animal, name, active)
                    SELECT :a, :n, :act
                    WHERE NOT EXISTS (SELECT 1 FROM custom_parts WHERE animal=:a AND name=:n)
                """), {"a": dst_animal, "n": r["name"], "act": int(r["active"])})
                inserted += 1
            except Exception:
                pass
    return inserted

# --- Részek + mapping ---
def get_custom_parts(animal: str, only_active: bool = True) -> pd.DataFrame:
    try:
        q = "SELECT name, active FROM custom_parts WHERE animal = ?"
        if only_active:
            q += " AND active = 1"
        q += " ORDER BY name"
        return pd.read_sql(q, engine, params=(animal,))
    except Exception:
        return pd.DataFrame(columns=["name", "active"])

def add_custom_part(animal: str, name: str):
    name = (name or "").strip()
    if not name:
        return False, "Üres név"
    with engine.begin() as conn:
        try:
            conn.execute(text("INSERT INTO custom_parts(animal, name, active) VALUES (:a, :n, 1)"),
                         {"a": animal, "n": name})
            return True, "Hozzáadva"
        except Exception as e:
            return False, str(e)

def rename_custom_part(animal: str, old_name: str, new_name: str):
    new_name = (new_name or "").strip()
    if not new_name:
        return False, "Üres új név"
    with engine.begin() as conn:
        try:
            conn.execute(text("UPDATE custom_parts SET name=:new WHERE animal=:a AND name=:old"),
                         {"new": new_name, "a": animal, "old": old_name})
            conn.execute(text("UPDATE part_mappings SET part_name=:new WHERE animal=:a AND part_name=:old"),
                         {"new": new_name, "a": animal, "old": old_name})
            return True, "Átnevezve"
        except Exception as e:
            return False, str(e)

def deactivate_custom_part(animal: str, name: str):
    with engine.begin() as conn:
        try:
            conn.execute(text("UPDATE custom_parts SET active=0 WHERE animal=:a AND name=:n"),
                         {"a": animal, "n": name})
            return True, "Deaktiválva"
        except Exception as e:
            return False, str(e)

def get_all_parts(animal: str) -> list:
    df = get_custom_parts(animal, only_active=True)
    return df["name"].tolist() if not df.empty else []

def get_mappings(animal: str = None) -> pd.DataFrame:
    try:
        if animal:
            return pd.read_sql(
                "SELECT animal, part_name, erp_id, erp_name, erp_code, created_at FROM part_mappings WHERE animal = ? ORDER BY part_name",
                engine, params=(animal,))
        return pd.read_sql(
            "SELECT animal, part_name, erp_id, erp_name, erp_code, created_at FROM part_mappings ORDER BY animal, part_name",
            engine)
    except Exception:
        return pd.DataFrame(columns=["animal","part_name","erp_id","erp_name","erp_code","created_at"])

def upsert_mapping(animal: str, part_name: str, erp_id: str, erp_name: str = None, erp_code: str = None):
    ts = datetime.now().isoformat(timespec="seconds")
    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO part_mappings(animal, part_name, erp_id, erp_name, erp_code, created_at)
            VALUES (:a, :p, :eid, :enm, :ecode, :ts)
            ON CONFLICT(animal, part_name)
            DO UPDATE SET erp_id = excluded.erp_id,
                          erp_name = excluded.erp_name,
                          erp_code = excluded.erp_code,
                          created_at = excluded.created_at
        """), {"a": animal, "p": part_name, "eid": str(erp_id), "enm": erp_name, "ecode": erp_code, "ts": ts})

def delete_mapping(animal: str, part_name: str):
    with engine.begin() as conn:
        conn.execute(text("DELETE FROM part_mappings WHERE animal=:a AND part_name=:p"), {"a": animal, "p": part_name})

# --- VIR config + payload ---
def get_vir_config():
    url = get_setting("vir_url", "")
    api_key = get_setting("vir_api_key", "")
    secret = get_setting("vir_secret", "")
    auto = get_setting("vir_auto_send", "0") == "1"
    return url, api_key, secret, auto

def set_vir_config(url: str, api_key: str, secret: str, auto: bool):
    set_setting("vir_url", url or "")
    set_setting("vir_api_key", api_key or "")
    set_setting("vir_secret", secret or "")
    set_setting("vir_auto_send", "1" if auto else "0")

# --- PG DSN építés / titkok kezelése ---

def _pg_key_id(host: str, port: int, db: str, user: str) -> str:
    return f"{user}@{host}:{port}/{db}"

def build_pg_dsn(host: str, port: int, db: str, user: str, password: str = None, sslmode: str = "prefer") -> str:
    if not (host and db and user):
        return ""
    pw = quote_plus(password or "")
    return f"postgresql+psycopg2://{user}:{pw}@{host}:{int(port)}/{db}"

def get_pg_conn_fields():
    """Kapcsolati mezők forrása prioritással:
       1) st.secrets['pg'] (Cloud/prod)
       2) env (PGHOST, PGPORT, PGDATABASE, PGUSER, PGPASSWORD)
       3) settings + keyring (lokál)
    """
    # 1) Secrets
    if "pg" in st.secrets:
        s = st.secrets["pg"]
        return {
            "host": s.get("host", ""),
            "port": int(s.get("port", 5432) or 5432),
            "db":   s.get("db", ""),
            "user": s.get("user", ""),
            "sslmode": s.get("sslmode", "prefer"),
            "password_saved": bool(s.get("password", "")),
            "password_source": "secrets",
        }

    # 2) Env
    host = os.getenv("PGHOST", get_setting("pg_host", ""))
    port = int(os.getenv("PGPORT", get_setting("pg_port", "5432") or 5432))
    db   = os.getenv("PGDATABASE", get_setting("pg_db", ""))
    user = os.getenv("PGUSER", get_setting("pg_user", ""))
    sslmode = os.getenv("PGSSLMODE", get_setting("pg_sslmode", "prefer"))
    pw_env = os.getenv("PGPASSWORD") or os.getenv("PG_PASSWORD")
    if pw_env:
        return {
            "host": host, "port": port, "db": db, "user": user,
            "sslmode": sslmode, "password_saved": True, "password_source": "env",
            "password_env": True
        }

    # 3) Settings + (opcionális) keyring
    saved = False
    if HAS_KEYRING and host and db and user:
        try:
            saved = keyring.get_password("Bontasinaplo", _pg_key_id(host, port, db, user)) is not None
        except Exception:
            saved = False
    return {
        "host": host, "port": port, "db": db, "user": user,
        "sslmode": sslmode, "password_saved": saved, "password_source": "settings",
    }

def set_pg_conn_fields(host: str, port: int, db: str, user: str, password: str = None, sslmode: str = "prefer"):
    # Cloudon/secrets esetén ne mentsünk semmit – ott a Secrets az igazság
    if "pg" in st.secrets:
        st.info("A PG kapcsolatot jelenleg a Streamlit Secrets adja. Itt nem mentek el semmit.")
        return
    set_setting("pg_host", host or "")
    set_setting("pg_port", str(int(port or 5432)))
    set_setting("pg_db", db or "")
    set_setting("pg_user", user or "")
    set_setting("pg_sslmode", sslmode or "prefer")
    set_setting("pg_dsn", "")  # régi plain DSN törlése
    # jelszó csak lokálisan keyringbe
    if password and password.strip():
        if HAS_KEYRING:
            try:
                keyring.set_password("Bontasinaplo", _pg_key_id(host, int(port or 5432), db, user), password.strip())
            except Exception as e:
                st.warning(f"Nem sikerült a jelszót kulcstárba menteni: {e}")
        else:
            st.warning("A keyring nincs telepítve – a jelszót nem tudom biztonságosan elmenteni. Telepítsd: pip install keyring")

def clear_pg_saved_password(host: str, port: int, db: str, user: str):
    if HAS_KEYRING and host and db and user:
        try:
            keyring.delete_password("Bontasinaplo", _pg_key_id(host, int(port or 5432), db, user))
            st.success("Mentett jelszó törölve a kulcstárból.")
        except Exception as e:
            st.warning(f"Jelszó törlés hiba: {e}")

def set_pg_config_compat(auto_send: bool, interval: int):
    set_setting("pg_auto_send", "1" if auto_send else "0")
    set_setting("pg_auto_interval", str(int(interval or 60)))

def get_pg_config():
    """Építs DSN-t a prioritás szerint, és add vissza az outbox beállításokat."""
    f = get_pg_conn_fields()
    # Password forrás
    pw = None
    if f.get("password_source") == "secrets":
        pw = st.secrets["pg"].get("password")
    if not pw:
        pw = os.getenv("PGPASSWORD") or os.getenv("PG_PASSWORD")
    if not pw and HAS_KEYRING and f["host"] and f["db"] and f["user"]:
        try:
            pw = keyring.get_password("Bontasinaplo", _pg_key_id(f["host"], f["port"], f["db"], f["user"]))
        except Exception:
            pw = None
    dsn = build_pg_dsn(f["host"], f["port"], f["db"], f["user"], pw, f["sslmode"])
    if not dsn:
        dsn = get_setting("pg_dsn", "")  # legacy
    auto_send = get_setting("pg_auto_send", "0") == "1"
    try:
        interval = int(get_setting("pg_auto_interval", "60") or 60)
    except Exception:
        interval = 60
    return dsn, auto_send, interval

def test_pg_connection(dsn: str):
    if not dsn:
        return False, "Üres DSN"
    try:
        eng = create_engine(dsn, future=True, pool_pre_ping=True)
        with eng.begin() as conn:
            conn.execute(text("SELECT 1"))
        return True, "OK"
    except Exception as e:
        return False, str(e)

def pg_insert_payload(dsn: str, payload_json: str):
    eng = create_engine(dsn, future=True, pool_pre_ping=True)
    with eng.begin() as conn:
        conn.execute(text("INSERT INTO api.api_bontasi_naplo(adat) VALUES (:adat)"), {"adat": payload_json})

def queue_pg_payload(payload, batch_id: int, event: str, dsn: str = None, auto: bool = True):
    """Outbox queue. Dedup CSAK a 'batch_closed' eseményre (hogy ne duplázzon lezárást).
       Inkrementális 'part_created' eseményeket NEM dedupoljuk."""
    payload_json = payload if isinstance(payload, str) else json.dumps(payload, ensure_ascii=False)
    now = datetime.now().isoformat(timespec="seconds")
    if event == "batch_closed":
        with engine.begin() as conn:
            exists = conn.execute(text(
                "SELECT id FROM pg_outbox WHERE batch_id=:b AND event='batch_closed' "
                "AND status IN ('pending','processing','sent') LIMIT 1"
            ), {"b": batch_id}).fetchone()
        if exists:
            return
    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO pg_outbox(batch_id, event, payload, status, tries, created_at)
            VALUES (:batch_id, :event, :payload, 'pending', 0, :created_at)
        """), {"batch_id": batch_id, "event": event, "payload": payload_json, "created_at": now})
    if dsn:
        flush_pg_outbox(dsn, max_items=10)

def _backoff_seconds(tries: int) -> int:
    return min(3600, int(30 * (2 ** max(0, tries))))

FLUSH_LOCK = threading.Lock()

def flush_pg_outbox(dsn: str, max_items: int = 50) -> bool:
    ok, _ = test_pg_connection(dsn)
    if not ok:
        return False
    acquired = FLUSH_LOCK.acquire(timeout=5)
    try:
        now_iso = datetime.now().isoformat(timespec="seconds")
        with engine.begin() as conn:
            rows = conn.execute(text("""
                SELECT id, payload, tries FROM pg_outbox
                WHERE status='pending' AND (next_try_at IS NULL OR next_try_at <= :now)
                ORDER BY id ASC LIMIT :lim
            """), {"now": now_iso, "lim": max_items}).fetchall()
        if not rows: return True
        for r in rows:
            rid = int(r[0]); payload_json = r[1]; tries = int(r[2] or 0)
            with engine.begin() as conn:
                res = conn.execute(text("UPDATE pg_outbox SET status='processing' WHERE id=:id AND status='pending'"),
                                   {"id": rid})
                if getattr(res, "rowcount", 0) == 0:
                    continue
            try:
                pg_insert_payload(dsn, payload_json)
                with engine.begin() as conn:
                    conn.execute(text("""
                        UPDATE pg_outbox SET status='sent', tries=:t, last_error=NULL, next_try_at=NULL WHERE id=:id
                    """), {"t": tries + 1, "id": rid})
            except Exception as e:
                nxt = (datetime.now() + timedelta(seconds=_backoff_seconds(tries))).isoformat(timespec="seconds")
                with engine.begin() as conn:
                    conn.execute(text("""
                        UPDATE pg_outbox SET status='pending', tries=:t, last_error=:err, next_try_at=:nxt WHERE id=:id
                    """), {"t": tries + 1, "err": str(e), "nxt": nxt, "id": rid})
        return True
    finally:
        if acquired:
            FLUSH_LOCK.release()

def get_pg_outbox_stats():
    try:
        df = pd.read_sql("SELECT status, COUNT(*) cnt FROM pg_outbox GROUP BY status", engine)
        m = {row["status"]: int(row["cnt"]) for _, row in df.iterrows()}
        return {"pending": m.get("pending", 0), "processing": m.get("processing", 0), "sent": m.get("sent", 0)}
    except Exception:
        return {"pending": 0, "processing": 0, "sent": 0}

_BG_STARTED = False
def start_pg_bg_flusher():
    global _BG_STARTED
    if _BG_STARTED:
        return
    _BG_STARTED = True
    def _loop():
        while True:
            try:
                dsn, _auto, interval = get_pg_config()
                if dsn:
                    flush_pg_outbox(dsn, max_items=50)
                time.sleep(max(5, int(interval)))
            except Exception:
                time.sleep(30)
    threading.Thread(target=_loop, daemon=True).start()
start_pg_bg_flusher()

# --- Batch státusz + payloadok (ERP adatokkal) ---
def get_batch_status(batch_id: int) -> str:
    with engine.begin() as conn:
        row = conn.execute(text("SELECT status FROM batches WHERE id=:id"), {"id": batch_id}).fetchone()
    return (row[0] if row else "open") or "open"

def close_batch(batch_id: int) -> bool:
    if get_batch_status(batch_id) == "closed":
        return False
    with engine.begin() as conn:
        conn.execute(text("UPDATE batches SET status='closed', closed_at=:ts WHERE id=:id"),
                     {"ts": datetime.now().isoformat(timespec="seconds"), "id": batch_id})
    return True

def reopen_batch(batch_id: int) -> bool:
    if get_batch_status(batch_id) != "closed":
        return False
    with engine.begin() as conn:
        conn.execute(text("UPDATE batches SET status='open', closed_at=NULL WHERE id=:id"), {"id": batch_id})
    return True

def build_full_batch_payload(batch_row, parts_df):
    try:
        osszeg = float(parts_df['tomeg'].sum()) if parts_df is not None and not parts_df.empty else 0.0
    except Exception:
        osszeg = 0.0
    be = float(batch_row["ossztomeg"]) if "ossztomeg" in batch_row else float(batch_row.get("ossztomeg", 0.0))
    hozam = (osszeg / be * 100.0) if be > 0 else 0.0
    diff = be - osszeg
    animal_name = str(batch_row["allat"]) if "allat" in batch_row else str(batch_row.get("allat"))
    try:
        map_df = pd.read_sql(
            "SELECT part_name, erp_id, erp_name, erp_code FROM part_mappings WHERE animal = ?",
            engine, params=(animal_name,))
        mapping = {
            row["part_name"]: {
                "erp_id": str(row["erp_id"]) if pd.notna(row["erp_id"]) else None,
                "erp_name": (row["erp_name"] if "erp_name" in row and pd.notna(row["erp_name"]) else None),
                "erp_code": (row["erp_code"] if "erp_code" in row and pd.notna(row["erp_code"]) else None),
            }
            for _, row in map_df.iterrows()
        } if map_df is not None and not map_df.empty else {}
    except Exception:
        mapping = {}
    parts = []
    if parts_df is not None and not parts_df.empty:
        for r in parts_df.to_dict("records"):
            pname = r.get("resz")
            pobj = {"name": pname, "weight_kg": float(r.get("tomeg", 0.0)), "note": r.get("megjegyzes")}
            m = mapping.get(pname)
            if m:
                pobj["tetel_id"] = m["erp_id"]
                pobj["erp_id"] = m["erp_id"]
                if m.get("erp_code") is not None: pobj["erp_code"] = m["erp_code"]
                if m.get("erp_name") is not None: pobj["erp_name"] = m["erp_name"]
            parts.append(pobj)
    return {
        "source": {"system": "Bontasinaplo", "device": socket.gethostname()},
        "event": "batch_closed",
        "batch": {
            "id": int(batch_row["id"]) if "id" in batch_row else int(batch_row.get("id", 0)),
            "date": str(batch_row["datum"]) if "datum" in batch_row else str(batch_row.get("datum")),
            "animal": animal_name,
            "gross_weight_kg": be,
            "lot": (batch_row["tetel_azon"] if "tetel_azon" in batch_row else batch_row.get("tetel_azon")),
            "supplier": (batch_row["beszallito"] if "beszallito" in batch_row else batch_row.get("beszallito")),
            "origin": (batch_row["eredet"] if "eredet" in batch_row else batch_row.get("eredet")),
            "inspector": (batch_row["ellenorzo"] if "ellenorzo" in batch_row else batch_row.get("ellenorzo")),
            "note": (batch_row["megjegyzes"] if "megjegyzes" in batch_row else batch_row.get("megjegyzes")),
            "closed_at": (batch_row.get("closed_at") if isinstance(batch_row, dict) else None),
        },
        "parts": parts,
        "summary": {"sum_kg": osszeg, "yield_pct": hozam, "diff_kg": diff}
    }

def build_part_payload(batch_row, part_dict):
    animal_name = str(batch_row["allat"]) if "allat" in batch_row else str(batch_row.get("allat"))
    pname = part_dict.get("resz")
    tetel_id = None; erp_code = None; erp_name = None
    try:
        mdf = pd.read_sql(
            "SELECT erp_id, erp_name, erp_code FROM part_mappings WHERE animal = ? AND part_name = ? LIMIT 1",
            engine, params=(animal_name, pname))
        if mdf is not None and not mdf.empty:
            row = mdf.iloc[0]
            tetel_id = str(row["erp_id"]) if pd.notna(row["erp_id"]) else None
            erp_code = row["erp_code"] if ("erp_code" in row and pd.notna(row["erp_code"])) else None
            erp_name = row["erp_name"] if ("erp_name" in row and pd.notna(row["erp_name"])) else None
    except Exception:
        pass
    part_obj = {"name": pname, "weight_kg": float(part_dict.get("tomeg", 0.0)), "note": part_dict.get("megjegyzes")}
    if tetel_id is not None:
        part_obj["tetel_id"] = tetel_id
        part_obj["erp_id"] = tetel_id
    if erp_code is not None: part_obj["erp_code"] = erp_code
    if erp_name is not None: part_obj["erp_name"] = erp_name
    return {
        "source": {"system": "Bontasinaplo", "device": socket.gethostname()},
        "event": "part_created",
        "batch": {
            "id": int(batch_row["id"]) if "id" in batch_row else int(batch_row.get("id", 0)),
            "date": str(batch_row["datum"]) if "datum" in batch_row else str(batch_row.get("datum")),
            "animal": animal_name,
        },
        "part": part_obj
    }

# --- Log + HMAC + VIR ---
def log_sync(batch_id, event, endpoint, payload, status, http_status=None, response=None):
    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO sync_log(batch_id, event, endpoint, payload, status, http_status, response, created_at)
            VALUES (:batch_id, :event, :endpoint, :payload, :status, :http_status, :response, :created_at)
        """), {
            "batch_id": batch_id,
            "event": event,
            "endpoint": endpoint,
            "payload": payload if isinstance(payload, str) else json.dumps(payload, ensure_ascii=False),
            "status": status,
            "http_status": http_status,
            "response": response,
            "created_at": datetime.now().isoformat(timespec="seconds"),
        })

def hmac_sign(secret: str, body: bytes) -> str:
    return hmac.new(secret.encode("utf-8"), body, hashlib.sha256).hexdigest()

def send_json_to_vir(payload, url: str, api_key: str = None, secret: str = None, event: str = None, batch_id: int = None) -> bool:
    body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    headers = {"Content-Type": "application/json"}
    if api_key: headers["X-API-Key"] = api_key
    if secret:  headers["X-Signature"] = hmac_sign(secret, body)
    try:
        r = requests.post(url, data=body, headers=headers, timeout=10)
        ok = 200 <= r.status_code < 300
        log_sync(batch_id, event or "push", url, body.decode("utf-8"), "ok" if ok else "error", r.status_code, r.text)
        return ok
    except Exception as e:
        log_sync(batch_id, event or "push", url, body.decode("utf-8"), "error", None, str(e))
        return False

# --- Adat-hozzáférés ---
def get_batches():
    return pd.read_sql("SELECT * FROM batches ORDER BY id DESC", engine)

def get_parts(batch_id: int):
    return pd.read_sql("SELECT id, resz, tomeg, megjegyzes FROM parts WHERE batch_id = ? ORDER BY id DESC",
                       engine, params=(batch_id,))

def save_batch(data: dict) -> int:
    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO batches(datum, allat, tetel_azon, beszallito, eredet, ellenorzo, ossztomeg, megjegyzes)
            VALUES (:datum, :allat, :tetel_azon, :beszallito, :eredet, :ellenorzo, :ossztomeg, :megjegyzes)
        """), data)
        new_id = conn.execute(text("SELECT last_insert_rowid()")).scalar_one()
    return int(new_id)

def save_part(batch_id: int, resz: str, tomeg: float, megjegyzes: str = ""):
    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO parts(batch_id, resz, tomeg, megjegyzes)
            VALUES (:batch_id, :resz, :tomeg, :megjegyzes)
        """), {"batch_id": batch_id, "resz": resz, "tomeg": tomeg, "megjegyzes": megjegyzes})

def delete_part(part_id: int) -> bool:
    with engine.begin() as conn:
        row = conn.execute(text("SELECT batch_id FROM parts WHERE id = :id"), {"id": part_id}).fetchone()
        if not row:
            return False
        bid = int(row[0])
        st_row = conn.execute(text("SELECT status FROM batches WHERE id=:id"), {"id": bid}).fetchone()
        if st_row and (st_row[0] or "open") == "closed":
            return False
        conn.execute(text("DELETE FROM parts WHERE id = :id"), {"id": part_id})
    return True

# --- Mellékletek ---
def get_attachments(batch_id: int):
    return pd.read_sql(
        "SELECT id, kind, path, mime, created_at, note FROM attachments WHERE batch_id = ? ORDER BY id DESC",
        engine, params=(batch_id,)
    )

def save_attachment(batch_id: int, file_bytes: bytes, filename: str, mime: str, kind: str = "szallitolevel", note: str = ""):
    os.makedirs("docs", exist_ok=True)
    if filename and "." in filename:
        ext = "." + filename.rsplit(".", 1)[1].lower()
    else:
        ext = ".jpg" if (mime and str(mime).startswith("image/")) else (".pdf" if mime == "application/pdf" else ".bin")
    unique = f"{batch_id}_{uuid.uuid4().hex}{ext}"
    path = os.path.join("docs", unique)
    with open(path, "wb") as f:
        f.write(file_bytes)
    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO attachments(batch_id, kind, path, mime, created_at, note)
            VALUES (:batch_id, :kind, :path, :mime, :created_at, :note)
        """), {
            "batch_id": batch_id, "kind": kind, "path": path, "mime": mime,
            "created_at": datetime.now().isoformat(timespec="seconds"), "note": note
        })
    return path

# --- ERP betöltés (PG) ---
@st.cache_data(ttl=300, show_spinner=False)
def load_erp_items(dsn: str, ceg_id: int = 3) -> pd.DataFrame:
    eng = create_engine(dsn, future=True, pool_pre_ping=True)
    df = pd.read_sql(text("SELECT * FROM alapadat.tetel WHERE ceg_id = :c"), eng, params={"c": int(ceg_id)})
    return df

def pick_erp_columns(df: pd.DataFrame):
    cols = list(df.columns)
    id_col = next((c for c in ["id","tetel_id"] if c in cols), cols[0] if cols else None)
    name_col = next((c for c in ["megnevezes","nev","leiras","megnevezes_hu"] if c in cols), cols[1] if len(cols)>1 else cols[0])
    code_col = next((c for c in ["cikkszam","kod","sku","termekkod"] if c in cols), None)
    return id_col, name_col, code_col

# --- PDF export ---
try:
    from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer, Image as RLImage
    from reportlab.lib import colors
    from reportlab.lib.pagesizes import A4
    from reportlab.lib.styles import getSampleStyleSheet
    from reportlab.lib.units import mm
    REPORTLAB_AVAILABLE = True
except Exception:
    REPORTLAB_AVAILABLE = False

def build_pdf(batch_row, parts_df, attachments_df):
    if not REPORTLAB_AVAILABLE:
        return None
    buf = BytesIO()
    doc = SimpleDocTemplate(buf, pagesize=A4, leftMargin=15*mm, rightMargin=15*mm, topMargin=15*mm, bottomMargin=15*mm)
    styles = getSampleStyleSheet()
    elems = []
    elems.append(Paragraph("Bontásinapló – tétel jelentés", styles['Title']))
    elems.append(Spacer(1, 8))
    meta = [
        f"Dátum: {batch_row['datum']}",
        f"Állat: {batch_row['allat']}",
        f"Beérkezett tömeg: {batch_row['ossztomeg']:.2f} kg",
        f"Tétel: {batch_row['tetel_azon'] or '-'}",
        f"Beszállító: {batch_row['beszallito'] or '-'}",
        f"Ellenőrizte: {batch_row['ellenorzo'] or '-'}",
    ]
    for m in meta:
        elems.append(Paragraph(m, styles['Normal']))
    elems.append(Spacer(1, 8))

    data = [["Rész","Tömeg (kg)","Megjegyzés"]]
    for _, r in parts_df.iterrows():
        data.append([r['resz'], f"{r['tomeg']:.2f}", r.get('megjegyzes') or ""])
    t = Table(data, colWidths=[80*mm, 30*mm, 60*mm])
    t.setStyle(TableStyle([
        ('GRID',(0,0),(-1,-1),0.5,colors.black),
        ('BACKGROUND',(0,0),(-1,0),colors.lightgrey),
        ('ALIGN',(1,1),(1,-1),'RIGHT'),
        ('VALIGN',(0,0),(-1,-1),'MIDDLE'),
        ('FONTNAME',(0,0),(-1,0),'Helvetica-Bold'),
    ]))
    elems.append(t)
    elems.append(Spacer(1, 6))

    osszeg = float(parts_df['tomeg'].sum()) if not parts_df.empty else 0.0
    be = float(batch_row['ossztomeg'])
    kulonbseg = be - osszeg
    hozam = (osszeg / be * 100.0) if be > 0 else 0.0
    elems.append(Paragraph(f"Rögzített részek összege: {osszeg:.2f} kg", styles['Normal']))
    elems.append(Paragraph(f"Hozam: {hozam:.1f} %", styles['Normal']))
    elems.append(Paragraph(f"Különbözet: {kulonbseg:.2f} kg", styles['Normal']))
    elems.append(Spacer(1, 10))

    if attachments_df is not None and not attachments_df.empty:
        img_row = None
        for _, a in attachments_df.iterrows():
            if a['mime'] and str(a['mime']).startswith('image/'):
                img_row = a
                break
        if img_row is not None:
            try:
                elems.append(Paragraph("Melléklet: Szállítólevél fotó", styles['Heading3']))
                img = RLImage(img_row['path'])
                img._restrictSize(170*mm, 120*mm)
                elems.append(img)
            except Exception:
                pass

    doc.build(elems)
    buf.seek(0)
    return buf

# --- UI stílus ---
st.markdown("""
<style>
html, body, [class*="css"] { font-size: 18px; }
input, select, textarea { font-size: 1.1rem !important; }
.stButton>button { padding: 1rem 1.2rem; font-size: 1.1rem; border-radius: 14px; }
:root { --card-pad: 1rem; }
.block { padding: var(--card-pad); background: #fff; border-radius: 14px; box-shadow: 0 2px 10px rgba(0,0,0,.05); }
.metric { text-align:center; padding:.5rem; border-radius:12px; background:#f6f7fb; }
.lcd { font-weight:700; font-variant-numeric: tabular-nums; text-align:right; padding:.75rem 1rem; font-size:2rem; background:#0b132b; color:#fff; border-radius:12px; margin:.5rem 0 1rem; display:block; }
.tile { display:block; padding:1rem; border:2px solid #e5e7eb; border-radius:14px; text-align:center; margin-bottom:.5rem; user-select:none; }
.tile.active { border-color:#3b82f6; background:#eff6ff; }
</style>
""", unsafe_allow_html=True)

# --- Oldalak (Settings + Mappings + Rögzítés) ---
def render_settings_page():
    st.header("Beállítások / Integrációk")

    # VIR
    st.subheader("VIR integráció (küldés JSON-ban)")
    vir_url = st.text_input("VIR endpoint URL", value=(get_setting("vir_url","") or os.environ.get("VIR_URL","")))
    vir_api_key = st.text_input("API kulcs (opcionális)", value=(get_setting("vir_api_key","") or os.environ.get("VIR_API_KEY","")), type="password")
    vir_secret = st.text_input("Aláíró titok HMAC-SHA256 (opcionális)", value=(get_setting("vir_secret","") or os.environ.get("VIR_SECRET","")), type="password")
    vir_auto = st.checkbox("Auto-küldés mentéskor (rész hozzáadásakor)", value=(get_setting("vir_auto_send","0")=="1"))
    if st.button("VIR beállítások mentése", key="vir_save_btn"):
        set_vir_config(vir_url, vir_api_key, vir_secret, vir_auto)
        st.success("VIR beállítások mentve.")

    st.markdown("---")

    # PostgreSQL – Secrets/Env/Keyring
    st.subheader("PostgreSQL integráció (JSON → api.api_bontasi_naplo)")
    f = get_pg_conn_fields()
    if f.get("password_source") == "secrets":
        st.info("A PG kapcsolat **Streamlit Secrets**-ből jön (ajánlott Cloudon). A mezők csak teszthez/infóhoz vannak.")
    elif os.getenv("PGPASSWORD") or os.getenv("PG_PASSWORD"):
        st.info("A PG jelszó **környezeti változóból** jön.")
    elif HAS_KEYRING:
        st.caption("Lokális gépen a jelszó a kulcstárban lehet elmentve (Keychain/Keyring).")

    c1, c2, c3, c4 = st.columns([1,1,1,1])
    with c1:
        host = st.text_input("Host", value=f["host"], key="pg_host")
    with c2:
        port = st.number_input("Port", min_value=1, value=int(f["port"] or 5432), step=1, key="pg_port")
    with c3:
        db = st.text_input("Adatbázis", value=f["db"], key="pg_db")
    with c4:
        user = st.text_input("Felhasználó", value=f["user"], key="pg_user")

    # Placeholder a forrástól függően
    if f.get("password_source") == "secrets":
        pw_placeholder = "(jelszó: Streamlit Secrets)"
    elif os.getenv("PGPASSWORD") or os.getenv("PG_PASSWORD"):
        pw_placeholder = "(jelszó: környezeti változó)"
    else:
        pw_placeholder = "(mentve a kulcstárban)" if f["password_saved"] else ""

    password_input = st.text_input("Jelszó", type="password", value="", placeholder=pw_placeholder, key="pg_password")

    colpg1, colpg2, colpg3, colpg4 = st.columns([1,1,1,1])
    with colpg1:
        if st.button("Kapcsolat teszt (PG)", key="pg_test_btn"):
            # build DSN a fenti prioritással
            pw = None
            if f.get("password_source") == "secrets":
                pw = st.secrets["pg"].get("password")
            if not pw:
                pw = password_input.strip() if password_input else None
            if not pw:
                pw = os.getenv("PGPASSWORD") or os.getenv("PG_PASSWORD")
            if not pw and HAS_KEYRING and host and db and user:
                try:
                    pw = keyring.get_password("Bontasinaplo", _pg_key_id(host, int(port or 5432), db, user))
                except Exception:
                    pw = None
            dsn_try = build_pg_dsn(host, port, db, user, pw)
            ok, msg = test_pg_connection(dsn_try)
            if ok: st.success("PG kapcsolat OK")
            else:  st.error(msg)
    with colpg2:
        pg_auto_send = st.checkbox("Auto-küldés mentéskor (PG)", value=(get_setting("pg_auto_send","0")=="1"), key="pg_auto_send_cb")
    with colpg3:
        pg_interval = st.number_input("Háttérküldés (mp)", min_value=10, step=10, value=int(get_setting("pg_auto_interval","60") or 60), key="pg_interval_num")
    with colpg4:
        if st.button("PG beállítások mentése", key="pg_save_btn"):
            set_pg_conn_fields(host, port, db, user, password_input, sslmode="prefer")
            set_pg_config_compat(pg_auto_send, int(pg_interval))
            st.success("PG beállítások mentve.")

    # Régi (legacy) DSN törlés gomb – ha korábban elmentve
    legacy_dsn = get_setting("pg_dsn", "")
    colld1, colld2 = st.columns([1,3])
    if legacy_dsn:
        with colld1:
            if st.button("Régi DSN törlése a settings-ből", key="wipe_legacy_dsn_btn"):
                set_setting("pg_dsn", "")
                st.success("Régi DSN törölve a beállításokból.")
        with colld2:
            st.caption("Biztonság: a jelszót mostantól Secrets/env/keyring kezeli. A teljes DSN-t nem tároljuk/plaintext nem jelenítjük meg.")

    stats = get_pg_outbox_stats()
    colpf1, colpf2 = st.columns([1,1])
    with colpf1:
        if st.button("Sor kiürítése most (Flush)", key="pg_flush_btn"):
            dsn, _, _ = get_pg_config()
            if not dsn:
                st.warning("Előbb mentsd a PG kapcsolatot (vagy állítsd be a Secrets-et).")
            else:
                flush_pg_outbox(dsn, max_items=200)
                st.success("Flush lefutott.")
    with colpf2:
        st.write(f"🕒 Sorban: {stats['pending']} | 🔄 Processing: {stats['processing']} | ✅ Elküldve: {stats['sent']}")

    st.subheader("Küldési napló (utolsó 20)")
    try:
        log_df = pd.read_sql(
            "SELECT id, created_at, event, endpoint, http_status, status FROM sync_log ORDER BY id DESC LIMIT 20", engine)
        st.dataframe(log_df, use_container_width=True)
    except Exception:
        st.info("Még nincs küldési napló.")

def render_mappings_page():
    st.header("Törzsadatok és kapcsolások")

    # Állatok
    st.subheader("Állatok kezelése")
    df_anim = get_animals(only_active=False)
    if df_anim.empty:
        st.info("Még nincs állat rögzítve. Hozz létre lent egyet!")
    else:
        st.dataframe(df_anim.rename(columns={"name":"Állat","active":"Aktív"}), use_container_width=True, height=180)

    colA1, colA2, colA3 = st.columns([1,1,1])
    with colA1:
        new_an = st.text_input("Új állat neve", key="new_animal_txt")
        tmpl_list = df_anim[df_anim["active"]==1]["name"].tolist() if not df_anim.empty else []
        tmpl = st.selectbox("Részek másolása (opcionális)", options=["(nincs)"] + tmpl_list, key="new_animal_tmpl")
        if st.button("➕ Állat létrehozása", key="animal_add_btn"):
            ok, msg = add_animal(new_an)
            if ok:
                if tmpl and tmpl != "(nincs)":
                    try: copy_parts_from_animal(tmpl, new_an)
                    except Exception: pass
                st.success("Állat létrehozva."); st.rerun()
            else:
                st.error(msg)
    with colA2:
        if not df_anim.empty:
            old = st.selectbox("Átnevezés – melyik állat?", df_anim["name"].tolist(), key="animal_rename_old")
            newn = st.text_input("Új név", key="animal_rename_new")
            prop = st.checkbox("Régi tételekben is frissítse a nevet", key="animal_rename_propagate")
            if st.button("✏️ Átnevezés", key="animal_rename_btn"):
                ok, msg = rename_animal(old, newn, propagate_batches=prop)
                if ok: st.success("Átnevezve."); st.rerun()
                else: st.error(msg)
    with colA3:
        if not df_anim.empty:
            tgt = st.selectbox("Aktiválás/Deaktiválás", df_anim["name"].tolist(), key="animal_toggle_sel")
            act_state = int(df_anim[df_anim["name"]==tgt].iloc[0]["active"]) if not df_anim.empty else 1
            if st.button("🔁 Állapot váltása", key="animal_toggle_btn"):
                set_animal_active(tgt, not bool(act_state))
                st.success("Státusz frissítve."); st.rerun()

    st.markdown("---")

    # Részek + mapping
    dsn, _, _ = get_pg_config()

    names_active = get_animals(only_active=True)
    animal_names = names_active["name"].tolist() if not names_active.empty else []
    if not animal_names:
        st.warning("Nincs aktív állat. Hozz létre és aktiválj egyet fent.")
        return

    colB, colC = st.columns([1,2])
    with colB:
        animal_sel = st.selectbox("Állat", animal_names, key="map_animal_sel")
        parts_list = get_all_parts(animal_sel)
        st.markdown("**Részek**")
        mapped_df = get_mappings(animal_sel)
        mapped_set = set(mapped_df["part_name"]) if not mapped_df.empty else set()
        part_to_map = st.selectbox("Kapcsolandó rész", parts_list, index=0 if parts_list else None, key="map_part_sel")
        if part_to_map in mapped_set:
            st.info("Ehhez a részhez már van kapcsolás. Alább felülírhatod.")

        st.markdown("---")
        st.subheader("Részek kezelése")
        new_part = st.text_input("Új rész neve", key="part_new_name")
        if st.button("➕ Rész hozzáadása", key="part_add_btn"):
            ok, msg = add_custom_part(animal_sel, new_part)
            if ok: st.success("Hozzáadva."); st.rerun()
            else: st.error(msg)

        custom_df = get_custom_parts(animal_sel, only_active=True)
        if not custom_df.empty:
            st.markdown("**Átnevezés / deaktiválás**")
            old = st.selectbox("Válassz részt", custom_df["name"].tolist(), key="part_rename_old")
            new_nm = st.text_input("Új név", key="part_rename_new")
            c1, c2 = st.columns(2)
            with c1:
                if st.button("✏️ Átnevezés", key="part_rename_btn"):
                    ok, msg = rename_custom_part(animal_sel, old, new_nm)
                    if ok: st.success("Átnevezve."); st.rerun()
                    else: st.error(msg)
            with c2:
                if st.button("🗑️ Deaktiválás", key="part_deactivate_btn"):
                    ok, msg = deactivate_custom_part(animal_sel, old)
                    if ok: st.success("Deaktiválva."); st.rerun()
                    else: st.error(msg)

    with colC:
        st.subheader("ERP tételek (alapadat.tetel – ceg_id=3)")
        if not dsn:
            st.warning("Nincs beállítva PostgreSQL kapcsolat. Menj a Beállításokhoz és mentsd el (vagy adj meg Secrets-et)!")
            erp_df = pd.DataFrame()
        else:
            ceg_id = st.number_input("ERP ceg_id", min_value=1, value=3, key="erp_ceg_id")
            try:
                erp_df = load_erp_items(dsn, ceg_id)
                if erp_df.empty:
                    st.info("Nincs találat az ERP-ben a megadott szűrővel.")
                else:
                    id_col, name_col, code_col = pick_erp_columns(erp_df)
                    q = st.text_input("Keresés (megnevezés / kód)", key="erp_search")
                    dfv = erp_df.copy()
                    if q:
                        ql = q.lower()
                        cols_to_search = [c for c in [name_col, code_col] if c and c in dfv.columns]
                        if cols_to_search:
                            mask = False
                            for c in cols_to_search:
                                s = dfv[c].astype(str).str.lower().str.contains(ql)
                                mask = (mask | s) if isinstance(mask, pd.Series) else s
                            dfv = dfv[mask]
                    show_cols = [c for c in [id_col, name_col, code_col] if c in dfv.columns]
                    st.dataframe(dfv[show_cols].rename(columns={id_col:"id", name_col:"megnevezes", (code_col or ""):"kod"}),
                                 use_container_width=True, height=320)

                    options = [
                        (str(r[id_col]),
                         f"{r[id_col]} – {r[name_col]}" + (f" ({r[code_col]})" if code_col and pd.notna(r[code_col]) else ""))
                        for _, r in dfv.iterrows()
                    ]
                    sel_label = st.selectbox("ERP tétel kiválasztása",
                                             options=[lbl for _, lbl in options],
                                             index=0 if options else None,
                                             key=f"erp_sel_{animal_sel}")
                    selected_val = None; selected_name = None; selected_code = None
                    if options and sel_label:
                        for v, lbl in options:
                            if lbl == sel_label:
                                selected_val = v; break
                        if selected_val is not None:
                            row = dfv[dfv[id_col].astype(str) == selected_val].iloc[0]
                            selected_name = str(row[name_col]) if name_col in row else None
                            selected_code = str(row[code_col]) if (code_col and code_col in row) else None

                    if st.button("💾 Kapcsolás mentése", key=f"save_mapping_btn_{animal_sel}",
                                 disabled=(not parts_list or not part_to_map or selected_val is None)):
                        upsert_mapping(animal_sel, part_to_map, selected_val, selected_name, selected_code)
                        st.success(f"Kapcsolás mentve: {animal_sel} / {part_to_map} → ERP #{selected_val}")
                        st.rerun()
            except Exception as e:
                st.error(f"ERP lekérdezés hiba: {e}")

        st.markdown("---")
        st.subheader("Meglévő kapcsolások")
        md = get_mappings(animal_sel)
        if md.empty:
            st.info("Ehhez az állathoz még nincs kapcsolás.")
        else:
            st.dataframe(md, use_container_width=True)
            mapped_parts = md["part_name"].tolist()
            del_part = st.selectbox("Kapcsolás törlése – válassz részt", mapped_parts,
                                    key=f"del_map_part_{animal_sel}")
            if st.button("❌ Kapcsolás törlése", key=f"delete_mapping_btn_{animal_sel}"):
                delete_mapping(animal_sel, del_part)
                st.success("Kapcsolás törölve."); st.rerun()

# --- Felső menü + oldalválasztás ---
st.title("🥩 Bontásinapló – vizuális MVP")

if "page" not in st.session_state:
    st.session_state["page"] = "Rögzítés"

if HAS_OPT_MENU:
    _pages = ["Rögzítés", "Törzsadatok & kapcsolások", "Beállítások"]
    try:
        _default_idx = _pages.index(st.session_state["page"])
    except ValueError:
        _default_idx = 0
    _selected = option_menu(
        None,
        _pages,
        icons=["clipboard-check", "diagram-3", "gear"],
        orientation="horizontal",
        default_index=_default_idx,
        styles={
            "container": {"padding": "0!important", "background-color": "#0b132b", "border-radius": "12px", "margin-bottom": "12px"},
            "icon": {"color": "#fff"},
            "nav-link": {"font-size": "16px", "color": "#fff", "padding": "10px 16px"},
            "nav-link-selected": {"background-color": "#3b82f6"},
        },
    )
    st.session_state["page"] = _selected
else:
    c1, c2, c3, _ = st.columns([1,1,1,6])
    if c1.button("Rögzítés", key="nav_rec_top", use_container_width=True):
        st.session_state["page"] = "Rögzítés"; st.rerun()
    if c2.button("Törzsadatok & kapcsolások", key="nav_map_top", use_container_width=True):
        st.session_state["page"] = "Törzsadatok & kapcsolások"; st.rerun()
    if c3.button("Beállítások", key="nav_set_top", use_container_width=True):
        st.session_state["page"] = "Beállítások"; st.rerun()

page = st.session_state["page"]
if page == "Beállítások":
    render_settings_page(); st.stop()
elif page == "Törzsadatok & kapcsolások":
    render_mappings_page(); st.stop()

# --- Oldalsáv – aktív állat + új tétel ---
st.sidebar.header("Műveletek")
_anim_df = get_animals(only_active=True)
_anim_names = _anim_df["name"].tolist() if not _anim_df.empty else []
if not _anim_names:
    st.sidebar.warning("Nincs aktív állat. Létrehozás: Törzsadatok & kapcsolások → Állatok kezelése")
    st.stop()
allat = st.sidebar.selectbox("Állat", _anim_names, key="sidebar_animal_sel")

st.sidebar.subheader("Új bontási tétel")
with st.sidebar.form("uj_batch_form"):
    datum = st.date_input("Dátum", datetime.now())
    tetel_azon = st.text_input("Tétel/Lot azonosító")
    beszallito = st.text_input("Beszállító")
    eredet = st.text_input("Eredet/ENAR/állat azonosítás")
    ellenorzo = st.text_input("Ellenőrizte (név)")
    ossztomeg = st.number_input("Beérkezett össztömeg (kg)", min_value=0.0, step=0.1, format="%0.2f")
    megjegyzes = st.text_area("Megjegyzés")
    submitted = st.form_submit_button("Tétel mentése", use_container_width=True)
    if submitted:
        if ossztomeg <= 0:
            st.warning("Az össztömeg legyen nagyobb mint 0.")
        else:
            new_id = save_batch({
                "datum": str(datum),
                "allat": allat,
                "tetel_azon": tetel_azon.strip() or None,
                "beszallito": beszallito.strip() or None,
                "eredet": eredet.strip() or None,
                "ellenorzo": ellenorzo.strip() or None,
                "ossztomeg": float(ossztomeg),
                "megjegyzes": megjegyzes.strip() or None,
            })
            st.success(f"Új bontási tétel mentve (ID: {new_id}). Válaszd ki lent és add meg a részeket.")

# --- Tétel kiválasztás ---
batches = get_batches()
col1, col2 = st.columns([2, 1])
with col1:
    st.subheader("Bontási tételek")
    if batches.empty:
        st.info("Még nincs tétel. Hozz létre egyet a bal oldalsávon!")
    else:
        st.dataframe(batches[["id","datum","allat","tetel_azon","beszallito","ossztomeg"]],
                     use_container_width=True)
with col2:
    batch_ids = batches["id"].tolist() if not batches.empty else []
    selected_id = st.selectbox("Aktív tétel ID", batch_ids, key="active_batch_id")

if batches.empty or selected_id is None:
    st.stop()

active_batch = batches[batches.id == selected_id].iloc[0]
closed_batch = (("status" in active_batch) and (active_batch["status"] == "closed"))

st.markdown("---")

# --- Rögzítés ---
st.header("Részek rögzítése")
left, right = st.columns([1, 1])

if "resz_sel" not in st.session_state:
    st.session_state["resz_sel"] = None
if "tomeg_str" not in st.session_state:
    st.session_state["tomeg_str"] = ""

with left:
    st.subheader("Érintő panel – rész és tömeg")
    parts = get_all_parts(active_batch["allat"])
    st.write("Válassz részt:")
    filter_txt = st.text_input("Keresés a részek között", key="filter_parts")
    display_parts = [p for p in parts if (filter_txt.lower() in p.lower())] if filter_txt else parts

    is_closed = closed_batch
    if is_closed:
        st.info("Ez a tétel **lezárt**. Új rögzítés nem engedélyezett.")
    else:
        if not display_parts:
            st.warning("Ehhez az állathoz még nincs rész felvéve. Menj a Törzsadatok & kapcsolások oldalra és adj hozzá részeket.")
        cols = st.columns(3)
        for i, p in enumerate(display_parts):
            col = cols[i % 3]
            if col.button(p, key=f"part_btn_{i}", use_container_width=True):
                st.session_state["resz_sel"] = p

        st.markdown(f"**Kiválasztott rész:** {st.session_state.get('resz_sel') or '—'}")

        st.write("Tömeg (kg)")
        keypad_rows = [["7","8","9"], ["4","5","6"], ["1","2","3"], ["0",".",","], ["⌫","C","+0.1"], ["-0.1","+1.0","-1.0"]]
        for r, row in enumerate(keypad_rows):
            kcols = st.columns(3)
            for j, label in enumerate(row):
                if kcols[j].button(label, key=f"kp_{r}_{j}", use_container_width=True):
                    s = st.session_state.get("tomeg_str", "")
                    if label == "C":
                        s = ""
                    elif label == "⌫":
                        s = s[:-1]
                    elif label in [".", ","]:
                        if "." not in s:
                            s = ("0." if s == "" else s + ".")
                    elif label[0] in "+-":
                        try:
                            inc = float(label.replace("+",""))
                            s = f"{(float(s) if s else 0.0) + inc:.3f}"
                        except Exception:
                            s = label.replace("+","")
                    else:
                        s = s + label
                    st.session_state["tomeg_str"] = s

        st.markdown(f"<div class='lcd'>{st.session_state.get('tomeg_str','') or '0'}</div>", unsafe_allow_html=True)

        resz_megj = st.text_input("Megjegyzés", key="resz_megj_touch")

        if st.button("➕ Hozzáadás", type="primary", use_container_width=True, key="add_part_btn", disabled=(not parts)):
            sel = st.session_state.get("resz_sel")
            s = st.session_state.get("tomeg_str", "")
            try:
                val = float(s)
            except Exception:
                val = 0.0
            if not sel:
                st.warning("Válassz először részt.")
            elif val <= 0:
                st.warning("Adj meg érvényes tömeget.")
            else:
                save_part(int(active_batch.id), sel, float(val), resz_megj)
                # Opcionális külső küldések
                try:
                    _url, _api_key, _secret, _auto = get_vir_config()
                    if _auto and _url:
                        _payload = build_part_payload(active_batch, {"resz": sel, "tomeg": float(val), "megjegyzes": resz_megj})
                        send_json_to_vir(_payload, _url, _api_key, _secret, event="part_created", batch_id=int(active_batch.id))
                except Exception as e:
                    st.warning(f"VIR küldés kihagyva: {e}")
                try:
                    _dsn, _pg_auto, _interval = get_pg_config()
                    _payload_pg = build_part_payload(active_batch, {"resz": sel, "tomeg": float(val), "megjegyzes": resz_megj})
                    if _pg_auto and _dsn:
                        queue_pg_payload(_payload_pg, int(active_batch.id), event="part_created", dsn=_dsn, auto=_pg_auto)
                except Exception as e:
                    st.warning(f"PG küldés sorba állítva hiba miatt: {e}")
                st.session_state["tomeg_str"] = ""
                st.rerun()

    if st.button("↩️ Visszavonás (utolsó tétel)", use_container_width=True, key="undo_btn", disabled=is_closed):
        df_last = get_parts(int(active_batch.id))
        if not df_last.empty:
            last_id = int(df_last.iloc[0]["id"])
            if not delete_part(last_id):
                st.warning("Lezárt tételből nem törölhetsz.")
            st.session_state["tomeg_str"] = ""
            st.rerun()
        else:
            st.warning("Nincs mit visszavonni.")

with right:
    st.subheader("Aktív tétel fejléce")
    is_closed = closed_batch
    closed_info = f" (lezárva: {active_batch.get('closed_at','')})" if is_closed else ""
    st.markdown(
        f"""
        **Dátum:** {active_batch['datum']}  
        **Állat:** {active_batch['allat']}  
        **Beérkezett tömeg:** **{active_batch['ossztomeg']:.2f} kg**  
        **Tétel:** {active_batch['tetel_azon'] or '-'}  
        **Beszállító:** {active_batch['beszallito'] or '-'}  
        **Ellenőrizte:** {active_batch['ellenorzo'] or '-'}  
        **Státusz:** {'Lezárt' if is_closed else 'Nyitott'}{closed_info}
        """
    )

    st.subheader("Szállítólevél / mellékletek")
    photo = st.camera_input("Fotó készítése (szállítólevél)", key="cam_input")
    upload = st.file_uploader("Feltöltés (JPG/PNG/PDF)", type=["jpg","jpeg","png","pdf"], accept_multiple_files=False, key="file_uploader")
    note_att = st.text_input("Melléklet megjegyzés", key="attach_note")
    if st.button("📎 Melléklet mentése", use_container_width=True, key="save_attach_btn"):
        file_to_save = None; mime = None; fname = None
        if photo is not None:
            file_to_save = photo.getvalue()
            mime = getattr(photo, 'type', 'image/jpeg'); fname = getattr(photo, 'name', 'camera.jpg')
        elif upload is not None:
            file_to_save = upload.getvalue()
            mime = getattr(upload, 'type', 'application/octet-stream'); fname = getattr(upload, 'name', 'file')
        if file_to_save:
            save_attachment(int(active_batch.id), file_to_save, fname, mime, kind="szallitolevel", note=note_att)
            st.success("Melléklet mentve."); st.rerun()
        else:
            st.warning("Nincs kiválasztott fotó vagy fájl.")

    atts = get_attachments(int(active_batch.id))
    if not atts.empty:
        for _, a in atts.iterrows():
            if a['mime'] and str(a['mime']).startswith('image/'):
                st.image(a['path'], caption=a.get('note') or a['path'], use_container_width=True)
            else:
                st.write(f"📄 {a['path']} ({a.get('mime') or 'ismeretlen mime'})")

st.subheader("Rögzített részek")
parts_df = get_parts(int(active_batch.id))
if parts_df.empty:
    st.info("Még nincs rögzített rész ehhez a tételhez.")
else:
    for _, row in parts_df.iterrows():
        c1, c2, c3, c4 = st.columns([3,2,2,2])
        c1.write(row["resz"])
        c2.write(f"{row['tomeg']:.2f} kg")
        c3.write(row.get("megjegyzes") or "—")
        if closed_batch:
            c4.button("🗑️ Törlés", key=f"del_btn_{int(row['id'])}", disabled=True)
        else:
            if c4.button("🗑️ Törlés", key=f"del_btn_{int(row['id'])}"):
                ok_del = delete_part(int(row["id"]))
                if not ok_del:
                    st.warning("Lezárt tételből nem törölhetsz.")
                st.rerun()

# --- Hozam/egyenleg ---
st.markdown("---")
st.header("Hozam és egyenleg")
osszeg = float(parts_df["tomeg"].sum()) if not parts_df.empty else 0.0
be = float(active_batch["ossztomeg"])
kulonbseg = be - osszeg
hozam = (osszeg / be * 100.0) if be > 0 else 0.0

m1, m2, m3, m4 = st.columns(4)
with m1: st.metric("Beérkezett (kg)", f"{be:.2f}")
with m2: st.metric("Rögzített részek (kg)", f"{osszeg:.2f}")
with m3: st.metric("Hozam (%)", f"{hozam:.1f}%")
with m4: st.metric("Különbözet (kg)", f"{kulonbseg:.2f}")

st.progress(max(0, min(100, int(hozam))))

# Különbözet felvétele
default_parts = get_all_parts(active_batch["allat"])
try:
    _default_index = default_parts.index("Csont")
except ValueError:
    _default_index = 0 if default_parts else 0
colk1, colk2 = st.columns([2,2])
with colk1:
    target_part = st.selectbox("Különbözet rögzítése ide", default_parts, index=_default_index, key="diff_target_sel")
with colk2:
    if st.button("➕ Különbözet felvétele", key="add_diff_btn", disabled=(kulonbseg<=0 or closed_batch or not default_parts)):
        if kulonbseg > 0:
            save_part(int(active_batch.id), target_part, float(round(kulonbseg,3)), "Automatikus: különbözet")
            st.rerun()
        else:
            st.warning("Nincs pozitív különbözet.")

st.caption("Megjegyzés: a különbözetbe beletartozhat csont, veszteség, zsírveszteség, fólia, csomagolás, párolgás stb.")

# --- Lezárás és küldés ---
st.markdown("---")
st.subheader("Tétel lezárása és teljes adathalmaz küldése")
closed = closed_batch
colz1, colz2 = st.columns([2,2])
with colz1:
    confirm_close = st.checkbox("Megerősítem: a bontás befejeződött és az adatok helyesek.", value=False, disabled=closed, key="confirm_close_cb")
with colz2:
    btn_close = st.button("🔒 Bontás kész / Lezárás", type="primary", use_container_width=True, key="close_batch_btn",
                          disabled=(closed or not confirm_close or parts_df.empty))

if btn_close:
    _did = close_batch(int(active_batch.id))
    batches = get_batches()
    active_batch = batches[batches.id == selected_id].iloc[0]
    parts_df = get_parts(int(active_batch.id))
    payload_full = build_full_batch_payload(active_batch, parts_df)
    dsn, _, _ = get_pg_config()
    if dsn:
        queue_pg_payload(payload_full, int(active_batch.id), event="batch_closed", dsn=dsn, auto=True)
    st.success("Tétel lezárva és teljes adathalmaz elküldésre sorba állítva a PostgreSQL felé.")
    st.rerun()

# --- Export / Nyomtatás ---
st.markdown("---")
st.subheader("Export / Nyomtatás")
colx, coly = st.columns([1, 1])
with colx:
    csv_b = batches.to_csv(index=False).encode("utf-8")
    st.download_button("Tételek CSV letöltése", csv_b, file_name="batches.csv", mime="text/csv", key="dl_batches_csv")
with coly:
    parts_exp = get_parts(int(active_batch.id))
    parts_exp.insert(0, "batch_id", int(active_batch.id))
    csv_p = parts_exp.to_csv(index=False).encode("utf-8")
    st.download_button("Aktív tétel részei CSV", csv_p, file_name=f"parts_batch_{int(active_batch.id)}.csv",
                       mime="text/csv", key="dl_parts_csv")

st.subheader("PDF jelentés")
if 'REPORTLAB_AVAILABLE' in globals() and REPORTLAB_AVAILABLE:
    _atts = get_attachments(int(active_batch.id))
    _pdf = build_pdf(active_batch, parts_df, _atts)
    st.download_button("📄 PDF jelentés letöltése", data=_pdf.getvalue() if _pdf else None,
                       file_name=f"bontas_{int(active_batch.id)}.pdf", mime="application/pdf",
                       disabled=(_pdf is None), key="dl_pdf_btn")
else:
    st.warning("A PDF exporthoz telepítsd a ReportLab csomagot: `pip install reportlab`")

st.info("Tippek: A fejléchez érdemes még rögzíteni: hőmérséklet, ellenőrzés ideje, takarítás státusz, nyomonkövetési azonosítók (szállítólevél, ENAR, vágási sorszám).")
