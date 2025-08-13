# streamlit_app.py
# Bontásinapló – vizuális, érintőbarát MVP
# Fő funkciók:
#  - Tétel felvétel (össztömeg RS232/USB mérlegből is)
#  - Részek rögzítése (rész tömeg RS232/USB mérlegből is)
#  - Csatolmányok külön oldalon (képtömörítés + thumbnail + állítható tárhely)
#  - ERP kapcsolások (állatok/részek törzs + PG-ből tétel kapcsolás)
#  - VIR JSON (HMAC, manuális/automatikus)
#  - PG outbox, háttérküldő, pool admin
#  - PDF export
#
# Indítás:
#   pip install -r requirements.txt
#   streamlit run streamlit_app.py
#
# Javasolt requirements.txt:
#   streamlit>=1.36
#   streamlit-option-menu>=0.3.12
#   pandas>=2.2
#   sqlalchemy>=2.0
#   psycopg2-binary>=2.9
#   requests>=2.31
#   reportlab>=4.0
#   pillow>=10.0
#   pyserial>=3.5
#
# Megjegyzések:
#  - PG jelszó: Streamlit Secrets / env változó / (lokálon keyring). A UI nem írja ki a jelszót.
#  - Minden gomb egyedi kulcsot kapott, nincs duplikált element ID.


import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.pool import QueuePool
from sqlalchemy.engine.url import make_url
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

# Opcionális felső menü
try:
    from streamlit_option_menu import option_menu
    HAS_OPT_MENU = True
except Exception:
    HAS_OPT_MENU = False

# Keyring (lokálra, ha van)
try:
    import keyring
    HAS_KEYRING = True
except Exception:
    HAS_KEYRING = False

# Pillow a képtömörítéshez
try:
    from PIL import Image, ImageOps
    HAS_PIL = True
except Exception:
    HAS_PIL = False

# Serial / mérleg
try:
    import serial
    from serial.tools import list_ports
    HAS_SERIAL = True
except Exception:
    HAS_SERIAL = False


# --- Alap seed adatok ---
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

# --- SQLite lokális adattár ---
engine = create_engine("sqlite:///bontas.db", future=True)

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
            created_at TEXT,
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
            thumb_path TEXT,
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
    # Indexek a gyorsításhoz
    conn.execute(text("CREATE INDEX IF NOT EXISTS parts_batch_idx ON parts(batch_id)"))
    conn.execute(text("CREATE INDEX IF NOT EXISTS mappings_idx ON part_mappings(animal, part_name)"))

# Sémabővítések biztosítása
with engine.begin() as conn:
    try:
        conn.execute(text("ALTER TABLE parts ADD COLUMN created_at TEXT"))
    except Exception:
        pass
    try:
        conn.execute(text("ALTER TABLE attachments ADD COLUMN thumb_path TEXT"))
    except Exception:
        pass

# Seed data idempotensen
def seed_defaults():
    with engine.begin() as conn:
        for a in SEED_ANIMALS.keys():
            conn.execute(text("INSERT OR IGNORE INTO animals(name, active) VALUES (:n, 1)"), {"n": a})
        # Régi batch-ek állatai (ha volt kézi felvétel)
        batch_animals = [r[0] for r in conn.execute(text("SELECT DISTINCT allat FROM batches")).fetchall()]
        for a in batch_animals:
            if a:
                conn.execute(text("INSERT OR IGNORE INTO animals(name, active) VALUES (:n, 1)"), {"n": a})
        # Részek seed
        for a, parts in SEED_ANIMALS.items():
            for p in parts:
                conn.execute(text("""
                    INSERT INTO custom_parts(animal, name, active)
                    SELECT :a, :p, 1
                    WHERE NOT EXISTS (SELECT 1 FROM custom_parts WHERE animal=:a AND name=:p)
                """), {"a": a, "p": p})

seed_defaults()


# --- Settings util ---
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


# --- Állatok / részek / mapping réteg ---
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
        res = conn.execute(text("INSERT OR IGNORE INTO animals(name, active) VALUES (:n, 1)"), {"n": name})
        if getattr(res, "rowcount", 0) == 0:
            return True, "Már létezett (kihagyva)"
    return True, "Hozzáadva"

def set_animal_active(name: str, active: bool):
    with engine.begin() as conn:
        conn.execute(text("UPDATE animals SET active=:a WHERE name=:n"),
                     {"a": 1 if active else 0, "n": name})

def rename_animal(old_name: str, new_name: str, propagate_batches: bool = False):
    new_name = (new_name or "").strip()
    if not new_name:
        return False, "Üres új név"
    with engine.begin() as conn:
        conn.execute(text("UPDATE animals SET name=:new WHERE name=:old"),
                     {"new": new_name, "old": old_name})
        conn.execute(text("UPDATE custom_parts SET animal=:new WHERE animal=:old"),
                     {"new": new_name, "old": old_name})
        conn.execute(text("UPDATE part_mappings SET animal=:new WHERE animal=:old"),
                     {"new": new_name, "old": old_name})
        if propagate_batches:
            conn.execute(text("UPDATE batches SET allat=:new WHERE allat=:old"),
                         {"new": new_name, "old": old_name})
    return True, "Átnevezve"

def copy_parts_from_animal(src_animal: str, dst_animal: str, include_inactive: bool = False) -> int:
    filt = "" if include_inactive else " AND active = 1"
    rows = pd.read_sql("SELECT name, active FROM custom_parts WHERE animal = :a" + filt,
                       engine, params={"a": src_animal})
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
        conn.execute(text("UPDATE custom_parts SET name=:new WHERE animal=:a AND name=:old"),
                     {"new": new_name, "a": animal, "old": old_name})
        conn.execute(text("UPDATE part_mappings SET part_name=:new WHERE animal=:a AND part_name=:old"),
                     {"new": new_name, "a": animal, "old": old_name})
    return True, "Átnevezve"

def deactivate_custom_part(animal: str, name: str):
    with engine.begin() as conn:
        conn.execute(text("UPDATE custom_parts SET active=0 WHERE animal=:a AND name=:n"),
                     {"a": animal, "n": name})
    return True, "Deaktiválva"

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
        conn.execute(text("DELETE FROM part_mappings WHERE animal=:a AND part_name=:p"),
                     {"a": animal, "p": part_name})


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


# --- PG DSN / titkok ---
def _pg_key_id(host: str, port: int, db: str, user: str) -> str:
    return f"{user}@{host}:{port}/{db}"

def build_pg_dsn(host: str, port: int, db: str, user: str, password: str = None, sslmode: str = "prefer") -> str:
    if not (host and db and user):
        return ""
    pw = quote_plus(password or "")
    return f"postgresql+psycopg2://{user}:{pw}@{host}:{int(port)}/{db}?application_name=Bontasinaplo"

def get_pg_conn_fields():
    # Secrets (Cloud)
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
    # ENV
    host = os.getenv("PGHOST", get_setting("pg_host", ""))
    port = int(os.getenv("PGPORT", get_setting("pg_port", "5432") or 5432))
    db   = os.getenv("PGDATABASE", get_setting("pg_db", ""))
    user = os.getenv("PGUSER", get_setting("pg_user", ""))
    sslmode = os.getenv("PGSSLMODE", get_setting("pg_sslmode", "prefer"))
    pw_env = os.getenv("PGPASSWORD") or os.getenv("PG_PASSWORD")
    if pw_env:
        return {"host": host,"port":port,"db":db,"user":user,"sslmode":sslmode,
                "password_saved": True, "password_source": "env", "password_env": True}
    # Keyring / settings
    saved = False
    if HAS_KEYRING and host and db and user:
        try:
            saved = keyring.get_password("Bontasinaplo", _pg_key_id(host, port, db, user)) is not None
        except Exception:
            saved = False
    return {"host": host,"port":port,"db":db,"user":user,"sslmode":sslmode,
            "password_saved": saved, "password_source": "settings"}

def set_pg_conn_fields(host: str, port: int, db: str, user: str, password: str = None, sslmode: str = "prefer"):
    if "pg" in st.secrets:
        st.info("A PG kapcsolatot a Streamlit Secrets adja. Itt nem mentek el semmit.")
        return
    set_setting("pg_host", host or "")
    set_setting("pg_port", str(int(port or 5432)))
    set_setting("pg_db", db or "")
    set_setting("pg_user", user or "")
    set_setting("pg_sslmode", sslmode or "prefer")
    set_setting("pg_dsn", "")  # legacy törlése
    if password and password.strip():
        if HAS_KEYRING:
            try:
                keyring.set_password("Bontasinaplo", _pg_key_id(host, int(port or 5432), db, user), password.strip())
            except Exception as e:
                st.warning(f"Nem sikerült a jelszót kulcstárba menteni: {e}")
        else:
            st.warning("Keyring nincs – a jelszót nem tudom elmenteni biztonságosan.")

def set_pg_config_compat(auto_send: bool, interval: int):
    set_setting("pg_auto_send", "1" if auto_send else "0")
    set_setting("pg_auto_interval", str(int(interval or 60)))

def get_pg_config():
    f = get_pg_conn_fields()
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
        dsn = get_setting("pg_dsn", "")
    auto_send = get_setting("pg_auto_send", "0") == "1"
    try:
        interval = int(get_setting("pg_auto_interval", "60") or 60)
    except Exception:
        interval = 60
    return dsn, auto_send, interval


# --- PG engine cache + outbox/flush ---
PG_ENGINE_CACHE = {}
PG_ENGINE_LOCK = threading.Lock()
FLUSH_LOCK = threading.Lock()

def get_or_create_pg_engine(dsn: str):
    if not dsn:
        return None
    with PG_ENGINE_LOCK:
        eng = PG_ENGINE_CACHE.get(dsn)
        if eng is None:
            eng = create_engine(
                dsn,
                future=True,
                poolclass=QueuePool,
                pool_size=5,
                max_overflow=2,
                pool_timeout=30,
                pool_recycle=1800,
                pool_pre_ping=True,
                connect_args={"application_name": "Bontasinaplo"},
            )
            PG_ENGINE_CACHE[dsn] = eng
        return eng

def dispose_pg_engine(dsn: str):
    with PG_ENGINE_LOCK:
        eng = PG_ENGINE_CACHE.pop(dsn, None)
        if eng is not None:
            try:
                eng.dispose()
            except Exception:
                pass

def dispose_all_pg_engines():
    with PG_ENGINE_LOCK:
        for dsn, eng in list(PG_ENGINE_CACHE.items()):
            try:
                eng.dispose()
            except Exception:
                pass
        PG_ENGINE_CACHE.clear()

def test_pg_connection(dsn: str):
    if not dsn:
        return False, "Üres DSN"
    try:
        eng = get_or_create_pg_engine(dsn)
        with eng.connect() as conn:
            conn.execute(text("SELECT 1"))
        return True, "OK"
    except Exception as e:
        return False, str(e)

def pg_insert_payload_with_engine(eng, payload_json: str):
    with eng.begin() as conn:
        conn.execute(text("INSERT INTO api.api_bontasi_naplo(adat) VALUES (:adat)"), {"adat": payload_json})

def queue_pg_payload(payload, batch_id: int, event: str, dsn: str = None, auto: bool = True):
    payload_json = payload if isinstance(payload, str) else json.dumps(payload, ensure_ascii=False)
    now = datetime.now().isoformat(timespec="seconds")
    # dedup lezárásra
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

def flush_pg_outbox(dsn: str, max_items: int = 50) -> bool:
    eng = get_or_create_pg_engine(dsn)
    if eng is None:
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
        if not rows:
            return True
        for r in rows:
            rid = int(r[0]); payload_json = r[1]; tries = int(r[2] or 0)
            with engine.begin() as conn:
                res = conn.execute(text("UPDATE pg_outbox SET status='processing' WHERE id=:id AND status='pending'"),
                                   {"id": rid})
                if getattr(res, "rowcount", 0) == 0:
                    continue
            try:
                pg_insert_payload_with_engine(eng, payload_json)
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

def start_pg_bg_flusher():
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


# --- Batch státusz + payloadok ---
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


# --- Adatelérés (SQLite) ---
def get_batches():
    return pd.read_sql("SELECT * FROM batches ORDER BY id DESC", engine)

def get_parts(batch_id: int):
    return pd.read_sql("SELECT id, resz, tomeg, megjegyzes, created_at FROM parts WHERE batch_id = ? ORDER BY id DESC",
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
            INSERT INTO parts(batch_id, resz, tomeg, megjegyzes, created_at)
            VALUES (:batch_id, :resz, :tomeg, :megjegyzes, :created_at)
        """), {"batch_id": batch_id, "resz": resz, "tomeg": tomeg, "megjegyzes": megjegyzes,
               "created_at": datetime.now().isoformat(timespec="seconds")})

def delete_part(part_id: int) -> bool:
    with engine.begin() as conn:
        row = conn.execute(text("SELECT batch_id FROM parts WHERE id = :id"), {"id": part_id}).fetchone()
        if not row: return False
        bid = int(row[0])
        st_row = conn.execute(text("SELECT status FROM batches WHERE id=:id"), {"id": bid}).fetchone()
        if st_row and (st_row[0] or "open") == "closed":
            return False
        conn.execute(text("DELETE FROM parts WHERE id = :id"), {"id": part_id})
    return True


# --- Mérleg (RS232/USB) util ---
import re

def list_serial_ports_cached():
    if not HAS_SERIAL:
        return []
    try:
        return [(p.device, f"{p.device} — {p.description}") for p in list_ports.comports()]
    except Exception:
        return []

def get_scale_config():
    port = get_setting("scale_port", "")
    try:
        baud = int(get_setting("scale_baud", "9600") or 9600)
    except Exception:
        baud = 9600
    try:
        bytesize = int(get_setting("scale_bytesize", "8") or 8)
    except Exception:
        bytesize = 8
    parity = (get_setting("scale_parity", "N") or "N").upper()
    try:
        stopbits = int(get_setting("scale_stopbits", "1") or 1)
    except Exception:
        stopbits = 1
    try:
        timeout = float(get_setting("scale_timeout", "1.0") or 1.0)
    except Exception:
        timeout = 1.0
    unit = get_setting("scale_unit", "kg")
    factor = {"kg": 1.0, "g": 0.001, "lb": 0.45359237, "t": 1000.0}.get(unit, 1.0)
    return {"port": port, "baud": baud, "bytesize": bytesize, "parity": parity,
            "stopbits": stopbits, "timeout": timeout, "unit": unit, "factor": factor}

def set_scale_config(port, baud, bytesize, parity, stopbits, timeout, unit):
    set_setting("scale_port", port or "")
    set_setting("scale_baud", str(int(baud or 9600)))
    set_setting("scale_bytesize", str(int(bytesize or 8)))
    set_setting("scale_parity", (parity or "N").upper())
    set_setting("scale_stopbits", str(int(stopbits or 1)))
    set_setting("scale_timeout", str(float(timeout or 1.0)))
    set_setting("scale_unit", unit or "kg")

def parse_weight_line(line: str):
    if not line:
        return None
    s = str(line).strip()
    m = re.findall(r"([-+]?\d+(?:[\.,]\d+)?)\s*([a-zA-Z]+)?", s)
    if not m:
        return None
    num, unit = m[-1]
    num = num.replace(",", ".")
    try:
        val = float(num)
    except Exception:
        return None
    unit = (unit or "").lower()
    if unit.startswith("g"):
        val *= 0.001
    elif unit in ("kg", ""):
        pass
    elif unit in ("t", "ton", "tonna"):
        val *= 1000.0
    elif unit in ("lb", "lbs"):
        val *= 0.45359237
    return val

def read_scale_value(max_seconds: float = 2.0):
    cfg = get_scale_config()
    if not HAS_SERIAL:
        return False, None, None, "A pyserial nincs telepítve. Telepítsd: pip install pyserial"
    if not cfg["port"]:
        return False, None, None, "Nincs beállított soros port."
    try:
        ser = serial.Serial(
            cfg["port"],
            baudrate=cfg["baud"],
            bytesize=cfg["bytesize"],
            parity=cfg["parity"],
            stopbits=cfg["stopbits"],
            timeout=cfg["timeout"],
        )
    except Exception as e:
        return False, None, None, f"Port megnyitási hiba: {e}"
    try:
        try:
            ser.reset_input_buffer(); ser.reset_output_buffer()
        except Exception:
            pass
        t0 = time.time()
        raw_lines = []
        parsed = None
        while (time.time() - t0) < max_seconds:
            try:
                line = ser.readline().decode("utf-8", errors="ignore")
            except Exception:
                line = ""
            if line:
                raw_lines.append(line.strip())
                v = parse_weight_line(line)
                if v is not None:
                    parsed = v
                    break
        if parsed is None:
            return False, None, "\n".join(raw_lines[-5:]), "Nem sikerült értelmezni a mérleg adatát."
        parsed = parsed * cfg["factor"]
        return True, parsed, "\n".join(raw_lines[-5:]), "OK"
    finally:
        try:
            ser.close()
        except Exception:
            pass


# --- Mellékletek tárolási beállítások + tömörítés ---
def get_attach_config():
    base_dir = get_setting("att_base_dir", "docs")
    subfolders = get_setting("att_subfolders", "1") == "1"   # YYYY/MM/batch_id
    try:
        max_px = int(get_setting("att_img_max_px", "1800") or 1800)
    except Exception:
        max_px = 1800
    try:
        quality = int(get_setting("att_img_quality", "85") or 85)
    except Exception:
        quality = 85
    return base_dir, subfolders, max_px, quality

def set_attach_config(base_dir: str, subfolders: bool, max_px: int, quality: int):
    set_setting("att_base_dir", base_dir or "docs")
    set_setting("att_subfolders", "1" if subfolders else "0")
    set_setting("att_img_max_px", str(int(max_px or 1800)))
    set_setting("att_img_quality", str(int(quality or 85)))

def ensure_dir(p):
    os.makedirs(p, exist_ok=True)
    return p

def _compress_image_bytes(img_bytes: bytes, max_side_px: int = 1800, quality: int = 85) -> bytes:
    if not HAS_PIL:
        return img_bytes
    with Image.open(BytesIO(img_bytes)) as im0:
        im = ImageOps.exif_transpose(im0)
        if im.mode not in ("RGB", "L"):
            im = im.convert("RGB")
        w, h = im.size
        scale = max(w, h) / float(max_side_px) if max(w, h) > max_side_px else 1.0
        if scale > 1.0:
            im = im.resize((int(w/scale), int(h/scale)), Image.LANCZOS)
        out = BytesIO()
        im.save(out, format="JPEG", quality=int(quality), optimize=True, progressive=True)
        return out.getvalue()

def _make_thumbnail_bytes(img_bytes: bytes, thumb_px: int = 420, quality: int = 75) -> bytes:
    if not HAS_PIL:
        return img_bytes
    with Image.open(BytesIO(img_bytes)) as im0:
        im = ImageOps.exif_transpose(im0)
        if im.mode not in ("RGB", "L"):
            im = im.convert("RGB")
        im.thumbnail((thumb_px, thumb_px), Image.LANCZOS)
        out = BytesIO()
        im.save(out, format="JPEG", quality=int(quality), optimize=True, progressive=True)
        return out.getvalue()

def get_attachments(batch_id: int):
    return pd.read_sql(
        "SELECT id, kind, path, mime, created_at, note, thumb_path FROM attachments WHERE batch_id = ? ORDER BY id DESC",
        engine, params=(batch_id,)
    )

def delete_attachment(att_id: int) -> bool:
    with engine.begin() as conn:
        row = conn.execute(text("SELECT batch_id, path, thumb_path FROM attachments WHERE id=:id"), {"id": att_id}).fetchone()
        if not row: return False
        batch_id = int(row[0]); p = row[1]; tp = row[2]
        st_row = conn.execute(text("SELECT status FROM batches WHERE id=:id"), {"id": batch_id}).fetchone()
        if st_row and (st_row[0] or "open") == "closed":
            return False
        conn.execute(text("DELETE FROM attachments WHERE id=:id"), {"id": att_id})
    # fájlok törlése (best-effort)
    for fp in [p, tp]:
        if fp and os.path.exists(fp):
            try: os.remove(fp)
            except Exception: pass
    return True

def save_attachment(batch_id: int, file_bytes: bytes, filename: str, mime: str, kind: str = "szallitolevel",
                    note: str = ""):
    base_dir, subfolders, max_px, quality = get_attach_config()
    now = datetime.now()
    sub = f"{now.year:04d}/{now.month:02d}/{batch_id}" if subfolders else ""
    folder = ensure_dir(os.path.join(base_dir, sub)) if sub else ensure_dir(base_dir)

    if mime and str(mime).startswith("image/") and HAS_PIL:
        imgc = _compress_image_bytes(file_bytes, max_side_px=max_px, quality=quality)
        imgt = _make_thumbnail_bytes(imgc, thumb_px=420, quality=max(50, quality - 10))
        uid = uuid.uuid4().hex
        f_main = os.path.join(folder, f"{batch_id}_{uid}.jpg")
        f_thumb = os.path.join(folder, f"{batch_id}_{uid}_tn.jpg")
        with open(f_main, "wb") as f: f.write(imgc)
        with open(f_thumb, "wb") as f: f.write(imgt)
        db_path = f_main; db_thumb = f_thumb; db_mime = "image/jpeg"
    else:
        # Nem kép: eredeti mentése
        if filename and "." in filename:
            ext = "." + filename.rsplit(".", 1)[1].lower()
        else:
            ext = ".pdf" if (mime == "application/pdf") else ".bin"
        uid = uuid.uuid4().hex
        f_main = os.path.join(folder, f"{batch_id}_{uid}{ext}")
        with open(f_main, "wb") as f: f.write(file_bytes)
        db_path = f_main; db_thumb = None; db_mime = mime

    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO attachments(batch_id, kind, path, mime, created_at, note, thumb_path)
            VALUES (:batch_id, :kind, :path, :mime, :created_at, :note, :thumb)
        """), {
            "batch_id": batch_id, "kind": kind, "path": db_path, "mime": db_mime,
            "created_at": datetime.now().isoformat(timespec="seconds"),
            "note": note, "thumb": db_thumb
        })
    return db_path


# --- ERP lekérés (cache) ---
@st.cache_data(ttl=300, show_spinner=False)
def load_erp_items(dsn: str, ceg_id: int = 3) -> pd.DataFrame:
    eng = get_or_create_pg_engine(dsn)
    if eng is None:
        return pd.DataFrame()
    df = pd.read_sql(text("SELECT * FROM alapadat.tetel WHERE gyorskod is not null and ceg_id = :c"), eng, params={"c": int(ceg_id)})
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
                img_row = a; break
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


# --- Stílus ---
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

/* Nagy, átlátható táblák */
div[data-testid="stDataFrame"] table { font-size: 18px; }
div[data-testid="stDataFrame"] tbody tr:nth-child(odd) { background: #fafafa; }
</style>
""", unsafe_allow_html=True)


# --- Oldalak / segédek ---
def _mask_dsn(dsn: str) -> str:
    try:
        url = make_url(dsn)
        return str(url.set(password="***"))
    except Exception:
        return "(rejtett jelszó)"


def render_settings_page():
    st.header("Beállítások / Integrációk")

    # VIR
    st.subheader("VIR integráció (küldés JSON-ban)")
    vir_url = st.text_input("VIR endpoint URL", value=(get_setting("vir_url","") or os.environ.get("VIR_URL","")), key="vir_url_in")
    vir_api_key = st.text_input("API kulcs (opcionális)", value=(get_setting("vir_api_key","") or os.environ.get("VIR_API_KEY","")), type="password", key="vir_key_in")
    vir_secret = st.text_input("Aláíró titok HMAC-SHA256 (opcionális)", value=(get_setting("vir_secret","") or os.environ.get("VIR_SECRET","")), type="password", key="vir_sec_in")
    vir_auto = st.checkbox("Auto-küldés mentéskor (rész hozzáadásakor)", value=(get_setting("vir_auto_send","0")=="1"), key="vir_auto_cb")
    if st.button("VIR beállítások mentése", key="vir_save_btn"):
        set_vir_config(vir_url, vir_api_key, vir_secret, vir_auto)
        st.success("VIR beállítások mentve.")

    st.markdown("---")

    # Csatolmány tárhely
    st.subheader("Csatolmányok tárhely")
    base_dir, subfolders, max_px, quality = get_attach_config()
    c1, c2 = st.columns([2,1])
    with c1:
        new_base = st.text_input("Alap könyvtár (pl. hálózati megosztás)", value=base_dir, key="att_base_dir_in")
    with c2:
        subf = st.checkbox("Dátum/Batch almappák (YYYY/MM/<batch_id>)", value=subfolders, key="att_subfolders_cb")
    c3, c4 = st.columns([1,1])
    with c3:
        new_maxpx = st.number_input("Kép max. hosszabbik oldala (px)", min_value=600, max_value=6000, value=int(max_px), step=100, key="att_max_px_in")
    with c4:
        new_q = st.slider("JPEG minőség", min_value=50, max_value=95, value=int(quality), key="att_jpeg_q_in")
    if st.button("Tárhely beállítások mentése", key="att_save_btn"):
        set_attach_config(new_base or "docs", bool(subf), int(new_maxpx), int(new_q))
        st.success("Csatolmány tárhely beállítások mentve.")

    st.markdown("---")

    # Mérleg (RS232/USB)
    st.subheader("Mérleg (RS232/USB)")
    if not HAS_SERIAL:
        st.warning("A mérleg használatához telepítsd a pyserial csomagot: pip install pyserial")

    cfg = get_scale_config()
    ports = list_serial_ports_cached()
    labels = [lbl for _, lbl in ports]
    devices = [dev for dev, _ in ports]

    csp1, csp2 = st.columns([2,2])
    with csp1:
        sel_idx = devices.index(cfg["port"]) + 1 if cfg["port"] in devices else 0
        sel_label = st.selectbox("Elérhető portok", options=["(válassz)"] + labels, index=sel_idx, key="sc_port_sel")
    with csp2:
        manual_port = st.text_input("Port (kézi megadás)", value=cfg["port"],
                                    placeholder="/dev/tty.usbserial-... vagy COM3", key="sc_port_manual")

    csp3, csp4, csp5, csp6, csp7 = st.columns(5)
    with csp3:
        baud = st.number_input("Baud", min_value=1200, max_value=115200, step=300, value=int(cfg["baud"]), key="sc_baud")
    with csp4:
        bytesize = st.selectbox("Adatbit", options=[7,8], index=(0 if int(cfg["bytesize"])==7 else 1), key="sc_bytes")
    with csp5:
        parity = st.selectbox("Paritás", options=["N","E","O"], index=["N","E","O"].index(str(cfg["parity"]).upper()), key="sc_parity")
    with csp6:
        stopbits = st.selectbox("Stopbit", options=[1,2], index=(0 if int(cfg["stopbits"])==1 else 1), key="sc_stop")
    with csp7:
        timeout = st.number_input("Timeout (s)", min_value=0.1, max_value=5.0, step=0.1, value=float(cfg["timeout"]), key="sc_timeout")
    unit = st.selectbox("Mértékegység (kimenet)", options=["kg","g","lb","t"],
                        index=["kg","g","lb","t"].index(cfg["unit"]), key="sc_unit")

    b1, b2 = st.columns(2)
    with b1:
        if st.button("Mérleg beállítások mentése", key="sc_save"):
            chosen = manual_port
            if sel_label and sel_label != "(válassz)":
                chosen = devices[labels.index(sel_label)]
            set_scale_config(chosen, baud, bytesize, parity, stopbits, timeout, unit)
            st.success("Mérleg beállítások mentve.")
    with b2:
        if st.button("⚖️ Teszt beolvasás", key="sc_test"):
            ok, val, raw, msg = read_scale_value()
            if ok:
                st.success(f"Beolvasva: {val:.3f} kg")
            else:
                st.error(msg)
            if raw:
                st.code(raw, language="text")

    st.markdown("---")

    # PG
    st.subheader("PostgreSQL integráció (JSON → api.api_bontasi_naplo)")
    f = get_pg_conn_fields()
    if f.get("password_source") == "secrets":
        st.info("A PG kapcsolat **Streamlit Secrets**-ből jön. (Ajánlott Cloudon.)")
    elif os.getenv("PGPASSWORD") or os.getenv("PG_PASSWORD"):
        st.info("A PG jelszó **környezeti változóból** jön.")
    elif HAS_KEYRING:
        st.caption("Lokálon a jelszó a kulcstárban lehet tárolva (Keychain/Keyring).")

    c1, c2, c3, c4 = st.columns([1,1,1,1])
    with c1: host = st.text_input("Host", value=f["host"], key="pg_host_in")
    with c2: port = st.number_input("Port", min_value=1, value=int(f["port"] or 5432), step=1, key="pg_port_in")
    with c3: db   = st.text_input("Adatbázis", value=f["db"], key="pg_db_in")
    with c4: user = st.text_input("Felhasználó", value=f["user"], key="pg_user_in")

    if f.get("password_source") == "secrets":
        pw_placeholder = "(jelszó: Streamlit Secrets)"
    elif os.getenv("PGPASSWORD") or os.getenv("PG_PASSWORD"):
        pw_placeholder = "(jelszó: környezeti változó)"
    else:
        pw_placeholder = "(mentve a kulcstárban)" if f["password_saved"] else ""
    password_input = st.text_input("Jelszó", type="password", value="", placeholder=pw_placeholder, key="pg_pass_in")

    colpg1, colpg2, colpg3, colpg4 = st.columns([1,1,1,1])
    with colpg1:
        if st.button("Kapcsolat teszt (PG)", key="pg_test_btn"):
            pw = None
            if f.get("password_source") == "secrets": pw = st.secrets["pg"].get("password")
            if not pw: pw = password_input.strip() if password_input else None
            if not pw: pw = os.getenv("PGPASSWORD") or os.getenv("PG_PASSWORD")
            if not pw and HAS_KEYRING and host and db and user:
                try: pw = keyring.get_password("Bontasinaplo", _pg_key_id(host, int(port or 5432), db, user))
                except Exception: pw = None
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
            dispose_all_pg_engines()
            st.success("PG beállítások mentve és a korábbi poolok lezárva.")

    legacy_dsn = get_setting("pg_dsn", "")
    if legacy_dsn:
        cL1, cL2 = st.columns([1,3])
        with cL1:
            if st.button("Régi DSN törlése (settings)", key="wipe_legacy_dsn_btn"):
                set_setting("pg_dsn", ""); st.success("Régi DSN törölve.")
        with cL2:
            st.caption("Biztonság: jelszót nem tárolunk plain DSN-ben; Secrets/env/keyring a preferált.")

    stats = get_pg_outbox_stats()
    colpf1, colpf2 = st.columns([1,1])
    with colpf1:
        if st.button("Sor kiürítése most (Flush)", key="pg_flush_btn"):
            dsn, _, _ = get_pg_config()
            if not dsn:
                st.warning("Előbb állíts be PG kapcsolatot.")
            else:
                flush_pg_outbox(dsn, max_items=200)
                st.success("Flush lefutott.")
    with colpf2:
        st.write(f"🕒 Sorban: {stats['pending']} | 🔄 Processing: {stats['processing']} | ✅ Elküldve: {stats['sent']}")

    # Admin / pool státusz
    st.markdown("---")
    st.subheader("Admin • PostgreSQL pool / kapcsolatok")
    dsn, _, interval = get_pg_config()
    if not dsn:
        st.info("Nincs beállított DSN.")
    else:
        st.code(_mask_dsn(dsn), language="text")
        eng = get_or_create_pg_engine(dsn)
        size = checkedin = checkedout = overflow = None
        status_str = ""
        try:
            pool = eng.pool
            size = pool.size() if hasattr(pool, "size") else None
            checkedin = pool.checkedin() if hasattr(pool, "checkedin") else None
            checkedout = pool.checkedout() if hasattr(pool, "checkedout") else None
            overflow = pool.overflow() if hasattr(pool, "overflow") else None
            status_str = pool.status() if hasattr(pool, "status") else ""
        except Exception as e:
            status_str = f"Pool státusz hiba: {e}"

        m1, m2, m3, m4 = st.columns(4)
        with m1: st.metric("Pool méret", size if size is not None else "—")
        with m2: st.metric("Checked-in", checkedin if checkedin is not None else "—")
        with m3: st.metric("Checked-out", checkedout if checkedout is not None else "—")
        with m4: st.metric("Overflow", overflow if overflow is not None else "—")
        if status_str: st.caption(status_str)
        st.caption(f"Háttérküldő szál: {'fut' if st.session_state.get('pg_bg_started') else 'nincs'} • Intervallum: {interval} mp")

        cA, cB, cC = st.columns(3)
        with cA:
            if st.button("🔌 Ping (SELECT 1)", key="admin_ping_btn"):
                ok, msg = test_pg_connection(dsn); st.success("OK") if ok else st.error(msg)
        with cB:
            if st.button("♻️ Aktuális pool újranyitása", key="admin_dispose_one_btn"):
                dispose_pg_engine(dsn); st.success("Pool dispose-olva.")
        with cC:
            if st.button("🧹 Összes pool lezárása", key="admin_dispose_all_btn"):
                dispose_all_pg_engines(); st.success("Minden PG pool lezárva.")

    st.subheader("Küldési napló (utolsó 20)")
    try:
        log_df = pd.read_sql(
            "SELECT id, created_at, event, endpoint, http_status, status FROM sync_log ORDER BY id DESC LIMIT 20", engine)
        st.dataframe(log_df, use_container_width=True)
    except Exception:
        st.info("Még nincs küldési napló.")


def render_mappings_page():
    st.header("Törzsadatok és kapcsolások")

    # Állatok kezelése
    st.subheader("Állatok")
    df_anim = get_animals(only_active=False)
    if df_anim.empty:
        st.info("Még nincs állat törzs. (Seed létrejött induláskor.)")
    else:
        st.dataframe(df_anim.rename(columns={"name":"Állat","active":"Aktív"}), use_container_width=True, height=200)

    c1, c2 = st.columns([2,2])
    with c1:
        st.markdown("**Új állat felvétele**")
        new_an = st.text_input("Állat neve", key="add_animal_name")
        if st.button("➕ Hozzáadás", key="add_animal_btn"):
            ok, msg = add_animal(new_an)
            st.success(msg) if ok else st.error(msg)
            st.rerun()
    with c2:
        st.markdown("**Állat átnevezése**")
        if not df_anim.empty:
            old = st.selectbox("Válassz állatot", options=df_anim["name"].tolist(), key="ren_animal_old")
            newnm = st.text_input("Új név", key="ren_animal_new")
            prop = st.checkbox("Batches táblában is átírom", value=False, key="ren_animal_prop")
            if st.button("✏️ Átnevezés", key="ren_animal_btn"):
                ok, msg = rename_animal(old, newnm, propagate_batches=prop)
                st.success(msg) if ok else st.error(msg)
                st.rerun()

    # Aktív/Passzív kapcsoló
    if not df_anim.empty:
        st.markdown("**Állat aktiválása/deaktiválása**")
        a1, a2 = st.columns([2,1])
        with a1:
            an_sel = st.selectbox("Állat", options=df_anim["name"].tolist(), key="act_animal_sel")
        with a2:
            cur = df_anim[df_anim["name"] == an_sel]["active"].iloc[0] if not df_anim.empty else 1
            act = st.checkbox("Aktív", value=bool(cur), key="act_animal_cb")
        if st.button("Mentés", key="act_animal_save"):
            set_animal_active(an_sel, act)
            st.success("Állapot mentve.")
            st.rerun()

    st.markdown("---")

    # Részek kezelése
    st.subheader("Részek")
    anim_df = get_animals()
    anims = anim_df["name"].tolist() if not anim_df.empty else []
    if not anims:
        st.warning("Nincs aktív állat.")
        return
    sel_an = st.selectbox("Állat kiválasztása", options=anims, key="parts_animal_sel")
    df_parts = get_custom_parts(sel_an, only_active=False)
    if df_parts.empty:
        st.info("Ehhez az állathoz még nincs rész.")
    else:
        st.dataframe(df_parts.rename(columns={"name":"Rész","active":"Aktív"}), use_container_width=True, height=200)

    pcol1, pcol2, pcol3 = st.columns([2,2,2])
    with pcol1:
        new_part = st.text_input("Új rész neve", key="add_part_name")
        if st.button("➕ Rész hozzáadása", key="add_part_btn2"):
            ok, msg = add_custom_part(sel_an, new_part)
            st.success(msg) if ok else st.error(msg)
            st.rerun()
    with pcol2:
        if not df_parts.empty:
            oldp = st.selectbox("Átnevezendő rész", options=df_parts["name"].tolist(), key="ren_part_old")
            newp = st.text_input("Új név", key="ren_part_new")
            if st.button("✏️ Rész átnevezése", key="ren_part_btn"):
                ok, msg = rename_custom_part(sel_an, oldp, newp)
                st.success(msg) if ok else st.error(msg)
                st.rerun()
    with pcol3:
        if not df_parts.empty:
            deactp = st.selectbox("Deaktiválandó rész", options=df_parts["name"].tolist(), key="deact_part_name")
            if st.button("⏸️ Rész deaktiválása", key="deact_part_btn"):
                ok, msg = deactivate_custom_part(sel_an, deactp)
                st.success(msg) if ok else st.error(msg)
                st.rerun()

    # Másolás másik állattól
    st.markdown("**Részek másolása másik állatról**")
    mc1, mc2, mc3 = st.columns([2,2,1])
    with mc1:
        src = st.selectbox("Forrás állat", options=[a for a in anims if a != sel_an], key="copy_src_an")
    with mc2:
        incl_inact = st.checkbox("Inaktív részek is", value=False, key="copy_inact_cb")
    with mc3:
        if st.button("📥 Másolás", key="copy_parts_btn"):
            cnt = copy_parts_from_animal(src, sel_an, include_inactive=incl_inact)
            st.success(f"{cnt} rész másolva.")
            st.rerun()

    st.markdown("---")

    # ERP kapcsolások
    st.subheader("ERP kapcsolások")
    dsn, _, _ = get_pg_config()
    if not dsn:
        st.warning("Állíts be PostgreSQL kapcsolatot a Beállítások oldalon (ERP lekéréshez).")
        return

    erp_df = load_erp_items(dsn, ceg_id=3)
    if erp_df.empty:
        st.info("Nincs visszaadott ERP tétel (ellenőrizd a kapcsolatot/jogosultságot).")
        return

    id_col, name_col, code_col = pick_erp_columns(erp_df)
    st.caption(f"ERP oszlopok: azonosító = `{id_col}`, név = `{name_col}`, kód = `{code_col or '—'}`")

    # Part választás + ERP választás
    mp1, mp2 = st.columns([2,2])
    with mp1:
        part_options = get_custom_parts(sel_an, only_active=True)
        part_names = part_options["name"].tolist() if not part_options.empty else []
        part_sel = st.selectbox("Rész", options=part_names, key="map_part_sel")
    with mp2:
        q = st.text_input("ERP keresés", key="map_search_txt")

    # Szűrés ERP listában
    df_view = erp_df
    if q:
        ql = q.lower()
        masks = []
        for col in [id_col, name_col, code_col] if code_col else [id_col, name_col]:
            masks.append(df_view[col].astype(str).str.lower().str.contains(ql, na=False))
        if masks:
            mask = masks[0]
            for m in masks[1:]:
                mask = mask | m
            df_view = df_view[mask]
    df_view = df_view.head(200)

    st.dataframe(df_view[[c for c in [id_col, name_col, code_col] if c]].rename(
        columns={id_col:"ERP ID", name_col:"Megnevezés", (code_col or "—"):"Kód"}), use_container_width=True, height=260)

    # Kapcsolás
    if not df_view.empty and part_sel:
        erp_id = st.text_input("ERP ID (ha tudod pontosan)", key="map_erp_id_direct")
        pick_row_idx = st.number_input("Válassz sor indexet (0..)", min_value=0, max_value=int(len(df_view)-1),
                                       value=0, step=1, key="map_row_idx")
        picked = df_view.iloc[int(pick_row_idx)] if not df_view.empty else None
        picked_id = str(picked[id_col]) if picked is not None else None
        picked_name = str(picked[name_col]) if picked is not None else None
        picked_code = str(picked[code_col]) if (picked is not None and code_col) else None

        st.write(f"**Kiválasztott ERP:** ID={picked_id or '—'}, Megn.: {picked_name or '—'}, Kód: {picked_code or '—'}")

        if st.button("🔗 Kapcsolás (part → ERP)", key="map_link_btn"):
            use_id = erp_id.strip() if erp_id.strip() else picked_id
            if not use_id:
                st.warning("Nincs ERP ID megadva/kiválasztva.")
            else:
                upsert_mapping(sel_an, part_sel, use_id, erp_name=picked_name, erp_code=picked_code)
                st.success("Kapcsolás mentve.")
                st.rerun()

    # Meglévő kapcsolások listája
    st.subheader("Meglévő kapcsolások")
    mdf = get_mappings(sel_an)
    if mdf.empty:
        st.info("Még nincs kapcsolás ennél az állatnál.")
    else:
        st.dataframe(mdf.rename(columns={"animal":"Állat","part_name":"Rész","erp_id":"ERP ID","erp_name":"ERP név","erp_code":"ERP kód","created_at":"Idő"}), use_container_width=True, height=250)
        dc1, dc2 = st.columns([2,1])
        with dc1:
            del_part = st.selectbox("Kapcsolás törlése ehhez a részhez", options=mdf["part_name"].tolist(), key="map_del_part")
        with dc2:
            if st.button("🗑️ Kapcsolás törlése", key="map_del_btn"):
                delete_mapping(sel_an, del_part)
                st.success("Kapcsolás törölve.")
                st.rerun()


def render_attachments_page():
    st.header("Csatolmányok")
    batches = get_batches()
    if batches.empty:
        st.info("Még nincs tétel.")
        return
    colA, colB = st.columns([2,1])
    with colA:
        st.subheader("Válassz tételt")
        bid = st.selectbox("Tétel ID", batches["id"].tolist(), key="att_batch_sel")
    with colB:
        base_dir, subfolders, max_px, quality = get_attach_config()
        st.caption(f"Tárhely: `{base_dir}` • Max: {max_px}px • JPEG: {quality}%")

    active_batch = batches[batches.id == bid].iloc[0]
    atts = get_attachments(int(bid))
    if atts.empty:
        st.info("Nincs csatolmány.")
    else:
        img_cnt = sum(1 for _, a in atts.iterrows() if a['mime'] and str(a['mime']).startswith('image/'))
        pdf_cnt = sum(1 for _, a in atts.iterrows() if a['mime'] == 'application/pdf')
        st.write(f"📎 Összesen: {len(atts)} (képek: {img_cnt}, PDF: {pdf_cnt})")

        # Rácsos előnézet
        st.subheader("Előnézet")
        cols = st.columns(4)
        k = 0
        for _, a in atts.iterrows():
            col = cols[k % 4]; k += 1
            if a['mime'] and str(a['mime']).startswith('image/'):
                imgp = a.get('thumb_path') or a['path']
                col.image(imgp, caption=a.get('note') or a['created_at'], use_container_width=True)
            else:
                col.write(f"📄 {os.path.basename(a['path'])}")
                col.caption(a.get('note') or a['created_at'])

        st.subheader("Lista")
        show_df = atts[["id","created_at","kind","mime","note","path"]].copy()
        st.dataframe(show_df, use_container_width=True, height=300)

        # Törlés
        is_closed = (('status' in active_batch) and (active_batch['status'] == 'closed'))
        del_id = st.number_input("Törlendő csatolmány ID", min_value=0, step=1, value=0, key="att_del_id")
        if st.button("🗑️ Csatolmány törlése", key="att_del_btn", disabled=is_closed or del_id <= 0):
            if is_closed:
                st.warning("Lezárt tételből nem törölhetsz csatolmányt.")
            else:
                ok = delete_attachment(int(del_id))
                st.success("Törölve.") if ok else st.error("Nem található ID.")
                st.rerun()

    st.markdown("---")
    st.subheader("Új csatolmány")
    photo = st.camera_input("Fotó készítése", key="att_cam")
    upload = st.file_uploader("Feltöltés (JPG/PNG/PDF)", type=["jpg","jpeg","png","pdf"], accept_multiple_files=False, key="att_upload")
    note_att = st.text_input("Megjegyzés", key="att_note")
    is_closed = (('status' in active_batch) and (active_batch['status'] == 'closed'))
    if st.button("📎 Melléklet mentése", use_container_width=True, key="att_save_btn2", disabled=is_closed):
        file_to_save = None; mime = None; fname = None
        if photo is not None:
            file_to_save = photo.getvalue()
            mime = getattr(photo, 'type', 'image/jpeg'); fname = getattr(photo, 'name', 'camera.jpg')
        elif upload is not None:
            file_to_save = upload.getvalue()
            mime = getattr(upload, 'type', 'application/octet-stream'); fname = getattr(upload, 'name', 'file')
        if file_to_save:
            save_attachment(int(bid), file_to_save, fname, mime, kind="szallitolevel", note=note_att)
            st.success("Melléklet mentve."); st.rerun()
        else:
            st.warning("Nincs kiválasztott fotó vagy fájl.")


# --- Cím, menü, háttérszál ---
st.title("🥩 Bontásinapló – vizuális MVP")

if "page" not in st.session_state:
    st.session_state["page"] = "Rögzítés"

if "pg_bg_started" not in st.session_state:
    start_pg_bg_flusher()
    st.session_state["pg_bg_started"] = True

pages = ["Rögzítés", "Csatolmányok", "Törzsadatok & kapcsolások", "Beállítások"]
if HAS_OPT_MENU:
    try:
        _default_idx = pages.index(st.session_state["page"])
    except ValueError:
        _default_idx = 0
    _selected = option_menu(
        None,
        pages,
        icons=["clipboard-check", "images", "diagram-3", "gear"],
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
    c1, c2, c3, c4 = st.columns([1,1,1,1])
    if c1.button("Rögzítés", key="nav_rec_top", use_container_width=True): st.session_state["page"] = "Rögzítés"; st.rerun()
    if c2.button("Csatolmányok", key="nav_att_top", use_container_width=True): st.session_state["page"] = "Csatolmányok"; st.rerun()
    if c3.button("Törzsadatok & kapcsolások", key="nav_map_top", use_container_width=True): st.session_state["page"] = "Törzsadatok & kapcsolások"; st.rerun()
    if c4.button("Beállítások", key="nav_set_top", use_container_width=True): st.session_state["page"] = "Beállítások"; st.rerun()

page = st.session_state["page"]
if page == "Beállítások":
    render_settings_page(); st.stop()
elif page == "Törzsadatok & kapcsolások":
    render_mappings_page(); st.stop()
elif page == "Csatolmányok":
    render_attachments_page(); st.stop()


# --- Oldalsáv – aktív állat + új tétel ---
st.sidebar.header("Műveletek")

_anim_df = get_animals(only_active=True)
_anim_names = _anim_df["name"].tolist() if not _anim_df.empty else []
if not _anim_names:
    st.sidebar.warning("Nincs aktív állat. Létrehozás: Törzsadatok & kapcsolások → Állatok kezelése")
    st.stop()
allat = st.sidebar.selectbox("Állat", _anim_names, key="sidebar_animal_sel")

# alapérték az össztömeg number_input-hoz
if "ossztomeg_input" not in st.session_state:
    st.session_state["ossztomeg_input"] = 0.0

st.sidebar.subheader("Új bontási tétel")
with st.sidebar.form("uj_batch_form"):
    datum = st.date_input("Dátum", datetime.now())
    tetel_azon = st.text_input("Tétel/Lot azonosító")
    beszallito = st.text_input("Beszállító")
    eredet = st.text_input("Eredet/ENAR/állat azonosítás")
    ellenorzo = st.text_input("Ellenőrizte (név)")
    ossztomeg = st.number_input("Beérkezett össztömeg (kg)", min_value=0.0, step=0.1,
                                format="%0.2f", key="ossztomeg_input")
    megjegyzes = st.text_area("Megjegyzés")

    colf1, colf2 = st.columns(2)
    with colf1:
        read_gross = st.form_submit_button("⚖️ Mérleg beolvasása", use_container_width=True)
    with colf2:
        submitted = st.form_submit_button("Tétel mentése", use_container_width=True)

    if read_gross:
        ok, val, raw, msg = read_scale_value()
        if ok:
            st.session_state["ossztomeg_input"] = float(val)
            st.info(f"Beérkezett tömeg frissítve: {val:.3f} kg")
        else:
            st.warning(msg)

    if submitted:
        if float(st.session_state.get("ossztomeg_input") or 0.0) <= 0:
            st.warning("Az össztömeg legyen nagyobb mint 0.")
        else:
            new_id = save_batch({
                "datum": str(datum),
                "allat": allat,
                "tetel_azon": tetel_azon.strip() or None,
                "beszallito": beszallito.strip() or None,
                "eredet": eredet.strip() or None,
                "ellenorzo": ellenorzo.strip() or None,
                "ossztomeg": float(st.session_state["ossztomeg_input"]),
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

# --- Részek rögzítése ---
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
        # Mérleg beolvasás gomb
        if st.button("⚖️ Mérleg beolvasása", key="scale_read_part_btn", use_container_width=True, disabled=is_closed):
            ok, val, raw, msg = read_scale_value()
            if ok:
                st.session_state["tomeg_str"] = f"{val:.3f}"
                st.toast("Mérleg érték beolvasva.")
            else:
                st.warning(msg)

        # Nagy számbillentyűzet
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
                        if "." not in s: s = ("0." if s == "" else s + ".")
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

    st.subheader("Csatolmányok – összegzés")
    atts = get_attachments(int(active_batch.id))
    att_cnt = 0 if atts.empty else len(atts)
    img_cnt = 0 if atts.empty else sum(1 for _, a in atts.iterrows() if a['mime'] and str(a['mime']).startswith('image/'))
    st.metric("Összes csatolmány", att_cnt)
    st.metric("Képek", img_cnt)
    if st.button("📂 Csatolmányok megnyitása", key="open_attachments_page_btn"):
        st.session_state["page"] = "Csatolmányok"
        st.rerun()

st.subheader("Rögzített részek – összesítés")
parts_df = get_parts(int(active_batch.id))
summary_df = pd.DataFrame(columns=["resz","tomeg"])
if not parts_df.empty:
    summary_df = parts_df.groupby("resz", as_index=False)["tomeg"].sum().sort_values("tomeg", ascending=False)
st.dataframe(
    summary_df.rename(columns={"resz":"Rész","tomeg":"Tömeg (kg)"}),
    use_container_width=True,
    height=240
)

st.subheader("Rögzített részek – részletek")
if parts_df.empty:
    st.info("Még nincs rögzített rész ehhez a tételhez.")
else:
    show = parts_df[["id","created_at","resz","tomeg","megjegyzes"]].copy()
    show = show.rename(columns={"id":"ID","created_at":"Időpont","resz":"Rész","tomeg":"Tömeg (kg)","megjegyzes":"Megjegyzés"})
    st.dataframe(show, use_container_width=True, height=300)
    st.markdown("**Törlés (nyitott tételnél):**")
    for _, row in parts_df.iterrows():
        c1, c2, c3, c4, c5 = st.columns([2,2,2,3,2])
        c1.write(f"#{int(row['id'])}")
        c2.write(row["resz"])
        c3.write(f"{row['tomeg']:.2f} kg")
        c4.write(row.get("megjegyzes") or "—")
        if closed_batch:
            c5.button("🗑️", key=f"del_btn_{int(row['id'])}", disabled=True)
        else:
            if c5.button("🗑️", key=f"del_btn_{int(row['id'])}"):
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

# --- Lezárás + teljes küldés ---
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
