"""
Etape 4 — CONSOMMATEUR (persistance)
=====================================
Consomme le topic 'wiki-edits' et stocke chaque message dans SQLite.

Objectif : montrer qu'on peut brancher plusieurs consommateurs
indépendants sur le même topic (celui-ci, plus le dashboard du script
dashboard/app.py) sans qu'ils se gênent — c'est le découplage apporté par
le broker. Lancez ce script et le dashboard en parallèle du producteur.

Groupe de consommateurs : 'store-group'. Coupez puis relancez ce script
pour constater qu'il reprend là où il s'était arrêté (grâce aux offsets
gérés par Redpanda), contrairement à un flux SSE consommé sans broker.

Usage :
    python 05_consumer_store.py
"""

import json
import sqlite3
import time
from pathlib import Path

from kafka import KafkaConsumer

BOOTSTRAP_SERVERS = "localhost:19092"
TOPIC = "wiki-edits"
GROUP_ID = "store-group"
DB_PATH = Path(__file__).parent.parent / "data" / "wiki_edits.db"


def init_db(db_path: Path):
    db_path.parent.mkdir(exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS edits (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            wiki TEXT,
            type TEXT,
            title TEXT,
            user TEXT,
            bot INTEGER,
            timestamp INTEGER,
            size_new INTEGER,
            size_old INTEGER,
            received_at REAL
        )
    """)
    conn.commit()
    return conn


def main():
    conn = init_db(DB_PATH)
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id=GROUP_ID,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="latest",  # 'earliest' pour rejouer depuis le début
        enable_auto_commit=True,
    )

    print(f"[CONSUMER-STORE] Groupe '{GROUP_ID}' branché sur '{TOPIC}'")
    print(f"[CONSUMER-STORE] Persistance dans {DB_PATH}")
    print("[CONSUMER-STORE] Ctrl+C pour arrêter.\n")

    stored = 0
    for record in consumer:
        m = record.value
        conn.execute(
            "INSERT INTO edits (wiki, type, title, user, bot, timestamp, "
            "size_new, size_old, received_at) VALUES (?,?,?,?,?,?,?,?,?)",
            (m.get("wiki"), m.get("type"), m.get("title"), m.get("user"),
             int(bool(m.get("bot"))), m.get("timestamp"), m.get("size_new"),
             m.get("size_old"), time.time()),
        )
        conn.commit()
        stored += 1
        if stored % 50 == 0:
            print(f"[CONSUMER-STORE] {stored} messages persistés (partition={record.partition}, offset={record.offset})")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nArrêt.")
