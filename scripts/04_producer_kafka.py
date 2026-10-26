"""
Etape 4 — PRODUCTEUR KAFKA
==========================
Lit le flux Wikimedia EventStreams, normalise chaque événement en un
message JSON minimal, et le publie dans un topic Kafka/Redpanda.

Partitionnement : par langue du wiki (clé = wiki), pour illustrer comment
un broker répartit les messages tout en garantissant l'ordre au sein d'une
même clé.

Note d'implémentation : le flux est lu directement avec `requests` en mode
streaming plutôt qu'avec la librairie `sseclient-py`, dont l'API s'est
révélée peu fiable selon les versions. On parse ici le protocole SSE
(Server-Sent Events) "à la main", ligne par ligne : chaque événement utile
est porté par une ligne commençant par "data:".

Le flux est filtré sur le wiki francophone (frwiki) avant envoi au topic,
comme les scripts 01 à 03 — ainsi tout ce qui circule en aval (topic
Kafka, script 05, dashboard, script 06) ne contient que des données
françaises, sans avoir à filtrer à nouveau à chaque étape.

Prérequis : docker compose up -d (voir docker-compose.yml)

Usage :
    python 04_producer_kafka.py --wiki frwiki
    python 04_producer_kafka.py --wiki all      # ne filtre aucun wiki
"""

import argparse
import json
import time

import requests
from kafka import KafkaProducer

# Nom correct du flux Wikimedia EventStreams (singulier).
STREAM_URL = "https://stream.wikimedia.org/v2/stream/recentchange"
BOOTSTRAP_SERVERS = "localhost:19092"
TOPIC = "wiki-edits"

HEADERS = {
    # Wikimedia bloque (403) les clients sans User-Agent identifiable :
    # https://meta.wikimedia.org/wiki/User-Agent_policy
    "User-Agent": "streaming-demo-cours/1.0 (https://github.com/epauldev/Demo-Data-Streaming)",
    "Accept": "text/event-stream",
    # Evite un 400 provoqué par la négociation de compression brotli
    # lorsque le paquet 'brotli' est installé localement.
    "Accept-Encoding": "identity",
}


def build_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        key_serializer=lambda k: k.encode("utf-8"),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        linger_ms=50,  # petit lot d'écriture pour l'efficacité, sans latence perceptible
    )


def normalize(change: dict) -> dict:
    return {
        "wiki": change.get("wiki"),
        "type": change.get("type"),
        "title": change.get("title"),
        "user": change.get("user"),
        "bot": bool(change.get("bot")),
        "timestamp": change.get("timestamp"),
        "size_new": (change.get("length") or {}).get("new"),
        "size_old": (change.get("length") or {}).get("old"),
    }


def iter_events(response):
    """Générateur qui extrait les payloads JSON des lignes 'data:' d'un
    flux SSE brut."""
    for line in response.iter_lines(decode_unicode=True):
        if not line or not line.startswith("data:"):
            continue
        payload = line[len("data:"):].strip()
        try:
            yield json.loads(payload)
        except json.JSONDecodeError:
            continue


RECONNECT_DELAY_SECONDS = 3


def stream_with_reconnect(headers):
    """Se connecte au flux et se RECONNECTE AUTOMATIQUEMENT en cas de
    coupure réseau (ProtocolError, ChunkedEncodingError, etc.) — un flux
    de longue durée se fait couper la connexion de temps en temps, c'est
    normal, un client robuste doit s'en remettre.

    La reprise se fait via le paramètre 'since' du flux Wikimedia, alimenté
    par l'horodatage du dernier événement reçu : même principe de CURSEUR
    que le script 01, pour limiter la perte de données au redémarrage.
    """
    since = None
    while True:
        url = STREAM_URL if not since else f"{STREAM_URL}?since={since}"
        try:
            with requests.get(url, stream=True, headers=headers, timeout=(5, 15)) as response:
                if response.status_code >= 400:
                    print(f"[DEBUG] Statut {response.status_code}")
                    print(f"[DEBUG] Corps de la réponse : {response.text[:500]}")
                response.raise_for_status()

                for change in iter_events(response):
                    since = (change.get("meta") or {}).get("dt") or since
                    yield change

        except requests.exceptions.RequestException as exc:
            print(f"[PRODUCER] Connexion interrompue ({exc.__class__.__name__}) — "
                  f"reconnexion dans {RECONNECT_DELAY_SECONDS}s"
                  + (f" (reprise depuis {since})" if since else "") + "...")
            time.sleep(RECONNECT_DELAY_SECONDS)


def main(wiki_filter: str):
    producer = build_producer()

    scope = "tous wikis" if wiki_filter == "all" else f"wiki:'{wiki_filter}'"
    print(f"[PRODUCER] Connecté à Redpanda ({BOOTSTRAP_SERVERS}), topic '{TOPIC}' ({scope})")
    print("[PRODUCER] Envoi des événements normalisés. Ctrl+C pour arrêter.\n")

    sent = 0
    t0 = time.time()

    for change in stream_with_reconnect(HEADERS):
        if wiki_filter != "all" and change.get("wiki") != wiki_filter:
            continue

        message = normalize(change)
        key = message["wiki"] or "unknown"
        producer.send(TOPIC, key=key, value=message)
        sent += 1

        if sent % 50 == 0:
            rate = sent / (time.time() - t0)
            print(f"[PRODUCER] {sent} messages envoyés ({rate:.1f} msg/s)")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--wiki", default="frwiki",
                         help="Code du wiki à filtrer avant envoi au topic "
                              "(frwiki par défaut, ou 'all' pour ne filtrer aucun wiki)")
    args = parser.parse_args()
    try:
        main(args.wiki)
    except KeyboardInterrupt:
        print("\nArrêt.")
