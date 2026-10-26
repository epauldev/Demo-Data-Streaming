"""
Etape 3 — TRAITEMENT A LA VOLEE (fenêtrage)
============================================
Filtre le flux sur un wiki donné et agrège les événements par fenêtre
temporelle fixe (tumbling window), pour introduire :
- le filtrage en continu (par opposition au filtrage a posteriori sur un
  fichier déjà complet) ;
- la notion de fenêtre temporelle (tumbling window) ;
- processing time (l'horloge de notre machine) vs event time (l'horodatage
  fourni par l'événement lui-même) — les deux sont affichés pour comparaison.

Note d'implémentation : le flux est lu directement avec `requests` en mode
streaming plutôt qu'avec la librairie `sseclient-py`, dont l'API s'est
révélée peu fiable selon les versions. On parse ici le protocole SSE
(Server-Sent Events) "à la main", ligne par ligne : chaque événement utile
est porté par une ligne commençant par "data:".

Usage :
    python 03_stream_windowed.py --wiki frwiki --window 10
"""

import argparse
import json
import time
from collections import Counter
from datetime import datetime, timezone

import requests

# Nom correct du flux Wikimedia EventStreams (singulier).
STREAM_URL = "https://stream.wikimedia.org/v2/stream/recentchange"

HEADERS = {
    # Wikimedia bloque (403) les clients sans User-Agent identifiable :
    # https://meta.wikimedia.org/wiki/User-Agent_policy
    "User-Agent": "streaming-demo-cours/1.0 (https://github.com/epauldev/Demo-Data-Streaming)",
    "Accept": "text/event-stream",
    # Evite un 400 provoqué par la négociation de compression brotli
    # lorsque le paquet 'brotli' est installé localement.
    "Accept-Encoding": "identity",
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
    coupure réseau — un flux de longue durée se fait couper la connexion
    de temps en temps, c'est normal, un client robuste doit s'en remettre.
    Reprend via le paramètre 'since', alimenté par l'horodatage du dernier
    événement reçu (même principe de curseur que le script 01)."""
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
            print(f"[WINDOWED] Connexion interrompue ({exc.__class__.__name__}) — "
                  f"reconnexion dans {RECONNECT_DELAY_SECONDS}s...")
            time.sleep(RECONNECT_DELAY_SECONDS)


def main(wiki_filter: str, window_seconds: int):
    print(f"[WINDOWED] Filtre = wiki:'{wiki_filter}' | fenêtre = {window_seconds}s")
    print("[WINDOWED] Ctrl+C pour arrêter.\n")

    window_start = time.time()
    edit_count = 0
    bot_count = 0
    users = Counter()

    for change in stream_with_reconnect(HEADERS):
        if change.get("wiki") != wiki_filter:
            continue

        edit_count += 1
        if change.get("bot"):
            bot_count += 1
        users[change.get("user", "?")] += 1

        now = time.time()
        if now - window_start >= window_seconds:
            ts = datetime.now(timezone.utc).strftime("%H:%M:%S")
            top_user = users.most_common(1)[0] if users else ("-", 0)
            bot_pct = (bot_count / edit_count * 100) if edit_count else 0
            print(f"[{ts}] Fenêtre de {window_seconds}s : "
                  f"{edit_count} éditions | {bot_pct:.0f}% par des bots | "
                  f"contributeur le plus actif : {top_user[0]} ({top_user[1]})")

            window_start = now
            edit_count = 0
            bot_count = 0
            users.clear()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--wiki", default="frwiki",
                         help="Code du wiki à filtrer (frwiki, enwiki, dewiki, wikidatawiki, ...)")
    parser.add_argument("--window", type=int, default=10,
                         help="Taille de la fenêtre en secondes")
    args = parser.parse_args()
    try:
        main(args.wiki, args.window)
    except KeyboardInterrupt:
        print("\nArrêt.")
