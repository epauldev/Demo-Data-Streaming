"""
Etape 1 — BATCH (contre-exemple)
================================
Interroge périodiquement l'API MediaWiki pour récupérer les dernières
modifications sur Wikipédia francophone. C'est le modèle "pull" classique :
on demande une photo de l'état du monde à intervalles réguliers.

Objectif pédagogique :
- montrer la latence introduite par le polling (on ne sait rien entre
  deux appels) ;
- montrer qu'une requête "top N" naïve, identique à chaque appel, peut
  être servie depuis le cache CDN de Wikimedia et donc sembler "figée" ;
- introduire la notion de CURSEUR (ici un horodatage), qui rend chaque
  requête différente et garantit qu'on récupère bien tout ce qui est
  arrivé depuis le dernier appel, sans trou ni doublon. C'est le même
  principe que l'offset Kafka utilisé plus loin (scripts 05 et 06) —
  bon parallèle à faire avec les étudiants.
- servir de point de comparaison avec le script 02 (flux poussé en continu).

Usage :
    python 01_batch_poll.py --interval 5
"""

import argparse
import time
from datetime import datetime, timezone

import requests

API_URL = "https://fr.wikipedia.org/w/api.php"

# L'API Wikimedia rejette (HTTP 403) les requêtes sans User-Agent identifiant
# clairement le client, conformément à sa politique d'étiquette :
# https://meta.wikimedia.org/wiki/User-Agent_policy
HEADERS = {
    "User-Agent": "streaming-demo-cours/1.0 (https://github.com/epauldev/Demo-Data-Streaming)"
}


def fetch_initial_snapshot(limit: int = 1):
    """Premier appel : récupère le(s) dernier(s) changement(s) pour
    initialiser le curseur temporel."""
    params = {
        "action": "query",
        "list": "recentchanges",
        "rcprop": "ids|title|user|timestamp|sizes|flags",
        "rclimit": limit,
        "rcdir": "older",  # ordre par défaut : du plus récent au plus ancien
        "format": "json",
        "formatversion": 2,
    }
    response = requests.get(API_URL, params=params, headers=HEADERS, timeout=10)
    response.raise_for_status()
    return response.json()["query"]["recentchanges"]


def fetch_since(rcstart: str, limit: int = 50):
    """Appels suivants : ne récupère que ce qui est arrivé APRES rcstart.
    C'est ce paramètre, qui change à chaque appel, qui évite le cache CDN
    et garantit qu'on ne rate rien."""
    params = {
        "action": "query",
        "list": "recentchanges",
        "rcprop": "ids|title|user|timestamp|sizes|flags",
        "rclimit": limit,
        "rcdir": "newer",   # du plus ancien au plus récent, à partir de rcstart
        "rcstart": rcstart,
        "format": "json",
        "formatversion": 2,
    }
    response = requests.get(API_URL, params=params, headers=HEADERS, timeout=10)
    response.raise_for_status()
    return response.json()["query"]["recentchanges"]


def print_changes(changes, seen_ids):
    printed = 0
    for c in changes:
        rcid = c.get("rcid")
        if rcid in seen_ids:
            continue  # filet de sécurité contre un doublon sur la borne du curseur
        seen_ids.add(rcid)
        bot = "bot" if c.get("bot") else "humain"
        print(f"    - {c['title']} par {c['user']} ({bot})")
        printed += 1
    return printed


def main(interval: int):
    print(f"[BATCH] Interrogation de l'API toutes les {interval}s (avec curseur). "
          "Ctrl+C pour arrêter.\n")

    seen_ids = set()

    # Appel initial : on prend juste le point de départ, sans tout afficher
    # comme "nouveau" (ce serait potentiellement des événements vieux de
    # plusieurs minutes).
    initial = fetch_initial_snapshot(limit=1)
    cursor = initial[0]["timestamp"] if initial else datetime.now(timezone.utc).isoformat()
    for c in initial:
        seen_ids.add(c.get("rcid"))
    print(f"[INIT] Curseur initialisé à {cursor}\n")

    while True:
        t0 = time.time()
        changes = fetch_since(cursor)
        now_str = datetime.now(timezone.utc).strftime("%H:%M:%S")

        count = print_changes(changes, seen_ids) if changes else 0

        if changes:
            cursor = changes[-1]["timestamp"]  # avance le curseur au plus récent reçu

        print(f"[{now_str}] Appel terminé en {time.time() - t0:.2f}s "
              f"→ {count} nouvelle(s) modification(s) depuis le dernier appel "
              f"(curseur = {cursor})\n")

        time.sleep(interval)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--interval", type=int, default=5,
                         help="Intervalle entre deux appels, en secondes")
    args = parser.parse_args()
    try:
        main(args.interval)
    except KeyboardInterrupt:
        print("\nArrêt.")
