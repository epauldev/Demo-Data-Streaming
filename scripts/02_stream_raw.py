"""
Etape 2 — STREAMING BRUT
========================
Se connecte au flux public Wikimedia EventStreams (Server-Sent Events) et
affiche chaque événement dès qu'il arrive.

Contrairement au script 01, on ne demande rien : le serveur pousse les
événements au fil de l'eau, dans l'ordre où ils se produisent. Pour rendre
la comparaison directe avec le script 01 (polling batch), ce script :
- filtre sur le même wiki par défaut (frwiki) ;
- affiche exactement les mêmes champs (titre, utilisateur, bot/humain,
  taille avant → après, horodatage).

Seule la MANIERE d'obtenir ces données change entre les deux scripts :
demandées périodiquement (01) vs poussées en continu (02). Le contenu
affiché, lui, est directement comparable.

Note d'implémentation : le flux est lu directement avec `requests` en mode
streaming plutôt qu'avec la librairie `sseclient-py`, dont l'API s'est
révélée peu fiable selon les versions. On parse ici le protocole SSE
(Server-Sent Events) "à la main", ligne par ligne : chaque événement utile
est porté par une ligne commençant par "data:".

Documentation du flux : https://wikitech.wikimedia.org/wiki/Event_Platform/EventStreams

Usage :
    python 02_stream_raw.py --wiki frwiki
    python 02_stream_raw.py --wiki all      # ne filtre aucun wiki
"""

import argparse
import json
import time

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
            print(f"[STREAM] Connexion interrompue ({exc.__class__.__name__}) — "
                  f"reconnexion dans {RECONNECT_DELAY_SECONDS}s...")
            time.sleep(RECONNECT_DELAY_SECONDS)


def print_change(change, count):
    """Affiche un événement avec les mêmes champs que le script 01
    (01_batch_poll.py), pour une comparaison directe."""
    title = change.get("title", "?")
    user = change.get("user", "?")
    bot = "bot" if change.get("bot") else "humain"
    length = change.get("length") or {}
    size_old = length.get("old", "?")
    size_new = length.get("new", "?")
    timestamp = change.get("timestamp", "?")

    print(f"[{count:05d}] '{title}' par {user} ({bot}) "
          f"| taille {size_old} → {size_new} | {timestamp}")


def main(wiki_filter: str):
    scope = "tous wikis" if wiki_filter == "all" else f"wiki:'{wiki_filter}'"
    print(f"[STREAM] Connexion à {STREAM_URL} ({scope}) ...")
    print("[STREAM] Les événements s'affichent au fur et à mesure. Ctrl+C pour arrêter.\n")

    count = 0
    for change in stream_with_reconnect(HEADERS):
        if wiki_filter != "all" and change.get("wiki") != wiki_filter:
            continue
        count += 1
        print_change(change, count)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--wiki", default="frwiki",
                         help="Code du wiki à filtrer (frwiki par défaut, "
                              "ou 'all' pour ne filtrer aucun wiki)")
    args = parser.parse_args()
    try:
        main(args.wiki)
    except KeyboardInterrupt:
        print("\nArrêt.")
