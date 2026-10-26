"""
Etape 6 — LAG ET BACKPRESSURE
==============================
Consommateur volontairement ralenti pour rendre visible :
- le LAG (nombre de messages en attente non encore consommés) ;
- l'accumulation dans le broker pendant qu'un consommateur est lent
  (contrairement à un flux SSE brut, où les messages non traités seraient
  simplement perdus) ;
- la notion de garantie "at-least-once" : si ce script plante après le
  traitement mais avant le commit d'offset, le message sera retraité au
  redémarrage → à discuter avec les étudiants (idempotence du traitement).

Le lag est calculé ainsi, pour chaque partition assignée :
    lag = offset de fin du topic − position courante du consommateur
Il est affiché en console toutes les 5 messages traités, et visible en
parallèle dans la Console Redpanda (http://localhost:8080), onglet
"Consumers" du topic, sous le groupe 'lag-demo-group'.

Déroulé suggéré en classe :
1. Lancer le producteur (04_producer_kafka.py) seul pendant 30 s pour que
   des messages s'accumulent sans consommateur actif.
2. Lancer ce script avec un traitement nettement plus lent que le flux
   (par exemple --sleep 2) : observer le lag élevé au démarrage, qui se
   résorbe lentement, ou continue de croître si le débit d'arrivée dépasse
   le débit de traitement (backpressure).
3. Couper le producteur : le lag diminue jusqu'à 0 (rattrapage).

Remarque : 'auto_offset_reset="earliest"' ne s'applique qu'à un groupe qui
n'a encore aucun offset committé. Pour rejouer la démo depuis le début du
topic, utilisez un autre nom de groupe (--group) ou supprimez le groupe
depuis la Console Redpanda.

Usage :
    python 06_consumer_lag_demo.py --sleep 2
    python 06_consumer_lag_demo.py --sleep 2 --group lag-demo-group-2
"""

import argparse
import json
import time

from kafka import KafkaConsumer

BOOTSTRAP_SERVERS = "localhost:19092"
TOPIC = "wiki-edits"
DEFAULT_GROUP_ID = "lag-demo-group"


def compute_lag(consumer: KafkaConsumer) -> int:
    """Somme, sur les partitions assignées, de (offset de fin − position)."""
    partitions = list(consumer.assignment())
    if not partitions:
        return 0
    end_offsets = consumer.end_offsets(partitions)
    lag = 0
    for tp in partitions:
        lag += max(0, end_offsets[tp] - consumer.position(tp))
    return lag


def main(sleep_seconds: float, group_id: str):
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id=group_id,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="earliest",
        enable_auto_commit=False,  # commit manuel pour illustrer at-least-once
    )

    print(f"[LAG-DEMO] Groupe '{group_id}' branché sur '{TOPIC}' ({BOOTSTRAP_SERVERS})")
    print(f"[LAG-DEMO] Consommateur volontairement lent (pause de {sleep_seconds}s par message)")
    print("[LAG-DEMO] En attente de messages. Ctrl+C pour arrêter.\n")

    processed = 0
    for record in consumer:
        # Traitement simulé, volontairement lent
        time.sleep(sleep_seconds)
        processed += 1

        # Commit manuel APRES traitement -> sémantique at-least-once
        consumer.commit()

        if processed % 5 == 0:
            lag = compute_lag(consumer)
            print(f"[LAG-DEMO] {processed} messages traités "
                  f"(partition={record.partition}, offset={record.offset}) "
                  f"| LAG actuel = {lag} messages en attente")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--sleep", type=float, default=2.0,
                         help="Pause simulée par message traité, en secondes")
    parser.add_argument("--group", default=DEFAULT_GROUP_ID,
                         help="Nom du groupe de consommateurs (changez-le pour "
                              "rejouer depuis le début du topic)")
    args = parser.parse_args()
    try:
        main(args.sleep, args.group)
    except KeyboardInterrupt:
        print("\nArrêt.")
