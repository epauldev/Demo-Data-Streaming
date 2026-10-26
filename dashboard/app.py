"""
Etape 5 — DASHBOARD TEMPS REEL (Streamlit)
============================================
Consomme le topic 'wiki-edits' (filtré sur frwiki par le producteur) et
affiche un tableau de bord rafraîchi en continu.

Avec une seule source (frwiki), un histogramme "par wiki" n'a pas
d'intérêt : on met plutôt en avant ce qui varie réellement au sein d'un
même wiki :
- les contributeurs les plus actifs, humains et bots séparés (les bots
  Wikipédia font souvent des dizaines d'éditions répétitives, ce qui vaut
  la peine d'être visualisé distinctement) ;
- le débit d'éditions dans le temps (démonstratif du caractère continu du
  flux, contrairement à un histogramme statique) ;
- la répartition par type de modification (nouvelle page, modification,
  etc.).

Ce dashboard tourne dans son propre groupe de consommateurs ('dashboard-
group'), indépendant de celui du script 05_consumer_store.py : les deux
peuvent lire le même flux sans interférer, ce qui illustre le découplage
apporté par le broker.

Note d'implémentation : le rafraîchissement automatique utilise
@st.fragment(run_every=...), qui ne recharge que la portion "vivante" de
la page (métriques, graphiques) plutôt que la page entière. L'ancienne
approche (time.sleep + st.rerun() sur tout le script) provoquait un
clignotement visible à chaque cycle car elle rechargeait tout, y compris
le titre et la mise en page, qui n'ont pourtant pas besoin de bouger.

Usage :
    streamlit run app.py
"""

import time
import json
from collections import deque

import pandas as pd
import streamlit as st
from kafka import KafkaConsumer

BOOTSTRAP_SERVERS = "localhost:19092"
TOPIC = "wiki-edits"
GROUP_ID = "dashboard-group"
MAX_HISTORY = 500       # nombre d'événements récents gardés en mémoire pour l'affichage
REFRESH_SECONDS = 2     # intervalle de rafraîchissement du fragment (2-3s = bon compromis)

st.set_page_config(page_title="Streaming Wikimedia — démo", layout="wide")
st.title("📡 Démo streaming — éditions Wikipédia francophone en temps réel")
st.caption("Source : Wikimedia EventStreams (frwiki) → Redpanda (Kafka) → ce dashboard")


@st.cache_resource
def get_consumer():
    return KafkaConsumer(
        TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id=GROUP_ID,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="latest",
        enable_auto_commit=True,
        consumer_timeout_ms=1000,  # évite de bloquer indéfiniment
    )


if "history" not in st.session_state:
    st.session_state.history = deque(maxlen=MAX_HISTORY)

consumer = get_consumer()


@st.fragment(run_every=f"{REFRESH_SECONDS}s")
def render_dashboard():
    """Seule cette fonction est ré-exécutée automatiquement toutes les
    REFRESH_SECONDS secondes — le reste de la page (titre, légende)
    ne bouge pas, d'où l'absence de clignotement global."""
    now = time.time()
    records = consumer.poll(timeout_ms=1000, max_records=200)
    new_count = 0
    for tp, batch in records.items():
        for record in batch:
            message = dict(record.value)
            message["received_at"] = now  # horloge locale, pour calculer le débit
            st.session_state.history.append(message)
            new_count += 1

    history = list(st.session_state.history)

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Messages en mémoire (fenêtre glissante)", len(history))
    col2.metric("Nouveaux messages ce rafraîchissement", new_count)

    if not history:
        st.info("En attente de messages... vérifiez que 04_producer_kafka.py tourne bien.")
        return

    df = pd.DataFrame(history)

    bot_share = df["bot"].mean() * 100
    col3.metric("Part de bots", f"{bot_share:.0f}%")

    span = df["received_at"].max() - df["received_at"].min()
    rate = (len(df) / span) if span > 0 else 0.0
    col4.metric("Débit moyen (fenêtre affichée)", f"{rate:.2f} éditions/s")

    st.subheader("👤🤖 Contributeurs les plus actifs")
    c1, c2 = st.columns(2)
    with c1:
        st.caption("Humains")
        human_counts = df.loc[~df["bot"], "user"].value_counts().head(10)
        if not human_counts.empty:
            st.bar_chart(human_counts)
        else:
            st.info("Aucune édition humaine dans la fenêtre actuelle.")
    with c2:
        st.caption("Bots")
        bot_counts = df.loc[df["bot"], "user"].value_counts().head(10)
        if not bot_counts.empty:
            st.bar_chart(bot_counts)
        else:
            st.info("Aucune édition de bot dans la fenêtre actuelle.")

    st.subheader("📈 Débit d'éditions (par tranche de 5 secondes)")
    df["bucket"] = (df["received_at"] // 5 * 5).astype(int)
    rate_series = df.groupby("bucket").size()
    rate_series.index = pd.to_datetime(rate_series.index, unit="s")
    st.line_chart(rate_series)

    st.subheader("🏷️ Répartition par type de modification")
    st.bar_chart(df["type"].value_counts())

    st.subheader("Dernières modifications reçues")
    st.dataframe(
        df[["user", "type", "title", "bot", "size_old", "size_new"]].tail(20).iloc[::-1],
        use_container_width=True,
    )


render_dashboard()
