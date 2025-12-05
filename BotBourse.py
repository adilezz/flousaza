import os
import sqlite3
import datetime
from datetime import date, timedelta
import pandas as pd
import requests
import casabourse as cb
from concurrent.futures import ThreadPoolExecutor, as_completed

# --- 1. CONFIGURATION & STRATÉGIE ---

# Paramètres Système
DB_NAME = "bourse_casa.db"
BOT_TOKEN = os.environ.get("BOT_TOKEN")
CHAT_ID = os.environ.get("CHAT_ID")

# Paramètres Investisseur (DCA)
BUDGET_MENSUEL = 4000.0  # Votre apport mensuel en MAD
MIN_VOLUME_MAD = 5000.0  # Filtre de liquidité minimum

# Whitelist "Bon Père de Famille" (Valeurs de Rendement & Croissance Sûre)
# Ces actions sont priorisées pour le DCA si elles sont en tendance haussière.
YIELD_STOCKS = [
    'IAM',  # Maroc Telecom (Rendement)
    'BCP',  # Banque Populaire (Solide)
    'ATW',  # Attijariwafa Bank (Leader)
    'CIM',  # Ciments du Maroc (Dividende)
    'LHM',  # LafargeHolcim (Construction)
    'MSA',  # Marsa Maroc (Croissance + Div)
    'COS',  # Cosumar (Défensive)
    'TQM',  # Taqa Morocco (Utilities)
]

# Blacklist (Actions à éviter : illiquides, spéculatives ou données erratiques)
BLACKLIST = ['ZDJ', 'DLM', 'IBM', 'SOP', 'NEJ']

# --- 2. GESTION DE LA BASE DE DONNÉES (SOCLE) ---

def get_db_connection():
    return sqlite3.connect(DB_NAME)

def init_db():
    """Initialise la structure de la base de données."""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Table Instruments
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS instruments (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT NOT NULL UNIQUE,
            name TEXT
        )
    """)
    
    # Table Historique
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS historical_quotes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            instrument_id INTEGER,
            Date TEXT,
            "Dernier_cours" REAL,
            "Volume" REAL,
            UNIQUE(instrument_id, Date)
        )
    """)
    conn.commit()
    conn.close()

def get_latest_session_date():
    """Récupère la dernière date enregistrée en base."""
    conn = get_db_connection()
    try:
        row = conn.execute("SELECT MAX(Date) FROM historical_quotes").fetchone()
        return row[0] if row and row[0] else None
    except Exception:
        return None
    finally:
        conn.close()

def clean_number(txt):
    """Nettoie les formats numériques (virgules, espaces, tirets)."""
    if not txt: return 0.0
    clean = str(txt).replace(' ', '').replace('%', '').replace(',', '.')
    if '--' in clean or clean in ['-', '']: return 0.0
    try:
        return float(clean)
    except Exception:
        return 0.0

# --- 3. MOTEUR D'ACQUISITION (SCRAPING INTELLIGENT) ---

def sync_instruments():
    """Synchronise la liste des actions."""
    try:
        df = cb.get_available_instrument()
        # On ne garde que les tickers à 3 lettres (Actions standards)
        df_actions = df[df["Symbole"].astype(str).str.len() == 3].copy()
    except Exception as e:
        print(f"⚠️ Erreur récupération instruments: {e}")
        return []

    conn = get_db_connection()
    instruments = []
    
    for _, row in df_actions.iterrows():
        sym = str(row["Symbole"]).strip()
        if sym in BLACKLIST: continue
        
        name = str(row["Nom"]).strip() if "Nom" in row else sym
        
        # Mise à jour ou Insertion
        conn.execute("INSERT OR IGNORE INTO instruments (symbol, name) VALUES (?, ?)", (sym, name))
        
        # Récupérer l'ID
        inst_id = conn.execute("SELECT id FROM instruments WHERE symbol = ?", (sym,)).fetchone()[0]
        instruments.append({"id": inst_id, "symbol": sym, "name": name})
        
    conn.commit()
    conn.close()
    return instruments

def fetch_worker(inst, start_s, end_s):
    """Fonction exécutée par les threads pour récupérer la data."""
    sym = inst["symbol"]
    try:
        df = cb.get_historical_data_auto(sym, start_s, end_s)
        if df is None or df.empty or "Date" not in df.columns:
            return None
            
        # Standardisation des colonnes
        df["Date"] = pd.to_datetime(df["Date"]).dt.strftime("%Y-%m-%d")
        return (inst["id"], df)
    except Exception:
        return None

def update_market_data():
    """Orchestre la mise à jour (Delta + Multithreading)."""
    print("🔄 Initialisation de la mise à jour...")
    init_db()
    instruments = sync_instruments()
    
    last_date = get_latest_session_date()
    
    # Définition de la fenêtre de tir
    if last_date:
        start_dt = datetime.datetime.strptime(last_date, "%Y-%m-%d").date() + timedelta(days=1)
    else:
        # Première initialisation : 2 ans d'historique suffisent pour la SMA200
        start_dt = date.today() - timedelta(days=730)
        print("🆕 Initialisation complète (2 ans d'historique).")

    end_dt = date.today()
    
    if start_dt > end_dt:
        print("✅ Base déjà à jour.")
        return 0

    print(f"📥 Téléchargement des données : {start_dt} -> {end_dt}")
    start_s = start_dt.strftime("%Y-%m-%d")
    end_s = end_dt.strftime("%Y-%m-%d")
    
    total_rows = 0
    conn = get_db_connection()
    
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = {executor.submit(fetch_worker, i, start_s, end_s): i for i in instruments}
        
        for future in as_completed(futures):
            res = future.result()
            if res:
                inst_id, df = res
                for _, row in df.iterrows():
                    close = clean_number(row.get("Dernier cours", 0))
                    vol = clean_number(row.get("Volume", 0))
                    
                    try:
                        conn.execute("""
                            INSERT OR IGNORE INTO historical_quotes (instrument_id, Date, "Dernier_cours", "Volume")
                            VALUES (?, ?, ?, ?)
                        """, (inst_id, row["Date"], close, vol))
                        total_rows += 1
                    except Exception:
                        pass
    
    conn.commit()
    conn.close()
    print(f"✅ Mise à jour terminée. +{total_rows} nouvelles cotations.")
    return total_rows

# --- 4. ANALYSE INVESTISSEUR (LE CERVEAU) ---

def get_stock_history(symbol, conn, limit=300):
    query = """
        SELECT h.Date, h."Dernier_cours" as close, h."Volume" as volume
        FROM historical_quotes h
        JOIN instruments i ON h.instrument_id = i.id
        WHERE i.symbol = ?
        ORDER BY h.Date ASC
    """ # Pas de LIMIT ici pour avoir tout l'historique pour la SMA200, on coupe après
    try:
        df = pd.read_sql_query(query, conn, params=(symbol,))
        if not df.empty:
            df['close'] = df['close'].apply(clean_number)
            df['volume'] = df['volume'].apply(clean_number)
            # On ne garde que la fin après nettoyage
            return df.tail(limit).reset_index(drop=True)
    except Exception:
        pass
    return pd.DataFrame()

def analyze_portfolio():
    """Analyse le marché pour trouver les opportunités DCA."""
    session_date = get_latest_session_date()
    if not session_date: return None

    conn = get_db_connection()
    # Récupérer tous les instruments actifs
    instruments = conn.execute("SELECT symbol, name FROM instruments").fetchall()
    
    analysis_results = []
    market_trend_bullish = 0
    total_analyzed = 0

    print(f"🧠 Analyse 'Bon Père de Famille' du {session_date}...")

    for symbol, name in instruments:
        df = get_stock_history(symbol, conn)
        if len(df) < 200: continue # Pas assez de données pour SMA200
        
        last = df.iloc[-1]
        
        # Filtre liquidité (si volume moyen < seuil, on ignore)
        avg_volume = df['volume'].tail(20).mean()
        if avg_volume < MIN_VOLUME_MAD: continue

        total_analyzed += 1

        # --- INDICATEURS CLÉS ---
        # 1. Tendance Long Terme (SMA 200)
        sma200 = df['close'].rolling(window=200).mean().iloc[-1]
        trend = "HAUSSIER" if last['close'] > sma200 else "BAISSIER"
        
        if trend == "HAUSSIER":
            market_trend_bullish += 1

        # 2. Performance Court Terme (Pour détecter le "Dip")
        # Variation sur 1 semaine (5 séances)
        price_1w_ago = df['close'].iloc[-6] if len(df) >= 6 else last['close']
        perf_week = ((last['close'] - price_1w_ago) / price_1w_ago) * 100
        
        # 3. Stratégie de Notation (Score / 10)
        score = 0
        reasons = []

        # A. Priorité aux valeurs sûres (Whitelist)
        if symbol in YIELD_STOCKS:
            score += 3
            reasons.append("💎 Valeur de Rendement")

        # B. Tendance de fond obligatoire pour acheter
        if trend == "HAUSSIER":
            score += 3
        else:
            score -= 5 # On n'achète pas en tendance baissière

        # C. "Buy the Dip" : Bonus si baisse récente dans une tendance haussière
        if trend == "HAUSSIER" and -5.0 < perf_week < -1.0:
            score += 2
            reasons.append(f"📉 Soldes ({perf_week:.1f}% sur 1 sem)")
        
        # D. Proximité SMA 200 (Point d'entrée idéal)
        dist_sma = (last['close'] - sma200) / sma200
        if 0 < dist_sma < 0.05: # Prix entre 0 et 5% au dessus de la SMA200
            score += 2
            reasons.append("⭐ Support SMA200 proche")

        if score > 0:
            analysis_results.append({
                "symbol": symbol,
                "name": name,
                "close": last['close'],
                "trend": trend,
                "perf_week": perf_week,
                "score": score,
                "reasons": reasons
            })

    conn.close()
    
    # Calcul Météo Marché
    bullish_ratio = (market_trend_bullish / total_analyzed) * 100 if total_analyzed > 0 else 0
    market_status = "NEUTRE"
    if bullish_ratio > 60: market_status = "🟢 HAUSSIER (Favorable)"
    elif bullish_ratio < 40: market_status = "🔴 BAISSIER (Prudence)"

    # Tri des résultats (Meilleur score d'abord)
    analysis_results.sort(key=lambda x: x['score'], reverse=True)
    
    return {
        "date": session_date,
        "market_status": market_status,
        "bullish_pct": bullish_ratio,
        "top_picks": analysis_results[:3], # Top 3 seulement
        "risks": [res for res in analysis_results if res['perf_week'] < -10] # Alerte si crash > 10%
    }

# --- 5. NOTIFICATION & RAPPORT (L'INTERFACE) ---

def generate_report(data):
    """Génère un message Telegram lisible et orienté action."""
    if not data: return "❌ Pas de données disponibles."
    
    date_report = datetime.datetime.strptime(data['date'], "%Y-%m-%d").strftime("%d/%m/%Y")
    
    # En-tête Météo
    msg = [
        f"📅 **CONSEIL INVESTISSEUR - {date_report}**",
        f"🌍 **Météo Marché** : {data['market_status']}",
        f"📊 {data['bullish_pct']:.0f}% des actions sont en tendance haussière.",
        "",
        f"💰 **Allocation du Mois ({BUDGET_MENSUEL:,.0f} MAD)**",
        "Voici comment répartir votre apport aujourd'hui :"
    ]
    
    # Allocation DCA
    budget_remaining = BUDGET_MENSUEL
    
    if not data['top_picks']:
        msg.append("😴 Rien d'intéressant aujourd'hui. Gardez votre cash.")
    else:
        # Répartition simple : 60% Top 1, 40% Top 2 (ou 100% si un seul)
        allocations = [0.6, 0.4] if len(data['top_picks']) >= 2 else [1.0]
        
        for i, stock in enumerate(data['top_picks']):
            if i >= len(allocations): break
            
            amount = BUDGET_MENSUEL * allocations[i]
            qty = int(amount // stock['close'])
            cost = qty * stock['close']
            
            if qty > 0:
                icon = "🥇" if i == 0 else "🥈"
                reasons_str = ", ".join(stock['reasons'])
                msg.append(
                    f"\n{icon} **{stock['name']} ({stock['symbol']})**"
                )
                msg.append(f"   🛒 **Acheter {qty} actions** à {stock['close']} MAD")
                msg.append(f"   💳 Total : {cost:,.0f} MAD")
                msg.append(f"   💡 *Pourquoi ?* {reasons_str}")
            
            budget_remaining -= cost

    # Alerte Risque
    if data['risks']:
        msg.append("\n⚠️ **Alertes Chute (>10%)**")
        for r in data['risks'][:2]: # Max 2 alertes
            msg.append(f"🔻 {r['symbol']}: {r['perf_week']:.1f}% sur 1 semaine.")

    # Footer Technique
    msg.append("\n------------------")
    msg.append(f"🤖 *BotBourse v2.0* | Base à jour : {data['date']}")
    
    return "\n".join(msg)

def send_telegram(message):
    print("\n📤 --- ENVOI TELEGRAM ---")
    print(message) # Log console
    
    if not BOT_TOKEN or not CHAT_ID:
        print("⚠️ Pas de token Telegram configuré. Sortie console uniquement.")
        return

    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload = {"chat_id": CHAT_ID, "text": message, "parse_mode": "Markdown"}
    
    try:
        r = requests.post(url, json=payload)
        if r.status_code != 200:
            print(f"❌ Erreur Telegram: {r.text}")
    except Exception as e:
        print(f"❌ Erreur connexion: {e}")

# --- 6. POINT D'ENTRÉE ---

def main():
    try:
        # 1. Mise à jour des données
        updated_rows = update_market_data()
        
        # 2. Analyse Investissement
        analysis = analyze_portfolio()
        
        # 3. Génération & Envoi Rapport
        if analysis:
            report = generate_report(analysis)
            send_telegram(report)
        else:
            print("⚠️ Analyse impossible (pas de données récentes ?)")
            
    except Exception as e:
        print(f"💥 ERREUR CRITIQUE: {e}")
        # Optionnel : Notifier le crash
        if BOT_TOKEN and CHAT_ID:
            requests.post(
                f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
                json={"chat_id": CHAT_ID, "text": f"🚨 Crash Bot: {str(e)}"}
            )
        raise e

if __name__ == "__main__":
    main()