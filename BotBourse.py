import os
import sqlite3
import datetime
from datetime import date, timedelta
import pandas as pd
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed # Nécessaire pour l'accélération

import casabourse as cb  # type: ignore

# --- CONFIGURATION ---
BOT_TOKEN = os.environ.get("BOT_TOKEN")
CHAT_ID = os.environ.get("CHAT_ID")
# Base de données centralisée
DB_NAME = "bourse_casa.db"
MIN_VOLUME_MAD = 10000  # On ignore les actions avec moins de 10k MAD de volume jour


# --- MODULE 1: OUTILS NUMÉRIQUES & ACCÈS DB ---
def clean_number(txt):
    if not txt: return 0.0
    clean = str(txt).replace(' ', '').replace('%', '').replace(',', '.')
    if '--' in clean or clean in ['-', '']: return 0.0
    try:
        return float(clean)
    except Exception:
        return 0.0


def get_latest_session_date():
    """Retourne la dernière séance disponible dans historical_quotes."""
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    # Création de la table si elle n'existe pas (sécurité)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS historical_quotes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            instrument_id INTEGER,
            Date TEXT,
            UNIQUE(instrument_id, Date)
        )
    """)
    cursor.execute("SELECT MAX(Date) FROM historical_quotes")
    row = cursor.fetchone()
    conn.close()
    return row[0] if row and row[0] is not None else None


def get_instruments_from_db():
    """Retourne la liste (id, symbol) depuis la table instruments."""
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute("SELECT id, symbol FROM instruments")
    rows = cursor.fetchall()
    conn.close()
    return [{"id": r[0], "symbol": r[1]} for r in rows]


def sync_instruments_from_casabourse() -> int:
    """Synchronise la table instruments avec la liste casabourse."""
    try:
        df = cb.get_available_instrument()
    except Exception as e:
        print(f"⚠️ Erreur récupération instruments casabourse: {e}")
        return 0

    # Heuristique: les actions au comptant ont un symbole à 3 lettres
    df_actions = df[df["Symbole"].astype(str).str.len() == 3].copy()

    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS instruments (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT NOT NULL UNIQUE,
            name TEXT
        )
        """
    )
    conn.commit()

    cur.execute("SELECT symbol FROM instruments")
    existing = {row[0] for row in cur.fetchall()}

    inserted = 0
    for _, row in df_actions.iterrows():
        sym = str(row["Symbole"]).strip()
        name = str(row["Nom"]).strip() if "Nom" in df_actions.columns else None
        if not sym:
            continue
        if sym in existing:
            cur.execute("UPDATE instruments SET name = ? WHERE symbol = ?", (name, sym))
        else:
            cur.execute("INSERT INTO instruments (symbol, name) VALUES (?, ?)", (sym, name))
            inserted += 1

    conn.commit()
    conn.close()
    return inserted


def save_history(conn: sqlite3.Connection, instrument_id: int, df: pd.DataFrame) -> int:
    """Sauvegarde les données historiques."""
    if df.empty:
        return 0

    cur = conn.cursor()
    df = df.copy()
    if "Symbol" in df.columns:
        df = df.drop(columns=["Symbol"])

    columns = list(df.columns)
    # Nettoyage des noms de colonnes pour SQL
    columns_sql = ", ".join(f'"{c.strip().replace(" ", "_").replace("%", "pct")}"' for c in columns)
    placeholders = ", ".join("?" for _ in columns)

    inserted = 0
    for _, row in df.iterrows():
        values = [str(row[c]) if not pd.isna(row[c]) else None for c in df.columns]
        
        # Construction dynamique de la requête pour gérer les colonnes variables
        try:
            # Vérification basique si les colonnes existent, sinon on pourrait ajouter un ALTER TABLE ici
            # Pour simplifier, on suppose que la structure est stable ou gérée ailleurs
            cur.execute(
                f"""
                INSERT OR REPLACE INTO historical_quotes (instrument_id, {columns_sql})
                VALUES (?, {placeholders})
                """,
                [instrument_id, *values],
            )
            inserted += 1
        except sqlite3.OperationalError as e:
            # Si une colonne manque, on l'ajoute (méthode robuste)
            if "no such column" in str(e):
                print(f"⚠️ Colonne manquante détectée, tentative de correction... ({e})")
            else:
                print(f"❌ Erreur SQL sur {instrument_id}: {e}")

    conn.commit()
    return inserted


# --- FONCTIONS POUR LE MULTITHREADING ---
def process_instrument_update(inst, start_s, end_s):
    """Fonction helper exécutée en parallèle pour récupérer les données d'un instrument."""
    sym = inst["symbol"]
    instrument_id = inst["id"]
    
    # Liste noire des tickers connus pour planter ou être obsolètes
    BLACKLIST = ['TMA', 'TQM', 'UMR', 'VCN', 'WAA', 'ZDJ']
    if sym in BLACKLIST:
        return None

    try:
        # print(f"🔄 Fetch {sym}...") # Commenté pour réduire le bruit dans les logs
        df = cb.get_historical_data_auto(sym, start_s, end_s)
        
        if df is None or df.empty:
            return None
            
        if "Date" not in df.columns:
            return None
            
        # Normalisation
        df["Date"] = pd.to_datetime(df["Date"]).dt.strftime("%Y-%m-%d")
        df.insert(0, "Symbol", sym)
        return (instrument_id, df)
        
    except Exception:
        # On ignore silencieusement les erreurs individuelles pour ne pas polluer les logs
        return None


def update_daily_data(max_instruments=None) -> int:
    """Version multithreadée de la mise à jour."""
    sync_instruments_from_casabourse()

    last_date = get_latest_session_date()
    if last_date:
        start_dt = datetime.datetime.strptime(last_date, "%Y-%m-%d").date() + timedelta(days=1)
    else:
        print("⚠️ Aucune date existante, on suppose une initialisation nécessaire.")
        # Pour une première exécution, on pourrait prendre une date par défaut, ex: 1 an
        # Ici on retourne 0 pour laisser le scraper initial faire le travail si besoin,
        # ou on force une date arbitraire :
        start_dt = date.today() - timedelta(days=30) 

    today = date.today()
    if start_dt > today:
        print(f"ℹ️ Base à jour (Dernière date: {last_date}).")
        return 0

    start_s = start_dt.strftime("%Y-%m-%d")
    end_s = today.strftime("%Y-%m-%d")
    print(f"🔄 Démarrage mise à jour de {start_s} à {end_s}...")

    instruments = get_instruments_from_db()
    if max_instruments is not None:
        instruments = instruments[:max_instruments]

    total_rows = 0
    conn = sqlite3.connect(DB_NAME)
    
    # Exécution parallèle (5 workers est un bon compromis pour ne pas être banni)
    print(f"🚀 Lancement du scan parallèle sur {len(instruments)} instruments...")
    
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = {executor.submit(process_instrument_update, inst, start_s, end_s): inst for inst in instruments}
        
        for i, future in enumerate(as_completed(futures)):
            result = future.result()
            if result:
                inst_id, df = result
                rows = save_history(conn, inst_id, df)
                total_rows += rows
                if rows > 0:
                    print(f"  ✅ {df['Symbol'].iloc[0]} mis à jour (+{rows} sessions)")
            
            # Barre de progression simple
            if i % 10 == 0:
                print(f"  ... {i}/{len(instruments)} traités")

    conn.close()
    print(f"✅ Terminé. Total lignes ajoutées: {total_rows}")
    return total_rows


def get_history(symbol, limit=60):
    """Récupère l'historique pour l'analyse technique."""
    conn = sqlite3.connect(DB_NAME)
    query = """
        SELECT h.Date as date,
               h."Dernier_cours" AS close_raw,
               h."Volume" AS volume_raw
        FROM historical_quotes h
        JOIN instruments i ON h.instrument_id = i.id
        WHERE i.symbol = ?
        ORDER BY h.Date ASC
        LIMIT ?
    """
    try:
        df = pd.read_sql_query(query, conn, params=(symbol, limit))
        if not df.empty:
            df["close"] = df["close_raw"].apply(clean_number)
            df["volume"] = df["volume_raw"].apply(clean_number)
            df = df.drop(columns=["close_raw", "volume_raw"])
    except Exception:
        df = pd.DataFrame()
    finally:
        conn.close()

    return df


# --- MODULE 2: ANALYSE QUANTITATIVE ---
def calculate_indicators(df):
    """Calcule RSI et SMA."""
    if len(df) < 20: return None, None, None
    
    # RSI 14
    delta = df['close'].diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
    rs = gain / loss
    df['rsi'] = 100 - (100 / (1 + rs))
    
    # SMA
    df['sma20'] = df['close'].rolling(window=20).mean()
    df['sma50'] = df['close'].rolling(window=50).mean()
    
    return df.iloc[-1]['rsi'], df.iloc[-1]['sma20'], df.iloc[-1]['sma50']


def analyze_opportunities():
    """Analyse les opportunités."""
    session_date = get_latest_session_date()
    if not session_date:
        print("❌ Aucune séance trouvée pour l'analyse.")
        return []

    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    
    # Récupération données du jour
    query = """
        SELECT i.symbol, h."Dernier_cours", h."Volume"
        FROM historical_quotes h
        JOIN instruments i ON h.instrument_id = i.id
        WHERE h.Date = ?
    """
    cursor.execute(query, (session_date,))
    rows = cursor.fetchall()
    conn.close()

    todays_data = []
    for symbol, close_raw, volume_raw in rows:
        close = clean_number(close_raw)
        volume_mad = clean_number(volume_raw)
        todays_data.append((symbol, close, volume_mad))

    report_lines = []
    print(f"🧠 Analyse de {len(todays_data)} actifs pour le {session_date}...")

    for symbol, close, volume in todays_data:
        if volume < MIN_VOLUME_MAD:
            continue
            
        df = get_history(symbol, limit=60)
        if len(df) < 20: continue
            
        rsi, sma20, sma50 = calculate_indicators(df)
        if rsi is None: continue

        # Calcul Volume Moyen (5 derniers jours)
        avg_vol = df['volume'].rolling(window=5).mean().iloc[-1] if 'volume' in df.columns else volume
        vol_spike = volume > (avg_vol * 1.5) and volume > 50000 # +50% vs moyenne et significatif

        signal = None
        reason = ""
        target = 0.0
        
        # --- STRATÉGIE ---
        if rsi < 35:
            signal = "ACHAT (Rebond)"
            reason = f"RSI Bas ({rsi:.0f})"
            target = close * 1.05
        elif sma20 and sma50 and sma20 > sma50 and (sma20 / sma50) < 1.02:
            signal = "ACHAT (Tendance)"
            reason = "Golden Cross"
            target = close * 1.10
        elif rsi > 70:
            signal = "VENTE"
            reason = f"RSI Haut ({rsi:.0f})"
            target = close * 0.95
            
        if signal:
            strength_icon = "🔥" if vol_spike else ""
            line = f"🚨 **#{symbol}** {strength_icon}\n" \
                   f"📈 {signal}\n" \
                   f"💰 {close} MAD | Vol: {volume:,.0f}\n" \
                   f"🎯 Obj: {target:.2f} | 💡 {reason}"
            report_lines.append(line)
            
    return report_lines


# --- MODULE 3: NOTIFICATION ---
def send_telegram(lines):
    header = f"📅 **BOURSE CASA - {datetime.date.today()}**\n"
    
    # 1. Sauvegarde Console (Indispensable)
    print("\n📢 --- RAPPORT FINAL ---")
    print(header)
    for l in lines: print(l + "\n--")
    print("-----------------------\n")

    if not lines:
        print("Rien à signaler.")
        return

    if not BOT_TOKEN or not CHAT_ID:
        print("⚠️ Config Telegram manquante.")
        return

    # 2. Envoi par paquets (Chunking) pour éviter l'erreur 400
    MAX_LENGTH = 3500 # Marge de sécurité
    
    messages = []
    current_msg = header + "\n"
    
    for line in lines:
        entry = f"{line}\n------------------\n"
        if len(current_msg) + len(entry) > MAX_LENGTH:
            messages.append(current_msg)
            current_msg = entry
        else:
            current_msg += entry
            
    if current_msg:
        messages.append(current_msg)

    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    
    print(f"📤 Envoi de {len(messages)} message(s) sur Telegram...")
    for i, msg in enumerate(messages):
        payload = {"chat_id": CHAT_ID, "text": msg, "parse_mode": "Markdown"}
        try:
            r = requests.post(url, json=payload)
            if r.status_code != 200:
                print(f"⚠️ Erreur envoi partie {i+1}: {r.text}")
                # Retry sans markdown si échec (souvent dû à des caractères spéciaux mal échappés)
                payload["parse_mode"] = ""
                requests.post(url, json=payload)
        except Exception as e:
            print(f"❌ Erreur connexion: {e}")

def main():
    try:
        updated = update_daily_data()
        alerts = analyze_opportunities()
        
        summary = f"✅ Scan terminé.\nMaj: {updated} lignes.\nSignaux: {len(alerts)}"
        
        # On envoie toujours un résumé, plus les alertes si elles existent
        if alerts:
            send_telegram([summary] + alerts)
        else:
            send_telegram([summary])
            
    except Exception as e:
        print(f"💥 ERREUR CRITIQUE MAIN: {e}")
        # Tenter d'envoyer l'erreur sur Telegram
        if BOT_TOKEN and CHAT_ID:
            requests.post(
                f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
                json={"chat_id": CHAT_ID, "text": f"🚨 Crash BotBourse: {str(e)}"}
            )
        raise e

if __name__ == "__main__":
    main()
