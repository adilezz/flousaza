import os
import sqlite3
import datetime
from datetime import date, timedelta
import pandas as pd
import requests

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
    clean = txt.replace(' ', '').replace('%', '').replace(',', '.')
    if '--' in clean or clean in ['-', '']: return 0.0
    try:
        return float(clean)
    except Exception:
        return 0.0


def get_latest_session_date():
    """Retourne la dernière séance disponible dans historical_quotes."""
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
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
    """
    Synchronise la table instruments avec la liste casabourse.

    Stratégie:
      - Récupère tous les instruments via casabourse.get_available_instrument().
      - Filtre sur les symboles à 3 lettres (actions cash, ~79 sociétés).
      - Upsert dans la table instruments (symbol, name).
    Retourne le nombre de nouveaux instruments insérés.
    """
    df = cb.get_available_instrument()
    # Heuristique: les actions au comptant ont un symbole à 3 lettres
    df_actions = df[df["Symbole"].astype(str).str.len() == 3].copy()

    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    # S'assure que la table existe (au cas où)
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
            # Met à jour le nom si besoin
            cur.execute(
                "UPDATE instruments SET name = ? WHERE symbol = ?",
                (name, sym),
            )
        else:
            cur.execute(
                """
                INSERT INTO instruments (symbol, name)
                VALUES (?, ?)
                """,
                (sym, name),
            )
            inserted += 1

    conn.commit()
    conn.close()

    print(
        f"🔄 Synchronisation des instruments terminée. "
        f"{inserted} nouveaux instruments insérés, total attendu ~{len(df_actions)} actions."
    )
    return inserted

def save_history(
    conn: sqlite3.Connection,
    instrument_id: int,
    df: pd.DataFrame,
) -> int:
    """
    Sauvegarde les données historiques pour un instrument dans historical_quotes.

    Cette fonction reprend la logique de save_history du scraper initial :
    - aligne les colonnes du DataFrame avec la table,
    - fait un INSERT OR REPLACE basé sur (instrument_id, Date) via la contrainte UNIQUE.
    """
    if df.empty:
        return 0

    cur = conn.cursor()

    df = df.copy()
    if "Symbol" in df.columns:
        df = df.drop(columns=["Symbol"])

    columns = list(df.columns)
    placeholders = ", ".join("?" for _ in columns)
    columns_sql = ", ".join(
        f'"{c.strip().replace(" ", "_").replace("%", "pct")}"' for c in columns
    )

    inserted = 0
    for _, row in df.iterrows():
        values = [str(row[c]) if not pd.isna(row[c]) else None for c in df.columns]
        cur.execute(
            f"""
            INSERT OR REPLACE INTO historical_quotes (instrument_id, {columns_sql})
            VALUES (?, {placeholders})
            """,
            [instrument_id, *values],
        )
        inserted += 1

    conn.commit()
    return inserted


def update_daily_data(max_instruments: int | None = None) -> int:
    """
    Met à jour historical_quotes avec les nouvelles séances via casabourse.

    - Cherche la dernière Date présente dans historical_quotes.
    - Pour chaque instrument en base, appelle casabourse.get_historical_data_auto
      sur l'intervalle [dernière_date+1, aujourd'hui].
    - Insère / remplace les lignes dans historical_quotes via save_history().

    Retourne le nombre total de lignes ajoutées/mises à jour.
    """
    # Avant toute chose, on s'assure que la table instruments est synchro
    sync_instruments_from_casabourse()

    last_date = get_latest_session_date()
    if last_date:
        start_dt = datetime.datetime.strptime(last_date, "%Y-%m-%d").date() + timedelta(days=1)
    else:
        # Si aucune date, on ne fait rien ici (la base doit être initialisée par scraper.py)
        print("⚠️ Aucune date existante dans historical_quotes, aucune mise à jour quotidienne effectuée.")
        return 0

    today = date.today()
    if start_dt > today:
        print(f"ℹ️ Aucune nouvelle séance à récupérer (dernière date = {last_date}).")
        return 0

    start_s = start_dt.strftime("%Y-%m-%d")
    end_s = today.strftime("%Y-%m-%d")
    print(f"🔄 Mise à jour quotidienne des données de {start_s} à {end_s}...")

    instruments = get_instruments_from_db()
    if max_instruments is not None:
        instruments = instruments[:max_instruments]

    conn = sqlite3.connect(DB_NAME)
    total_rows = 0
    try:
        for idx, inst in enumerate(instruments, start=1):
            sym = inst["symbol"]
            instrument_id = inst["id"]
            print(f"  ▶️ [{idx}/{len(instruments)}] Mise à jour de {sym}...")
            try:
                df = cb.get_historical_data_auto(sym, start_s, end_s)
                if df is None or df.empty:
                    continue
                if "Date" not in df.columns:
                    # casabourse devrait renvoyer une colonne Date; si ce n'est pas le cas on ignore
                    continue
                # Normalisation du format de date
                df["Date"] = pd.to_datetime(df["Date"]).dt.strftime("%Y-%m-%d")
                # On ajoute une colonne Symbol pour être cohérent avec save_history()
                df.insert(0, "Symbol", sym)
                rows = save_history(conn, instrument_id, df)
                total_rows += rows
            except Exception as exc:  # noqa: BLE001
                print(f"❌ Erreur lors de la mise à jour de {sym}: {exc}")
    finally:
        conn.close()

    print(f"✅ Mise à jour quotidienne terminée, {total_rows} lignes insérées/mises à jour.")
    return total_rows


def get_history(symbol, limit=60):
    """Récupère l'historique pour l'analyse technique depuis historical_quotes."""
    conn = sqlite3.connect(DB_NAME)
    query = """
        SELECT h.Date as date,
               h."Dernier_cours" AS close_raw
        FROM historical_quotes h
        JOIN instruments i ON h.instrument_id = i.id
        WHERE i.symbol = ?
        ORDER BY h.Date ASC
        LIMIT ?
    """
    df = pd.read_sql_query(query, conn, params=(symbol, limit))
    conn.close()

    if not df.empty:
        df["close"] = df["close_raw"].apply(clean_number)
        df = df.drop(columns=["close_raw"])
    return df

# --- MODULE 2: ANALYSE QUANTITATIVE ---
def calculate_indicators(df):
    """Calcule RSI et SMA sur un DataFrame pandas."""
    if len(df) < 15: return None, None, None # Pas assez de data pour RSI 14
    
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
    """Analyse les opportunités à partir de la dernière séance dans bourse_casa.db."""
    session_date = get_latest_session_date()
    if not session_date:
        print("❌ Aucune séance trouvée dans la base casablanca_bourse.db")
        return []

    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()

    # On récupère tous les tickers pour la dernière séance
    query = """
        SELECT i.symbol,
               h."Dernier_cours" AS close_raw,
               h."Volume" AS volume_raw
        FROM historical_quotes h
        JOIN instruments i ON h.instrument_id = i.id
        WHERE h.Date = ?
    """
    cursor.execute(query, (session_date,))
    rows = cursor.fetchall()
    conn.close()

    todays_data = []
    for symbol, close_raw, volume_raw in rows:
        close = clean_number(close_raw or "0")
        volume_mad = clean_number(volume_raw or "0")
        todays_data.append((symbol, close, volume_mad))

    report_lines = []

    print(f"🧠 Analyse de {len(todays_data)} actifs pour la séance {session_date}...")

    for symbol, close, volume in todays_data:
        # 1. Filtre de Liquidité
        if volume < MIN_VOLUME_MAD:
            continue # On ignore les "actions fantômes"
            
        # 2. Récupérer l'historique pour Analyse Technique
        df = get_history(symbol, limit=60)
        
        # Si pas assez d'historique (ex: premier lancement du script), on skip l'analyse technique
        if len(df) < 20:
            continue 
            
        rsi, sma20, sma50 = calculate_indicators(df)
        
        if rsi is None: continue

        signal = None
        reason = ""
        target = 0.0
        
        # --- STRATÉGIE SWING TRADING ---
        
        # Achat: RSI survendu (<35)
        if rsi < 35:
            signal = "ACHAT (Rebond)"
            reason = f"RSI Survendu ({rsi:.1f})"
            target = close * 1.05 # +5%
            
        # Achat: Golden Cross (SMA20 passe au dessus de SMA50)
        # Note: Pour un vrai Golden Cross, il faut comparer avec J-1, ici on fait simple
        elif sma20 and sma50 and sma20 > sma50 and (sma20 / sma50) < 1.02: 
            # < 1.02 signifie que le croisement est récent
            signal = "ACHAT (Tendance)"
            reason = "Golden Cross (SMA20 > SMA50)"
            target = close * 1.10
            
        # Vente: RSI Surchauffé (>70)
        elif rsi > 70:
            signal = "VENTE"
            reason = f"RSI Surchauffé ({rsi:.1f})"
            target = close * 0.95
            
        if signal:
            line = f"🚨 **#{symbol}**\n" \
                   f"📈 ACTION : {signal}\n" \
                   f"💰 PRIX : {close} MAD\n" \
                   f"🎯 OBJECTIF : {target:.2f} MAD\n" \
                   f"💡 RAISON : {reason}\n" \
                   f"📊 VOL : {volume:,.0f} MAD"
            report_lines.append(line)
            
    return report_lines

# --- MODULE 3: NOTIFICATION ---
def send_telegram(lines):
    if not lines:
        print("Rien à signaler aujourd'hui.")
        return
        
    header = f"📅 **ANALYSE BOURSE CASA - {datetime.date.today()}**\n\n"
    # Telegram a une limite de 4096 caractères, on découpe si besoin
    full_msg = header + "\n------------------\n".join(lines)
    
    if not BOT_TOKEN or not CHAT_ID:
        print("⚠️ Pas de config Telegram, affichage console uniquement:")
        print(full_msg)
        return

    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload = {"chat_id": CHAT_ID, "text": full_msg, "parse_mode": "Markdown"}
    try:
        r = requests.post(url, json=payload)
        if r.status_code == 200:
            print("✅ Rapport envoyé sur Telegram.")
        else:
            print(f"⚠️ Erreur Telegram: {r.text}")
    except Exception as e:
        print(f"Erreur connexion Telegram: {e}")

def main():
    """
    Point d'entrée :
      1) met à jour les données quotidiennes dans casablanca_bourse.db,
      2) analyse les opportunités sur la dernière séance disponible,
      3) envoie un rapport (Telegram ou console).
    """
    updated_rows = update_daily_data()
    alerts = analyze_opportunities()

    # On ajoute une ligne d'en-tête de santé dans le rapport
    health_line = (
        f"✅ Mise à jour quotidienne effectuée.\n"
        f"Lignes mises à jour/ajoutées: {updated_rows}\n"
        f"Signaux trouvés: {len(alerts)}"
    )
    if alerts:
        send_telegram([health_line] + alerts)
    else:
        send_telegram([health_line])


# --- MAIN ---
if __name__ == "__main__":
    main()
