import sqlite3
import datetime
from datetime import date, timedelta
import pandas as pd
import casabourse as cb
from concurrent.futures import ThreadPoolExecutor, as_completed

# --- CONFIGURATION ---
DB_NAME = "bourse_casa.db"
# Liste noire : Actions qui causent souvent des erreurs de scraping ou sans intérêt
BLACKLIST = ['TMA', 'TQM', 'UMR', 'VCN', 'WAA', 'ZDJ']

def get_db_connection():
    return sqlite3.connect(DB_NAME)

def init_db():
    """Initialise la structure de la base si elle n'existe pas."""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Table Instruments (Liste des sociétés)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS instruments (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT NOT NULL UNIQUE,
            name TEXT,
            sector TEXT
        )
    """)
    
    # Table Historique (Prix & Volumes)
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

def sync_instruments():
    """Met à jour la liste des actions disponibles."""
    print("📋 Synchronisation des instruments...")
    try:
        df = cb.get_available_instrument()
        # Filtre : Symboles à 3 lettres (Actions standard)
        df_actions = df[df["Symbole"].astype(str).str.len() == 3].copy()
    except Exception as e:
        print(f"⚠️ Erreur sync instruments: {e}")
        return

    conn = get_db_connection()
    cur = conn.cursor()
    
    count = 0
    for _, row in df_actions.iterrows():
        sym = str(row["Symbole"]).strip()
        if sym in BLACKLIST: continue
        
        name = row["Nom"].strip() if "Nom" in row else None
        
        # Upsert (Insert or Ignore/Update)
        cur.execute("SELECT id FROM instruments WHERE symbol = ?", (sym,))
        exists = cur.fetchone()
        
        if not exists:
            cur.execute("INSERT INTO instruments (symbol, name) VALUES (?, ?)", (sym, name))
            count += 1
            
    conn.commit()
    conn.close()
    print(f"✅ {count} nouveaux instruments ajoutés.")
    return

def fetch_data_for_instrument(inst, start_date, end_date):
    """Récupère les données pour un instrument donné (exécuté en thread)."""
    sym = inst["symbol"]
    inst_id = inst["id"]
    
    try:
        df = cb.get_historical_data_auto(sym, start_date, end_date)
        if df is None or df.empty or "Date" not in df.columns:
            return None
        
        # Nettoyage
        df["Date"] = pd.to_datetime(df["Date"]).dt.strftime("%Y-%m-%d")
        
        # Nettoyage numérique
        for col in ["Dernier cours", "Volume"]:
            if col in df.columns:
                df[col] = df[col].astype(str).str.replace(',', '.').str.replace(' ', '').str.replace('-', '0')
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0.0)
                
        return (inst_id, df)
    except Exception:
        return None

def update_history():
    """Met à jour l'historique des cours."""
    conn = get_db_connection()
    
    # Trouver la dernière date en base
    try:
        last_date_row = conn.execute("SELECT MAX(Date) FROM historical_quotes").fetchone()
        last_date = last_date_row[0] if last_date_row else None
    except:
        last_date = None
        
    if last_date:
        start_dt = datetime.datetime.strptime(last_date, "%Y-%m-%d").date() + timedelta(days=1)
    else:
        # Première exécution : on remonte 5 ans en arrière (Investissement Long Terme)
        start_dt = date.today() - timedelta(days=365*5)
        print("🆕 Première initialisation : Récupération de 5 ans d'historique.")

    end_dt = date.today()
    
    if start_dt > end_dt:
        print("✅ Base de données déjà à jour.")
        conn.close()
        return 0

    print(f"🔄 Mise à jour de {start_dt} à {end_dt}...")
    start_s = start_dt.strftime("%Y-%m-%d")
    end_s = end_dt.strftime("%Y-%m-%d")

    # Récupérer les instruments
    instruments = conn.execute("SELECT id, symbol FROM instruments").fetchall()
    inst_list = [{"id": r[0], "symbol": r[1]} for r in instruments]
    conn.close() # On ferme pour éviter les locks dans les threads, on réouvrira pour écrire

    total_inserted = 0
    
    # Exécution parallèle (5 workers)
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = {executor.submit(fetch_data_for_instrument, i, start_s, end_s): i for i in inst_list}
        
        conn_write = get_db_connection() # Connexion unique pour l'écriture
        
        for future in as_completed(futures):
            res = future.result()
            if res:
                inst_id, df = res
                # Écriture en base
                for _, row in df.iterrows():
                    try:
                        conn_write.execute("""
                            INSERT OR IGNORE INTO historical_quotes (instrument_id, Date, "Dernier_cours", "Volume")
                            VALUES (?, ?, ?, ?)
                        """, (inst_id, row["Date"], row.get("Dernier cours", 0), row.get("Volume", 0)))
                        total_inserted += 1
                    except Exception as e:
                        print(f"Erreur SQL: {e}")
                        
        conn_write.commit()
        conn_write.close()

    print(f"📥 {total_inserted} nouvelles lignes insérées.")
    return total_inserted

def audit_database():
    """Vérifie la santé de la base de données (Audit)."""
    conn = get_db_connection()
    print("\n🏥 --- AUDIT DE LA BASE DE DONNÉES ---")
    
    # 1. Vérifier la dernière date globale
    last_date = conn.execute("SELECT MAX(Date) FROM historical_quotes").fetchone()[0]
    print(f"📅 Dernière donnée enregistrée : {last_date}")
    
    # 2. Vérifier les instruments "morts" (pas de maj depuis 7 jours alors que la base est récente)
    if last_date:
        query_dead = """
            SELECT i.symbol, MAX(h.Date) as last 
            FROM instruments i 
            LEFT JOIN historical_quotes h ON i.id = h.instrument_id 
            GROUP BY i.id 
            HAVING last < date(?, '-7 days') OR last IS NULL
        """
        dead_rows = conn.execute(query_dead, (last_date,)).fetchall()
        if dead_rows:
            print(f"⚠️ {len(dead_rows)} actions semblent inactives ou obsolètes (ex: {dead_rows[0][0]})")
        else:
            print("✅ Tous les instruments actifs sont à jour.")

    # 3. Compter le volume total de données
    count = conn.execute("SELECT COUNT(*) FROM historical_quotes").fetchone()[0]
    print(f"📊 Total points de données : {count}")
    conn.close()

if __name__ == "__main__":
    print("🚀 Démarrage du Scraper Bourse Casa...")
    init_db()
    sync_instruments()
    update_history()
    audit_database()
    print("👋 Fin du script.")
