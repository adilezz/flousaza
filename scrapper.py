import os
import datetime
from datetime import date, timedelta
import pandas as pd
import casabourse as cb
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

# Charger les variables d'environnement (URL de votre base Supabase/Neon)
DB_URL = "postgresql://postgres.naoagvvceeztbpcdpect:adilFG12345FIGUIG@aws-1-eu-west-1.pooler.supabase.com:5432/postgres"

# --- 1. LE SCHÉMA SQL VALIDÉ ---
SCHEMA_SQL = """
-- Nettoyage préventif (Optionnel, attention ça efface tout !)
-- DROP TABLE IF EXISTS transactions, corporate_actions, company_financials, market_data, strategy_config, instruments CASCADE;

-- 1. Référentiel
CREATE TABLE IF NOT EXISTS instruments (
    symbol VARCHAR(10) PRIMARY KEY,
    name VARCHAR(100),
    sector VARCHAR(50),
    is_active BOOLEAN DEFAULT TRUE,
    quality_score INTEGER DEFAULT 50, -- Note par défaut neutre
    created_at TIMESTAMP DEFAULT NOW()
);

-- 2. Configuration
CREATE TABLE IF NOT EXISTS strategy_config (
    key VARCHAR(50) PRIMARY KEY,
    value VARCHAR(50),
    description TEXT
);

-- 3. Données de Marché
CREATE TABLE IF NOT EXISTS market_data (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(10) REFERENCES instruments(symbol),
    date DATE NOT NULL,
    open NUMERIC(10, 2),
    high NUMERIC(10, 2),
    low NUMERIC(10, 2),
    close NUMERIC(10, 2),
    volume BIGINT,
    UNIQUE (symbol, date)
);

-- 4. Intelligence Financière (Fondamentaux)
CREATE TABLE IF NOT EXISTS company_financials (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(10) REFERENCES instruments(symbol),
    year INT NOT NULL,
    period VARCHAR(10) DEFAULT 'ANNUAL',
    net_income NUMERIC(20, 2),
    equity NUMERIC(20, 2),
    revenue NUMERIC(20, 2),
    net_debt NUMERIC(20, 2),
    per NUMERIC(10, 2),
    pbr NUMERIC(10, 2),
    publication_date DATE DEFAULT CURRENT_DATE,
    UNIQUE (symbol, year, period)
);

-- 5. Dividendes (Corporate Actions)
CREATE TABLE IF NOT EXISTS corporate_actions (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(10) REFERENCES instruments(symbol),
    fiscal_year INT,
    amount NUMERIC(10, 2),
    ex_date DATE,
    payment_date DATE,
    type VARCHAR(20) DEFAULT 'Ordinary',
    status VARCHAR(20) DEFAULT 'Proposed'
);

-- 6. Portefeuille
CREATE TABLE IF NOT EXISTS transactions (
    id SERIAL PRIMARY KEY,
    date TIMESTAMP DEFAULT NOW(),
    symbol VARCHAR(10) REFERENCES instruments(symbol),
    type VARCHAR(20) NOT NULL, -- BUY, SELL, DIVIDEND, DEPOSIT
    quantity INT DEFAULT 0,
    price NUMERIC(10, 2) DEFAULT 0,
    fees NUMERIC(10, 2) DEFAULT 0,
    total_amount NUMERIC(12, 2),
    notes TEXT
);

-- Vue Portefeuille Live
CREATE OR REPLACE VIEW portfolio_live AS
SELECT 
    symbol,
    SUM(CASE WHEN type = 'BUY' THEN quantity WHEN type = 'SELL' THEN -quantity ELSE 0 END) as qty,
    SUM(CASE WHEN type = 'BUY' THEN total_amount ELSE 0 END) / NULLIF(SUM(CASE WHEN type = 'BUY' THEN quantity ELSE 0 END), 0) as avg_price
FROM transactions
GROUP BY symbol
HAVING SUM(CASE WHEN type = 'BUY' THEN quantity WHEN type = 'SELL' THEN -quantity ELSE 0 END) > 0;
"""

def get_db_connection():
    if not DB_URL:
        raise ValueError("❌ DATABASE_URL manquante dans le fichier .env")
    return psycopg2.connect(DB_URL)

def init_schema():
    print("🏗️ Création de la structure de la base de données...")
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute(SCHEMA_SQL)
    conn.commit()
    cur.close()
    conn.close()
    print("✅ Tables créées avec succès.")

def seed_instruments():
    print("🌍 Récupération de la liste des actions (Casabourse)...")
    try:
        # On utilise votre bibliothèque existante
        df = cb.get_available_instrument()
        # Filtrage simple (Symboles courts = Actions généralement)
        df_actions = df[df["Symbole"].astype(str).str.len() <= 6].copy()
    except Exception as e:
        print(f"⚠️ Erreur récupération casabourse: {e}")
        return

    conn = get_db_connection()
    cur = conn.cursor()
    
    instruments_to_insert = []
    for _, row in df_actions.iterrows():
        sym = str(row["Symbole"]).strip()
        name = str(row["Nom"]).strip()
        sector = "Unknown" # Casabourse ne donne pas toujours le secteur directement ici
        instruments_to_insert.append((sym, name, sector))

    # Upsert (Insérer ou ne rien faire si existe)
    query = """
        INSERT INTO instruments (symbol, name, sector) 
        VALUES %s 
        ON CONFLICT (symbol) DO UPDATE SET name = EXCLUDED.name
    """
    execute_values(cur, query, instruments_to_insert)
    conn.commit()
    print(f"✅ {len(instruments_to_insert)} instruments insérés/mis à jour.")
    
    # Injection des dividendes "connus" de votre ancien code pour démarrer
    # Cela permet d'avoir une base minimale pour le yield
    known_dividends = [
        ('IAM', 2023, 2.19),  # Itissalat Al Maghrib
        ('BCP', 2023, 9.5),   # Banque Populaire
        ('ATW', 2023, 15.5),  # Attijariwafa Bank
        ('CMA', 2023, 55.0),  # Ciments du Maroc (CIM -> CMA)
        ('LHM', 2023, 66.0),  # LafargeHolcim
        ('MSA', 2023, 8.5),   # Marsa Maroc
        ('CSR', 2023, 8.5),   # Cosumar (COS -> CSR)
        ('TQM', 2023, 38.0)   # Taqa Morocco
    ]
    cur.executemany("""
        INSERT INTO corporate_actions (symbol, fiscal_year, amount, status)
        VALUES (%s, %s, %s, 'Paid')
        ON CONFLICT DO NOTHING
    """, known_dividends)
    conn.commit()
    cur.close()
    conn.close()

def seed_history():
    print("📊 Récupération de l'historique (365 jours)...")
    conn = get_db_connection()
    cur = conn.cursor()
    
    # Récupérer les symboles actifs
    cur.execute("SELECT symbol FROM instruments WHERE is_active = TRUE")
    symbols = [row[0] for row in cur.fetchall()]
    
    end_date = date.today()
    start_date = end_date - timedelta(days=1825) # 5 ans d'historique pour SMA200
    
    start_s = start_date.strftime("%Y-%m-%d")
    end_s = end_date.strftime("%Y-%m-%d")
    
    total_inserted = 0
    
    for sym in symbols:
        try:
            df = cb.get_historical_data_auto(sym, start_s, end_s)
            if df is None or df.empty: continue
            
            records = []
            for _, row in df.iterrows():
                # Nettoyage des données (virgules, espaces)
                close_val = float(str(row["Dernier cours"]).replace(',', '.').replace(u'\xa0', ''))
                vol_val = float(str(row["Volume"]).replace(',', '.').replace(u'\xa0', '')) if "Volume" in row else 0
                
                # Format date YYYY-MM-DD
                d_obj = pd.to_datetime(row["Date"])
                d_str = d_obj.strftime("%Y-%m-%d")
                
                records.append((sym, d_str, close_val, vol_val))
            
            if records:
                query = """
                    INSERT INTO market_data (symbol, date, close, volume)
                    VALUES %s
                    ON CONFLICT (symbol, date) DO NOTHING
                """
                execute_values(cur, query, records)
                total_inserted += len(records)
                print(f"   -> {sym}: {len(records)} jours importés.")
                
        except Exception as e:
            print(f"   ⚠️ Erreur sur {sym}: {e}")

    conn.commit()
    cur.close()
    conn.close()
    print(f"✅ Total: {total_inserted} cotations importées.")

def seed_config():
    """Initialise vos règles de gestion."""
    conn = get_db_connection()
    cur = conn.cursor()
    
    configs = [
        ('budget_monthly', '4000', 'Budget mensuel en MAD'),
        ('target_yield', '8.0', 'Objectif de rendement annuel %'),
        ('max_sector_allocation', '0.30', 'Max 30% du portefeuille par secteur'),
        ('max_stock_allocation', '0.15', 'Max 15% par action (Diversification)')
    ]
    
    query = """
        INSERT INTO strategy_config (key, value, description)
        VALUES %s
        ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value
    """
    execute_values(cur, query, configs)
    conn.commit()
    cur.close()
    conn.close()
    print("✅ Configuration initialisée.")

if __name__ == "__main__":
    print("🚀 Démarrage de l'initialisation BotBourse V2...")
    init_schema()
    seed_config()
    seed_instruments()
    seed_history()
    print("✨ Initialisation terminée ! Votre base est prête.")
