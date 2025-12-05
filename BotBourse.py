import os
import sqlite3
import datetime
from datetime import date, timedelta
import pandas as pd
import pandas_ta as ta  # Pour l'analyse technique (RSI, SMA)
import requests
import casabourse as cb
from concurrent.futures import ThreadPoolExecutor, as_completed

# --- 1. CONFIGURATION DYNAMIQUE ---

# On récupère les secrets de l'environnement, sinon valeurs par défaut pour test local
DB_NAME = "bourse_casa_pro.db"
BOT_TOKEN = os.environ.get("BOT_TOKEN")
CHAT_ID = os.environ.get("CHAT_ID")

# Stratégie Financière
BUDGET_MENSUEL = 4000.0     # Apport mensuel
MAX_ALLOCATION_PER_STOCK = 0.20 # Max 20% du portefeuille sur une seule action (Diversification)
MIN_RSI = 30                # Zone de survente (Bon pour acheter)
MAX_RSI = 70                # Zone de surachat (Dangereux)
MIN_YIELD = 3.5             # Rendement dividende minimum visé pour les valeurs de fond

# --- 2. GESTION DE LA BASE DE DONNÉES (PERSISTANCE) ---

def get_db_connection():
    return sqlite3.connect(DB_NAME)

def init_db():
    """Initialise une structure de base de données professionnelle."""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # 1. Instruments (Liste des actions)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS instruments (
            symbol TEXT PRIMARY KEY,
            name TEXT,
            sector TEXT,
            last_dividend REAL DEFAULT 0.0,
            last_dividend_year INTEGER DEFAULT 0
        )
    """)
    
    # 2. Historique des Cours (OHLCV)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS historical_quotes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT,
            date TEXT,
            close REAL,
            volume REAL,
            UNIQUE(symbol, date)
        )
    """)

    # 3. Portefeuille (Ce que vous possédez)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS portfolio (
            symbol TEXT PRIMARY KEY,
            quantity INTEGER DEFAULT 0,
            avg_price REAL DEFAULT 0.0, -- Prix de revient unitaire (PRU)
            total_invested REAL DEFAULT 0.0
        )
    """)

    # 4. Transactions (Journal de bord)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS transactions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT,
            type TEXT, -- ACHAT / VENTE / DIVIDENDE
            symbol TEXT,
            quantity INTEGER,
            price REAL,
            total_amount REAL
        )
    """)
    
    conn.commit()
    conn.close()

def get_latest_date():
    conn = get_db_connection()
    try:
        row = conn.execute("SELECT MAX(date) FROM historical_quotes").fetchone()
        return row[0] if row and row[0] else None
    except:
        return None
    finally:
        conn.close()

# --- 3. MOTEUR D'ACQUISITION (SCRAPING & DATA) ---

def clean_number(txt):
    if not txt: return 0.0
    clean = str(txt).replace(' ', '').replace('%', '').replace(',', '.').replace(u'\xa0', '')
    try: return float(clean)
    except: return 0.0

def sync_instruments_and_dividends():
    """
    Récupère la liste des actions et essaie d'estimer/scrapper les dividendes.
    Note: Le scraping précis des dividendes sur le web marocain est complexe car les URL changent.
    Ici, nous simulons une mise à jour via une 'Knowledge Base' initiale pour les actions connues,
    mais le code est prêt pour scrapper une source externe.
    """
    print("🌍 Synchronisation des instruments...")
    try:
        df = cb.get_available_instrument()
        # Filtrer pour ne garder que les actions (Ticker < 6 chars généralement)
        df_actions = df[df["Symbole"].astype(str).str.len() <= 3].copy()
    except Exception as e:
        print(f"⚠️ Erreur API Bourse: {e}")
        return []

    conn = get_db_connection()
    
    # Dictionnaire de secours pour les dividendes 2023/2024 (Pour éviter le hardcoding pur dans la logique)
    # Dans une version idéale, on ferait un requests.get('site_bourse/dividendes') et un BeautifulSoup
    known_dividends = {
        'IAM': 2.19, 'BCP': 9.5, 'ATW': 15.5, 'CIM': 55.0, 'LHM': 66.0, 
        'MSA': 8.5, 'COS': 8.5, 'TQM': 38.0, 'SID': 0.0, 'ADH': 0.0
    }

    instruments_list = []
    for _, row in df_actions.iterrows():
        sym = str(row["Symbole"]).strip()
        name = str(row["Nom"]).strip()
        
        # Mise à jour des dividendes connus (ou 0 par défaut)
        div = known_dividends.get(sym, 0.0)
        year = 2023 if sym in known_dividends else 0
        
        conn.execute("""
            INSERT INTO instruments (symbol, name, last_dividend, last_dividend_year) 
            VALUES (?, ?, ?, ?)
            ON CONFLICT(symbol) DO UPDATE SET 
            name=excluded.name, 
            last_dividend=CASE WHEN excluded.last_dividend > 0 THEN excluded.last_dividend ELSE last_dividend END
        """, (sym, name, div, year))
        
        instruments_list.append(sym)
        
    conn.commit()
    conn.close()
    return instruments_list

def fetch_history_worker(symbol, start_s, end_s):
    """Worker pour le multithreading."""
    try:
        df = cb.get_historical_data_auto(symbol, start_s, end_s)
        if df is None or df.empty or "Date" not in df.columns: return None
        df["Date"] = pd.to_datetime(df["Date"]).dt.strftime("%Y-%m-%d")
        return (symbol, df)
    except: return None

def update_market_data():
    """Met à jour les cours."""
    init_db()
    symbols = sync_instruments_and_dividends()
    
    last_date = get_latest_date()
    start_dt = datetime.datetime.strptime(last_date, "%Y-%m-%d").date() + timedelta(days=1) if last_date else date.today() - timedelta(days=730)
    end_dt = date.today()

    if start_dt > end_dt:
        print("✅ Données à jour.")
        return 0

    print(f"📥 Téléchargement: {start_dt} -> {end_dt}")
    start_s, end_s = start_dt.strftime("%Y-%m-%d"), end_dt.strftime("%Y-%m-%d")
    
    conn = get_db_connection()
    count = 0
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = {executor.submit(fetch_history_worker, s, start_s, end_s): s for s in symbols}
        for future in as_completed(futures):
            res = future.result()
            if res:
                sym, df = res
                data_tuples = []
                for _, row in df.iterrows():
                    c = clean_number(row.get("Dernier cours", 0))
                    v = clean_number(row.get("Volume", 0))
                    data_tuples.append((sym, row["Date"], c, v))
                
                if data_tuples:
                    conn.executemany("INSERT OR IGNORE INTO historical_quotes (symbol, date, close, volume) VALUES (?, ?, ?, ?)", data_tuples)
                    count += len(data_tuples)
    
    conn.commit()
    conn.close()
    print(f"✅ +{count} nouvelles cotations.")
    return count

# --- 4. LE CERVEAU (ANALYSE TECHNIQUE & FONDAMENTALE) ---

def get_data_for_analysis(conn, symbol, limit=300):
    query = "SELECT date, close, volume FROM historical_quotes WHERE symbol = ? ORDER BY date ASC"
    df = pd.read_sql_query(query, conn, params=(symbol,))
    if df.empty: return pd.DataFrame()
    
    # Calcul Indicateurs Techniques avec Pandas TA
    df['SMA200'] = ta.sma(df['close'], length=200)
    df['RSI'] = ta.rsi(df['close'], length=14)
    return df.tail(limit) # On garde la fin

def get_portfolio_exposure(conn):
    """Calcule la valeur totale et l'exposition par action."""
    df = pd.read_sql_query("SELECT symbol, quantity, avg_price FROM portfolio WHERE quantity > 0", conn)
    exposure = {}
    total_value = 0.0
    
    # Il faut récupérer le prix actuel pour valoriser le portefeuille
    current_prices = {}
    for sym in df['symbol'].tolist():
        row = conn.execute("SELECT close FROM historical_quotes WHERE symbol = ? ORDER BY date DESC LIMIT 1", (sym,)).fetchone()
        if row: current_prices[sym] = row[0]

    for _, row in df.iterrows():
        sym = row['symbol']
        price = current_prices.get(sym, row['avg_price'])
        val = row['quantity'] * price
        exposure[sym] = val
        total_value += val
        
    return total_value, exposure

def analyze_market():
    conn = get_db_connection()
    today_str = datetime.date.today().strftime("%Y-%m-%d")
    
    # 1. Récupérer infos portefeuille
    total_pf_value, pf_exposure = get_portfolio_exposure(conn)
    cash_available = BUDGET_MENSUEL # On considère l'apport mensuel comme cash dispo immédiat
    
    # 2. Récupérer les instruments et dividendes
    instruments = pd.read_sql_query("SELECT * FROM instruments", conn)
    
    opportunities = []
    risks = []
    
    print("🧠 Analyse du marché en cours...")
    
    for _, stock in instruments.iterrows():
        sym = stock['symbol']
        df = get_data_for_analysis(conn, sym)
        
        if len(df) < 200: continue # Pas assez d'historique
        
        last = df.iloc[-1]
        sma200 = last['SMA200']
        rsi = last['RSI']
        close = last['close']
        
        # --- CALCUL FONDAMENTAL ---
        # Calcul du Yield (Rendement)
        div_amount = stock['last_dividend']
        yield_pct = (div_amount / close * 100) if close > 0 else 0
        
        # --- FILTRES STRATÉGIQUES ---
        score = 0
        reasons = []
        
        # A. Tendance de Fond (Sécurité)
        if close > sma200:
            score += 2
        else:
            score -= 5 # On évite la tendance baissière
            
        # B. Rendement (Objectif 10%)
        if yield_pct >= MIN_YIELD:
            score += 3
            reasons.append(f"💰 Bon Rendement ({yield_pct:.2f}%)")
        
        # C. Timing (RSI) - Buy the Dip
        if rsi < 40:
            score += 2
            reasons.append(f"📉 En survente (RSI {rsi:.0f})")
        elif rsi > 70:
            score -= 3 # Trop cher actuellement
            
        # D. Gestion Portefeuille
        current_inv = pf_exposure.get(sym, 0)
        proj_total = total_pf_value + cash_available
        if proj_total > 0 and (current_inv / proj_total) > MAX_ALLOCATION_PER_STOCK:
            score = -10 # On bloque, trop d'exposition
            reasons.append("⛔ Exposition Max atteinte")

        # Sélection finale
        if score >= 4:
            opportunities.append({
                'symbol': sym,
                'name': stock['name'],
                'close': close,
                'score': score,
                'yield': yield_pct,
                'reasons': reasons
            })
            
        # Détection de Risques (Crash)
        prev_close = df.iloc[-2]['close']
        var_day = ((close - prev_close)/prev_close)*100
        if var_day < -4.0:
            risks.append(f"🔻 {sym} a chuté de {var_day:.2f}% aujourd'hui.")

    conn.close()
    
    # Tri par score
    opportunities.sort(key=lambda x: x['score'], reverse=True)
    return opportunities[:3], risks, total_pf_value

# --- 5. RAPPORT & INTELLIGENCE TEMPORELLE ---

def generate_report(opportunities, risks, pf_value, report_type):
    today_fr = datetime.date.today().strftime("%d/%m/%Y")
    
    emojis = {"DAILY": "📅", "WEEKLY": "📆", "MONTHLY": "📊"}
    title = f"{emojis.get(report_type, '📅')} **RAPPORT {report_type} - {today_fr}**"
    
    msg = [title, ""]
    
    # Section 1 : Opportunités du jour
    msg.append(f"💡 **Top Opportunités (Budget: {BUDGET_MENSUEL} MAD)**")
    
    if not opportunities:
        msg.append("😴 Marché incertain ou trop cher. Gardez votre cash.")
    else:
        remaining_budget = BUDGET_MENSUEL
        for op in opportunities:
            # Allocation intelligente : 60% au 1er, 40% au 2ème
            alloc_pct = 0.6 if op == opportunities[0] and len(opportunities)>1 else 0.4
            # Si un seul choix, 100%
            if len(opportunities) == 1: alloc_pct = 1.0
            
            invest_amount = remaining_budget * alloc_pct
            qty = int(invest_amount // op['close'])
            
            if qty > 0:
                cost = qty * op['close']
                msg.append(f"\n🚀 **{op['name']} ({op['symbol']})**")
                msg.append(f"   🎯 Acheter **{qty}** à {op['close']} MAD")
                msg.append(f"   ℹ️ {', '.join(op['reasons'])}")
                remaining_budget -= cost
    
    # Section 2 : Risques
    if risks:
        msg.append("\n⚠️ **Alertes Marché**")
        for r in risks: msg.append(r)
        
    # Section 3 : Spécifique Hebdo/Mensuel
    if report_type == "MONTHLY":
        msg.append("\n------------------")
        msg.append("📈 **Bilan Mensuel**")
        msg.append(f"💼 Valeur Portefeuille estimée : {pf_value:,.2f} MAD")
        msg.append("✅ N'oubliez pas d'injecter votre épargne mensuelle.")
        
    msg.append("\n🤖 *BotBourse Pro v3.0*")
    return "\n".join(msg)

def send_telegram(message):
    if not BOT_TOKEN or not CHAT_ID:
        print("MSG (Console):", message)
        return
    try:
        url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
        requests.post(url, json={"chat_id": CHAT_ID, "text": message, "parse_mode": "Markdown"})
    except Exception as e:
        print(f"❌ Erreur Telegram: {e}")

# --- 6. EXÉCUTION PRINCIPALE ---

def main():
    # 1. Mise à jour des données
    update_market_data()
    
    # 2. Détermination du type de rapport
    today = datetime.date.today()
    report_type = "DAILY"
    
    # Est-ce vendredi ? (0=Lundi, 4=Vendredi)
    if today.weekday() == 4:
        report_type = "WEEKLY"
    
    # Est-ce la fin du mois ? (Si demain on change de mois)
    tomorrow = today + timedelta(days=1)
    if tomorrow.month != today.month:
        report_type = "MONTHLY"
        
    # 3. Analyse
    opps, risks, pf_val = analyze_market()
    
    # 4. Envoi
    if opps or risks or report_type != "DAILY":
        report = generate_report(opps, risks, pf_val, report_type)
        send_telegram(report)
    else:
        print("Rien à signaler aujourd'hui.")

if __name__ == "__main__":
    main()