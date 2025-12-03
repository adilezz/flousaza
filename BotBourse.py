import os
import sqlite3
import datetime
import time
import pandas as pd
import requests
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager

# --- CONFIGURATION ---
BOT_TOKEN = os.environ.get("BOT_TOKEN")
CHAT_ID = os.environ.get("CHAT_ID")
URL_OFFICIELLE = "https://www.casablanca-bourse.com/fr/live-market/marche-actions-groupement"
DB_NAME = "bourse_casa.db"
MIN_VOLUME_MAD = 10000  # On ignore les actions avec moins de 10k MAD de volume jour

# --- MODULE 1: GESTION DE DONNÉES (DATABASE) ---
def init_db():
    """Crée la base de données locale pour l'historique."""
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS market_data (
            date TEXT,
            symbol TEXT,
            close REAL,
            volume_mad REAL,
            PRIMARY KEY (date, symbol)
        )
    ''')
    conn.commit()
    conn.close()

def save_daily_data(data_list):
    """Sauvegarde les données du jour en base."""
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    today = datetime.date.today().isoformat()
    
    count = 0
    for item in data_list:
        try:
            cursor.execute('''
                INSERT OR REPLACE INTO market_data (date, symbol, close, volume_mad)
                VALUES (?, ?, ?, ?)
            ''', (today, item['symbol'], item['close'], item['volume']))
            count += 1
        except Exception as e:
            print(f"❌ Erreur DB sur {item['symbol']}: {e}")
            
    conn.commit()
    conn.close()
    print(f"💾 {count} entrées sauvegardées/mises à jour en base de données.")

def get_history(symbol, limit=60):
    """Récupère l'historique pour l'analyse technique."""
    conn = sqlite3.connect(DB_NAME)
    df = pd.read_sql_query(f"SELECT date, close FROM market_data WHERE symbol = ? ORDER BY date ASC LIMIT {limit}", conn, params=(symbol,))
    conn.close()
    return df

# --- MODULE 2: SCRAPING (INGESTION) ---
def clean_number(txt):
    if not txt: return 0.0
    clean = txt.replace(' ', '').replace('%', '').replace(',', '.')
    if '--' in clean or clean in ['-', '']: return 0.0
    try:
        return float(clean)
    except:
        return 0.0

def scrape_market():
    print("🚀 Démarrage du scraping...")
    
    chrome_options = Options()
    chrome_options.add_argument("--headless") 
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36")
    
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=chrome_options)
    
    data_extracted = []
    
    try:
        driver.get(URL_OFFICIELLE)
        wait = WebDriverWait(driver, 30)
        wait.until(EC.presence_of_element_located((By.TAG_NAME, "tbody")))
        
        # Petit délai pour être sûr que tout le JS est chargé
        time.sleep(3) 
        
        rows = driver.find_elements(By.CSS_SELECTOR, "tbody tr")
        print(f"📊 {len(rows)} lignes trouvées.")
        
        for row in rows:
            cols = row.find_elements(By.TAG_NAME, "td")
            if len(cols) < 8: continue
            
            # Mapping basé sur l'observation standard de la Bourse de Casa
            # 0: Nom, 4: Dernier cours, 10 ou 11: Volume Montant (à vérifier selon affichage)
            # On prend souvent le dernier cours et le volume global
            
            nom = cols[0].text.strip()
            prix_txt = cols[4].text.strip()
            # Astuce: Parfois le volume est plus loin, on prend souvent la col 9 ou 10 pour le nbr de titres
            # Pour simplifier ici, on suppose que col 11 est le volume en Montant (Capital échangé)
            # Si indisponible, on met 0.
            vol_txt = "0"
            if len(cols) > 10:
                vol_txt = cols[-2].text.strip() # Souvent l'avant dernière est le volume montant
            
            prix = clean_number(prix_txt)
            volume_mad = clean_number(vol_txt)
            
            if prix > 0:
                data_extracted.append({
                    "symbol": nom,
                    "close": prix,
                    "volume": volume_mad
                })
                
    except Exception as e:
        print(f"❌ Erreur Scraping : {e}")
    finally:
        driver.quit()
        
    return data_extracted

# --- MODULE 3: ANALYSE QUANTITATIVE ---
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
    conn = sqlite3.connect(DB_NAME)
    # On récupère la date d'aujourd'hui pour ne traiter que les données fraîches
    today = datetime.date.today().isoformat()
    cursor = conn.cursor()
    
    # On récupère tous les tickers mis à jour aujourd'hui
    cursor.execute("SELECT symbol, close, volume_mad FROM market_data WHERE date = ?", (today,))
    todays_data = cursor.fetchall()
    conn.close()
    
    report_lines = []
    
    print(f"🧠 Analyse de {len(todays_data)} actifs...")

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

# --- MODULE 4: NOTIFICATION ---
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

# --- MAIN ---
if __name__ == "__main__":
    # 1. Initialiser la DB (si elle n'existe pas)
    init_db()
    
    # 2. Scraper les données fraiches
    data = scrape_market()
    
    # 3. Sauvegarder
    if data:
        save_daily_data(data)
        
        # 4. Analyser & Notifier
        # Note importante : Au premier lancement, l'analyse ne donnera rien 
        # car il faut environ 20 jours de données pour calculer une Moyenne Mobile 20.
        # Le script accumulera les données jour après jour.
        alerts = analyze_opportunities()
        
        if len(data) > 0 and len(alerts) == 0:
            # Message pour rassurer l'utilisateur au début (Cold Start)
            send_telegram([f"Données mises à jour pour {len(data)} actions.\nPas assez d'historique pour l'analyse technique (Mode Apprentissage activé)."])
        else:
            send_telegram(alerts)
    else:
        print("❌ Aucune donnée récupérée.")
