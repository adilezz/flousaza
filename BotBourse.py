import os
import sqlite3
import datetime
import time
import logging
import threading
import queue
import requests
import pandas as pd
import numpy as np
import yfinance as yf
from concurrent.futures import ThreadPoolExecutor
--- CONFIGURATION UTILISATEUR & MARCHÉ ---
CONFIG = {
"BOT_TOKEN": os.environ.get("BOT_TOKEN"),
"CHAT_ID": os.environ.get("CHAT_ID"),
"DB_NAME": "bourse_casa_pro.db",
"CAPITAL_INITIAL": 20000,
"EPARGNE_MENSUELLE": 3000,  # Moyenne entre 2k et 4k
"MIN_VOLUME_MAD": 25000,    # Liquidité minimale pour éviter le piégeage
"FRAIS_COURTAGE_PCT": 0.0066, # 0.66% TTC (Moyenne marché)
"FRAIS_MIN_MAD": 15.0,      # Minimum de perception moyen (impacte les petits ordres)
"OBJECTIF_RENDEMENT": 0.10, # 10%
"MAX_WORKERS": 5            # Pour le téléchargement parallèle
}
Liste des tickers Yahoo Finance pour la BVC (Mise à jour 2024/2025)
TICKERS_BVC = {
"ATW.MA": "Attijariwafa Bank", "IAM.MA": "Maroc Telecom", "BCP.MA": "BCP",
"LHM.MA": "LafargeHolcim Maroc", "CSR.MA": "Cosumar", "MSA.MA": "Marsa Maroc",
"CMA.MA": "Ciments du Maroc", "ADH.MA": "Addoha", "TQM.MA": "Taqa Morocco",
"WAA.MA": "Wafa Assurance", "BOA.MA": "Bank of Africa", "ADI.MA": "Alliances",
"HPS.MA": "HPS", "LBV.MA": "Label Vie", "ATL.MA": "AtlantaSanad",
"SNA.MA": "Stokvis Nord Afrique", "JET.MA": "Jet Contractors", "MUT.MA": "Mutandis",
"DHO.MA": "Delta Holding", "SAH.MA": "Saham Assurance", "RDS.MA": "Residences Dar Saada",
"ALM.MA": "Aluminium du Maroc", "SNP.MA": "Snep", "MNG.MA": "Managem",
"SBM.MA": "Boissons du Maroc", "CDA.MA": "Auto Hall", "ITP.MA": "Itissalat Al-Maghrib"
}
Configuration des logs
logging.basicConfig(
format='%(asctime)s - %(levelname)s - %(message)s',
level=logging.INFO,
handlers=
)
--- MODULE 1: GESTIONNAIRE DE BASE DE DONNÉES (THREAD-SAFE) ---
class DatabaseHandler:
def init(self, db_name):
self.db_name = db_name
self.log_queue = queue.Queue()
self.running = True
self.worker_thread = threading.Thread(target=self._worker, daemon=True)
self.worker_thread.start()
self._init_db()
def _init_db(self):
"""Initialisation synchrone des tables au démarrage."""
conn = sqlite3.connect(self.db_name)
cursor = conn.cursor()
# Table Historique des Cours
cursor.execute("""
CREATE TABLE IF NOT EXISTS quotes (
symbol TEXT,
date TEXT,
open REAL, high REAL, low REAL, close REAL, volume REAL,
PRIMARY KEY (symbol, date)
)
""")
# Table Portefeuille Virtuel (Suivi)
cursor.execute("""
CREATE TABLE IF NOT EXISTS portfolio (
symbol TEXT PRIMARY KEY,
shares INTEGER,
avg_price REAL,
date_bought TEXT
)
""")
conn.commit()
conn.close()
def _worker(self):
"""Worker dédié qui gère toutes les écritures séquentiellement."""
conn = sqlite3.connect(self.db_name)
cursor = conn.cursor()
while self.running:
try:
task = self.log_queue.get(timeout=1)
sql, params = task
try:
cursor.execute(sql, params)
conn.commit()
except sqlite3.Error as e:
logging.error(f"Erreur SQL Worker: {e}")
self.log_queue.task_done()
except queue.Empty:
continue
except Exception as e:
logging.error(f"Erreur Critique Worker DB: {e}")
conn.close()
def execute_write(self, sql, params):
"""Envoie une requête d'écriture dans la file d'attente."""
self.log_queue.put((sql, params))
def fetch_df(self, query, params=None):
"""Lecture synchrone (SQLite supporte multi-lecteurs)."""
conn = sqlite3.connect(self.db_name)
try:
return pd.read_sql_query(query, conn, params=params)
finally:
conn.close()
def stop(self):
self.running = False
self.worker_thread.join()
--- MODULE 2: ANALYSE TECHNIQUE & INDICATEURS ---
class TechnicalAnalyst:
@staticmethod
def calculate_indicators(df):
"""Calcule RSI, SMA, et Volatilité."""
if len(df) < 50: return df # Pas assez de données
# Copie pour éviter SettingWithCopyWarning
df = df.copy()
# 1. RSI (14)
delta = df['close'].diff()
gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
rs = gain / loss
df['rsi'] = 100 - (100 / (1 + rs))
# 2. Moyennes Mobiles
df['sma20'] = df['close'].rolling(window=20).mean()
df['sma50'] = df['close'].rolling(window=50).mean()
df['sma200'] = df['close'].rolling(window=200).mean()
# 3. Moyenne Volume (Liquidité)
df['avg_vol_20'] = df['volume'].rolling(window=20).mean()
return df
@staticmethod
def get_signal(row, prev_row):
"""Génère un signal interprétable."""
signal = "NEUTRE"
score = 0
details =
# Logique de Tendance (Trend Following)
if row['close'] > row['sma200']:
score += 2
details.append("Tendance Long Terme Haussière (Prix > MM200)")
# Logique de Momentum (RSI)
if row['rsi'] < 30:
score += 3
signal = "ACHAT FORT (Survendu)"
details.append(f"RSI Bas ({row['rsi']:.1f}) - Potentiel rebond")
elif row['rsi'] > 70:
score -= 2
details.append(f"RSI Haut ({row['rsi']:.1f}) - Risque surchauffe")
elif 50 < row['rsi'] < 65 and row['close'] > row['sma50']:
score += 1
details.append("Momentum positif sain")
# Croisement Golden Cross (SMA50 croise SMA200 à la hausse)
if prev_row['sma50'] < prev_row['sma200'] and row['sma50'] > row['sma200']:
score += 5
signal = "ACHAT MAJEUR (Golden Cross)"
details.append("Croisement MM50/MM200 validé")
# Détection "Yield Hunter" (Simplifiée ici par le prix bas vs historique)
# Note: Dans un système réel, on injecterait le dividende ici.
return signal, score, details
--- MODULE 3: INTELLIGENCE DE MARCHÉ (DATA FETCHING) ---
class MarketIntelligence:
def init(self, db_handler):
self.db = db_handler
def update_market_data(self):
"""Récupère les données Yahoo Finance et met à jour la DB."""
logging.info("Démarrage de la mise à jour marché...")
today = datetime.date.today().strftime("%Y-%m-%d")
def fetch_ticker(symbol, name):
try:
# On récupère beaucoup d'historique pour les MM200
ticker = yf.Ticker(symbol)
hist = ticker.history(period="1y")
if hist.empty:
return 0
hist.reset_index(inplace=True)
hist = hist.dt.strftime("%Y-%m-%d")
count = 0
for _, row in hist.iterrows():
self.db.execute_write(
"""INSERT OR IGNORE INTO quotes (symbol, date, open, high, low, close, volume)
VALUES (?,?,?,?,?,?,?)""",
(symbol, row, row['Open'], row['High'], row['Low'], row['Close'], row['Volume'])
)
# Mise à jour si existe (pour corriger les données partielles)
self.db.execute_write(
"""UPDATE quotes SET close=?, volume=? WHERE symbol=? AND date=?""",
(row['Close'], row['Volume'], symbol, row)
)
count += 1
return count
except Exception as e:
logging.error(f"Erreur sur {symbol}: {e}")
return 0
total_updates = 0
with ThreadPoolExecutor(max_workers=CONFIG) as executor:
futures =
for future in futures:
total_updates += future.result()
logging.info(f"Mise à jour terminée. {total_updates} points de données traités.")
def scan_opportunities(self):
"""Le Cerveau : Analyse et trouve les meilleures actions."""
opportunities =
for symbol, name in TICKERS_BVC.items():
df = self.db.fetch_df(f"SELECT * FROM quotes WHERE symbol = '{symbol}' ORDER BY date ASC")
if df.empty or len(df) < 200:
continue
# Nettoyage et typage
df['close'] = pd.to_numeric(df['close'])
df['volume'] = pd.to_numeric(df['volume'])
# Application Indicateurs
df = TechnicalAnalyst.calculate_indicators(df)
current = df.iloc[-1]
prev = df.iloc[-2]
# --- FILTRE 1: LIQUIDITÉ (CRITIQUE MAROC) ---
# On ignore si le volume moyen en MAD (Prix * Volume) est trop faible
avg_vol_mad = current['avg_vol_20'] * current['close']
if avg_vol_mad < CONFIG:
continue # Action illiquide, danger pour petit porteur
signal, score, details = TechnicalAnalyst.get_signal(current, prev)
if score > 0: # On ne garde que le positif
opportunities.append({
"symbol": symbol,
"name": name,
"price": current['close'],
"rsi": current['rsi'],
"score": score,
"signal": signal,
"details": details,
"vol_mad": avg_vol_mad
})
# Tri par score décroissant
opportunities.sort(key=lambda x: x['score'], reverse=True)
return opportunities
--- MODULE 4: NOTIFICATION & PÉDAGOGIE ---
class TelegramBot:
def init(self, token, chat_id):
self.token = token
self.chat_id = chat_id
self.api_url = f"https://api.telegram.org/bot{token}/sendMessage"
def send_message(self, text):
if not self.token:
print("LOG (No Token):", text)
return
# Gestion des longs messages (Chunking)
chunks = [text[i:i+4000] for i in range(0, len(text), 4000)]
for chunk in chunks:
try:
payload = {"chat_id": self.chat_id, "text": chunk, "parse_mode": "Markdown"}
requests.post(self.api_url, json=payload)
time.sleep(1) # Anti-spam
except Exception as e:
logging.error(f"Erreur Telegram: {e}")
def generate_daily_report(self, opportunities):
lines =
lines.append(f"📅 {datetime.date.today().strftime('%d/%m/%Y')}")
lines.append("")
# Section 1: Money Management (Instruction)
lines.append("🎓 Conseil Gestion Capital :")
lines.append(f"Votre apport mensuel : {CONFIG} MAD")
# Calcul des frais d'impact
frais_estimes = max(CONFIG * CONFIG, CONFIG)
impact_pct = (frais_estimes / CONFIG) * 100
if impact_pct > 1.0:
lines.append(f"⚠️ Attention Frais : Acheter pour 3000 Dhs vous coûte ~{impact_pct:.1f}% en frais.")
lines.append("💡 Astuce : Groupez vos achats tous les 2 mois (6000 Dhs) pour réduire l'impact sous 0.5%.")
else:
lines.append("✅ Taille d'ordre optimale pour minimiser les frais.")
lines.append("")
# Section 2: Top Opportunités
lines.append("🚀 Top Opportunités (Liquidité Validée) :")
if not opportunities:
lines.append("🚫 Le marché est indécis ou illiquide aujourd'hui. Cash is King.")
for opp in opportunities[:4]: # Top 4 seulement
icon = "🔥" if opp['score'] >= 4 else "📈"
lines.append(f"{icon} {opp['name']} ({opp['symbol']})")
lines.append(f"   Prix: {opp['price']:.2f} MAD | RSI: {opp['rsi']:.1f}")
lines.append(f"   📊 Signal : {opp['signal']}")
lines.append(f"   💡 Pourquoi? {', '.join(opp['details'])}")
# Petit calcul de rendement théorique dividende (Hardcodé pour l'exemple, à dynamiser)
# Dans une version V3, on scraperait le dividende exact
lines.append("")
lines.append("⚠️ Ceci est une aide à la décision, pas un conseil financier certifié.")
return "\n".join(lines)
--- ORCHESTRATEUR PRINCIPAL ---
def main():
logging.info("Démarrage du Bot BVC...")
# 1. Init DB
db = DatabaseHandler(CONFIG)
try:
# 2. Update Data
intel = MarketIntelligence(db)
intel.update_market_data()
# 3. Analyse
opps = intel.scan_opportunities()
# 4. Rapport
bot = TelegramBot(CONFIG, CONFIG)
report = bot.generate_daily_report(opps)
bot.send_message(report)
logging.info("Rapport envoyé avec succès.")
except Exception as e:
logging.error(f"Erreur main loop: {e}")
finally:
db.stop()
if name == "main":
main()