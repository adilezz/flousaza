import os
import sqlite3
import pandas as pd
import requests
import numpy as np
from telegram import Update
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes
from flask import Flask
from threading import Thread

# --- CONFIGURATION ---
# IMPORTANT : Vérifie que c'est bien l'URL de TON dépôt (branche main ou master)
GITHUB_DB_URL = "https://github.com/adilezz/flousaza/raw/main/bourse_casa.db"
DB_LOCAL_PATH = "bourse_casa.db"
TOKEN = os.environ.get("BOT_TOKEN")

# --- SERVEUR FLASK (Keep-Alive pour Render) ---
# Render a besoin qu'on écoute sur un port, sinon il tue l'app.
app = Flask('')

@app.route('/')
def home():
    return "🤖 Bot Bourse Casa est EN LIGNE !"

def run_http():
    # Render donne le port via la variable PORT, défaut 8080
    port = int(os.environ.get("PORT", 8080))
    app.run(host='0.0.0.0', port=port)

def keep_alive():
    t = Thread(target=run_http)
    t.start()

# --- GESTION DONNÉES ---
def download_db():
    """Télécharge la dernière DB à jour depuis GitHub."""
    print("⬇️ Téléchargement de la base de données depuis GitHub...")
    try:
        r = requests.get(GITHUB_DB_URL)
        if r.status_code == 200:
            with open(DB_LOCAL_PATH, 'wb') as f:
                f.write(r.content)
            print("✅ DB téléchargée et prête.")
        else:
            print(f"❌ Erreur téléchargement DB (Code {r.status_code})")
    except Exception as e:
        print(f"❌ Exception download: {e}")

def get_db_connection():
    return sqlite3.connect(DB_LOCAL_PATH)

def get_stock_data(symbol):
    """Récupère l'historique nettoyé pour un symbole."""
    conn = get_db_connection()
    try:
        # Récup ID
        cur = conn.execute("SELECT id, name FROM instruments WHERE symbol = ?", (symbol,))
        res = cur.fetchone()
        if not res: return None, None
        inst_id, name = res
        
        # Récup Data
        df = pd.read_sql_query(
            'SELECT Date, "Dernier_cours" as close FROM historical_quotes WHERE instrument_id = ? ORDER BY Date ASC',
            conn, params=(inst_id,)
        )
        df['Date'] = pd.to_datetime(df['Date'])
        df.set_index('Date', inplace=True)
        return name, df
    except Exception as e:
        print(f"Erreur SQL: {e}")
        return None, None
    finally:
        conn.close()

# --- INDICATEURS INVESTISSEUR (LONG TERME) ---
def calculate_investor_metrics(df):
    if len(df) < 250: return None # Il faut au moins 1 an d'historique
    
    curr_price = df['close'].iloc[-1]
    
    # 1. Tendance de fond (Moyenne Mobile 200 jours)
    sma200 = df['close'].rolling(200).mean().iloc[-1]
    trend = "HAUSSIERE 🟢" if curr_price > sma200 else "BAISSIERE 🔴"
    
    # 2. Volatilité (Risque) sur 1 an
    # Écart-type des rendements journaliers * racine(252 jours)
    volatility = df['close'].pct_change().std() * np.sqrt(252) * 100
    risk_label = "FAIBLE ✅" if volatility < 15 else "MODÉRÉ ⚠️" if volatility < 25 else "ÉLEVÉ 🚨"
    
    # 3. CAGR (Taux de croissance annuel moyen) sur 3 ans
    cagr_3y = 0.0
    if len(df) > 756: # ~3 ans de bourse
        start_price = df['close'].iloc[-756]
        # Formule : (ValFin / ValInit)^(1/n) - 1
        cagr_3y = ((curr_price / start_price) ** (1/3) - 1) * 100
        
    return {
        "price": curr_price,
        "sma200": sma200,
        "trend": trend,
        "volatility": volatility,
        "risk_label": risk_label,
        "cagr_3y": cagr_3y
    }

# --- COMMANDES TELEGRAM ---
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    download_db() # Force la mise à jour au start
    await update.message.reply_text(
        "👋 **Bienvenue sur Bourse Casa Bot !**\n\n"
        "Je suis votre assistant d'investissement Long Terme.\n"
        "La base de données a été synchronisée.\n\n"
        "📜 **Commandes disponibles :**\n"
        "🔹 `/rapport IAM` : Analyse fondamentale & Risque\n"
        "🔹 `/simuler 10000 ATW` : Simulation d'investissement sur 5 ans\n"
        "🔹 `/maj` : Force le re-téléchargement de la base de données",
        parse_mode='Markdown'
    )

async def rapport(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("❌ Indiquez un symbole. Exemple: `/rapport IAM`")
        return
    
    symbol = context.args[0].upper()
    name, df = get_stock_data(symbol)
    
    if df is None or df.empty:
        await update.message.reply_text(f"❌ Action '{symbol}' introuvable ou historique vide.")
        return

    m = calculate_investor_metrics(df)
    if not m:
        await update.message.reply_text("⚠️ Pas assez d'historique (min 1 an) pour l'analyse investisseur.")
        return
    
    msg = (
        f"📊 **RAPPORT INVESTISSEUR : {name}**\n"
        f"🏷 Symbole : #{symbol}\n\n"
        f"💰 **Cours Actuel : {m['price']:.2f} MAD**\n"
        f"📈 Tendance (SMA200) : {m['trend']}\n\n"
        f"🛡 **Profil de Risque :**\n"
        f"• Volatilité anuelle : {m['volatility']:.1f}% ({m['risk_label']})\n\n"
        f"🚀 **Performance Croissance :**\n"
        f"• CAGR 3 ans : {m['cagr_3y']:+.2f}% / an\n"
        f"(C'est la rentabilité moyenne annuelle lissée sur 3 ans)\n"
    )
    await update.message.reply_text(msg, parse_mode='Markdown')

async def simulation(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        amount = float(context.args[0])
        symbol = context.args[1].upper()
    except (IndexError, ValueError):
        await update.message.reply_text("❌ Erreur. Usage : `/simuler [MONTANT] [SYMBOLE]`\nExemple : `/simuler 5000 IAM`")
        return

    name, df = get_stock_data(symbol)
    if df is None:
        await update.message.reply_text("Action introuvable.")
        return

    # Simulation : Achat au tout début de l'historique disponible (max 5-10 ans selon la base)
    start_date = df.index[0]
    end_date = df.index[-1]
    years = (end_date - start_date).days / 365.25
    
    start_price = df['close'].iloc[0]
    end_price = df['close'].iloc[-1]
    
    # Combien d'actions on aurait acheté ?
    shares = int(amount // start_price)
    rest = amount % start_price
    
    final_value = (shares * end_price) + rest
    plus_value = final_value - amount
    perf_total = (plus_value / amount) * 100
    
    msg = (
        f"💼 **SIMULATION PAPER TRADING**\n"
        f"Action : {name} (#{symbol})\n"
        f"⏳ Durée : {years:.1f} années\n\n"
        f"📥 **Investissement Initial :** {amount:,.0f} MAD\n"
        f"   (Date : {start_date.strftime('%d/%m/%Y')} à {start_price:.2f} MAD)\n\n"
        f"🏁 **Valeur Aujourd'hui :** {final_value:,.2f} MAD\n"
        f"💵 Gain/Perte : {plus_value:+,.2f} MAD\n"
        f"📊 Performance Totale : **{perf_total:+.2f}%**"
    )
    await update.message.reply_text(msg, parse_mode='Markdown')

async def force_update(update: Update, context: ContextTypes.DEFAULT_TYPE):
    download_db()
    await update.message.reply_text("✅ Base de données re-téléchargée depuis GitHub.")

# --- MAIN LOOP ---
if __name__ == '__main__':
    # 1. Lancer le serveur HTTP (thread séparé) pour Render
    keep_alive()
    
    # 2. Vérifier token
    if not TOKEN:
        print("⚠️ ERREUR: Variable BOT_TOKEN manquante.")
        exit(1)
        
    # 3. Préparer le bot
    download_db()
    app_bot = ApplicationBuilder().token(TOKEN).build()
    
    # 4. Ajouter les commandes
    app_bot.add_handler(CommandHandler("start", start))
    app_bot.add_handler(CommandHandler("rapport", rapport))
    app_bot.add_handler(CommandHandler("simuler", simulation))
    app_bot.add_handler(CommandHandler("maj", force_update))
    
    print("🤖 Bot Telegram en écoute...")
    app_bot.run_polling()
