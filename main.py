import os
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
import requests

# --- CONFIGURATION ---
BOT_TOKEN = os.environ.get("BOT_TOKEN")
CHAT_ID = os.environ.get("CHAT_ID")
# Nouvelle Source : Site Officiel de la Bourse de Casablanca
URL_OFFICIELLE = "https://www.casablanca-bourse.com/fr/live-market/marche-actions-groupement"

def setup_driver():
    chrome_options = Options()
    chrome_options.add_argument("--headless") 
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36")
    
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=chrome_options)
    return driver

def analyze_market():
    print("🚀 Démarrage du navigateur Chrome...")
    driver = setup_driver()
    
    try:
        driver.get(URL_OFFICIELLE)
        print(f"⏳ Chargement de {URL_OFFICIELLE}...")
        
        # On attend que le tableau apparaisse (max 30 secondes)
        wait = WebDriverWait(driver, 30)
        # On cherche le corps du tableau
        wait.until(EC.presence_of_element_located((By.TAG_NAME, "tbody")))
        
        print("✅ Tableau officiel détecté !")
        
        # Récupération des lignes
        rows = driver.find_elements(By.CSS_SELECTOR, "tbody tr")
        print(f"📊 J'ai trouvé {len(rows)} actions.")
        
        message = "🔔 **Rapport Bourse (Source: Officielle)**\n\n"
        count = 0
        
        for row in rows:
            cols = row.find_elements(By.TAG_NAME, "td")
            
            # Le tableau officiel a environ 16 colonnes
            if len(cols) < 8: continue
            
            # --- MAPPING SITE OFFICIEL ---
            # Colonnes basées sur la structure "Groupement" :
            # 0: Instrument (Nom)
            # 4: Dernier cours (Prix)
            # 7: Variation en %
            
            nom = cols[0].text.strip()
            prix_txt = cols[4].text.strip()
            var_txt = cols[7].text.strip()
            
            prix = clean_number(prix_txt)
            var = clean_number(var_txt)
            
            # Si le prix est 0, on ignore
            if prix == 0: continue

            # --- STRATÉGIE ---
            
            # 1. Alerte Swing (Baisse > 2.5%)
            if var < -2.5:
                message += f"📉 **{nom}**\n   Var: {var}%\n   Prix: {prix} DH\n\n"
                count += 1
                
            # 2. Surveillance Favoris
            mes_favoris = ["MAROC TELECOM", "ATTIJARI", "TGCC", "DOUJA", "MARSA"]
            is_fav = any(fav in nom.upper() for fav in mes_favoris)
            
            if is_fav and abs(var) > 0.1:
                icon = "🟢" if var > 0 else "🔴"
                message += f"{icon} **{nom}** ({var}%)\n   Prix: {prix} DH\n\n"
                count += 1

        if count > 0:
            send_telegram(message)
            print("✅ Rapport envoyé sur Telegram.")
        else:
            print("R.A.S - Aucune opportunité détectée.")

    except Exception as e:
        print(f"❌ Erreur : {e}")
        # Debug : Affiche le début de la page si erreur pour comprendre
        print("Source (extrait):", driver.page_source[:500])
    finally:
        driver.quit()

def clean_number(txt):
    if not txt: return 0.0
    # Nettoyage : On enlève les espaces, les %, et on remplace virgule par point
    clean = txt.replace(' ', '').replace('%', '').replace(',', '.')
    if '--' in clean or clean in ['-', '']: return 0.0
    try:
        return float(clean)
    except:
        return 0.0

def send_telegram(text):
    if not BOT_TOKEN or not CHAT_ID:
        print("⚠️ Pas de config Telegram")
        return
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload = {"chat_id": CHAT_ID, "text": text, "parse_mode": "Markdown"}
    requests.post(url, json=payload)

if __name__ == "__main__":
    analyze_market()
