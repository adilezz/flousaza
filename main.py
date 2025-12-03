import os
import time
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
URL_WAFA = "https://www.wafabourse.com/fr/market-tracking/instruments-financiers"

# --- CONFIGURATION DU NAVIGATEUR INVISIBLE ---
def setup_driver():
    chrome_options = Options()
    chrome_options.add_argument("--headless") # Pas d'interface graphique
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36")
    
    # Installation automatique du driver Chrome
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=chrome_options)
    return driver

def analyze_market():
    print("🚀 Démarrage du navigateur Chrome...")
    driver = setup_driver()
    
    try:
        driver.get(URL_WAFA)
        print("⏳ Chargement de Wafa Bourse...")
        
        # On attend jusqu'à 20 secondes que le tableau apparaisse
        # On cherche une balise <tbody> qui contient les données
        wait = WebDriverWait(driver, 20)
        wait.until(EC.presence_of_element_located((By.TAG_NAME, "tbody")))
        
        # On récupère toutes les lignes du tableau
        rows = driver.find_elements(By.XPATH, "//table//tbody/tr")
        print(f"✅ Données chargées ! {len(rows)} actions trouvées.")
        
        message = "🔔 **Rapport Wafa Bourse**\n\n"
        count = 0
        
        for row in rows:
            cols = row.find_elements(By.TAG_NAME, "td")
            
            # Vérification basique de la structure de la ligne
            if len(cols) < 3: continue
            
            # --- MAPPING WAFA BOURSE ---
            # Colonne 1 : Nom (parfois contient le code ISIN, on nettoie)
            # Colonne 2 : Cours (Prix)
            # Colonne 3 : Variation %
            
            nom_brut = cols[0].text.strip()
            prix_txt = cols[1].text.strip()
            var_txt = cols[2].text.strip() # Souvent avec flèche ou couleur
            
            # Nettoyage des chiffres
            prix = clean_number(prix_txt)
            var = clean_number(var_txt)
            
            # Nettoyage du nom (Wafa met parfois des retours à la ligne)
            nom = nom_brut.split('\n')[0]

            if prix == 0: continue

            # --- STRATÉGIE ---
            
            # 1. Baisse significative (Opportunité)
            if var < -2.0:
                message += f"📉 **{nom}**\n   Var: {var}%\n   Prix: {prix} DH\n\n"
                count += 1
                
            # 2. Dividendes / Blue Chips (Surveillance)
            favorites = ["MAROC TELECOM", "ATTIJARI", "CIMENTS", "COSUMAR"]
            is_fav = any(fav in nom.toUpperCase() for fav in favorites) if hasattr(nom, 'toUpperCase') else False
            
            if is_fav and var < -0.5:
                message += f"💎 **Favori en baisse : {nom}** ({var}%)\n\n"
                count += 1

        if count > 0:
            send_telegram(message)
        else:
            print("R.A.S - Aucune opportunité détectée.")

    except Exception as e:
        print(f"❌ Erreur critique : {e}")
    finally:
        driver.quit() # Ferme le navigateur pour économiser la mémoire

def clean_number(txt):
    if not txt: return 0.0
    # Nettoyage agressif : on ne garde que chiffres, virgule, point, moins
    clean = txt.replace(' ', '').replace('%', '').replace(',', '.')
    # Gestion des tirets bizarres ou symboles de Wafa
    if '--' in clean or clean == '-': return 0.0
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
