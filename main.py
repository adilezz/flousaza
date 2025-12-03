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

def setup_driver():
    chrome_options = Options()
    chrome_options.add_argument("--headless") 
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--window-size=1920,1080") # On simule un grand écran
    chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36")
    
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=chrome_options)
    return driver

def analyze_market():
    print("🚀 Démarrage du navigateur Chrome...")
    driver = setup_driver()
    
    try:
        driver.get(URL_WAFA)
        print("⏳ Chargement de la page...")
        
        # --- CORRECTION MAJEURE ICI ---
        # On attend spécifiquement qu'une grosse action apparaisse pour être sûr que la liste est là
        wait = WebDriverWait(driver, 25)
        print("👀 En attente de l'affichage des données (Maroc Telecom)...")
        wait.until(EC.text_to_be_present_in_element((By.TAG_NAME, "body"), "MAROC TELECOM"))
        
        # On récupère toutes les lignes de tous les tableaux pour trouver le bon
        rows = driver.find_elements(By.TAG_NAME, "tr")
        print(f"✅ Page chargée ! J'ai trouvé {len(rows)} lignes au total.")
        
        message = "🔔 **Rapport Wafa Bourse**\n\n"
        count = 0
        actions_analysees = 0
        
        for row in rows:
            cols = row.find_elements(By.TAG_NAME, "td")
            
            # Le vrai tableau a généralement au moins 3-4 colonnes
            if len(cols) < 3: continue
            
            # Extraction du texte
            nom_brut = cols[0].text.strip()
            if not nom_brut: continue

            # Vérification : Est-ce bien une ligne d'action ? (On ignore les titres)
            if "Valeur" in nom_brut or "Cours" in nom_brut: continue
            
            actions_analysees += 1
            
            # --- RECUPERATION DONNEES ---
            # Wafa : Col 0 = Nom, Col 1 = Cours, Col 2 = Var %
            prix_txt = cols[1].text.strip()
            var_txt = cols[2].text.strip()
            
            prix = clean_number(prix_txt)
            var = clean_number(var_txt)
            
            # Si le prix est 0, c'est probablement une erreur de lecture ou action suspendue
            if prix == 0: continue

            # --- STRATÉGIE ---
            # 1. Baisse Swing
            if var < -2.0:
                message += f"📉 **{nom_brut}**\n   Var: {var}%\n   Prix: {prix} DH\n\n"
                count += 1
                
            # 2. Favoris (Ajoute ici tes actions préférées)
            mes_favoris = ["MAROC TELECOM", "ATTIJARI", "COSUMAR", "TGCC"]
            # On vérifie si le nom de l'action contient un de tes favoris
            is_fav = any(fav in nom_brut.upper() for fav in mes_favoris)
            
            if is_fav and var < -0.1: # On veut savoir dès que ça baisse un tout petit peu
                 message += f"💎 **{nom_brut}** ({var}%)\n   Prix: {prix} DH\n\n"
                 count += 1

        print(f"📊 Analyse terminée sur {actions_analysees} actions.")
        
        if count > 0:
            send_telegram(message)
        else:
            print("R.A.S - Le marché est calme, pas d'alerte envoyée.")
            # OPTIONNEL : Décommente la ligne ci-dessous pour recevoir un message de confirmation "Tout va bien"
            # send_telegram(f"✅ Scan terminé sur {actions_analysees} actions. Aucune opportunité détectée.")

    except Exception as e:
        print(f"❌ Erreur : {e}")
        # En cas d'erreur, on prend une capture d'écran virtuelle pour comprendre (invisible pour toi mais utile)
        print("Page source debug (extrait):", driver.page_source[:500]) 
    finally:
        driver.quit()

def clean_number(txt):
    if not txt: return 0.0
    # On enlève tout sauf les chiffres, le moins et la virgule
    clean = txt.replace(' ', '').replace('%', '').replace(',', '.')
    # Gestion spéciale pour les formats bizarres
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
