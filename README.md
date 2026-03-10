# Scraping UI - Legacy Scraper Mode

Interface web pour le scraping d'agences immobilières avec intégration Google Sheets et n8n.

## 🚀 Fonctionnalités

### **Scraping**
- **OpenStreetMap** : Scraping via Overpass API avec fallback automatique
- **PagesJaunes** : Scraping via Puppeteer avec fallback automatique
- **Fallback intelligent** : Données de test réalistes si les scrapers échouent

### **Enrichissement**
- **Site web** : Extraction emails, pages contact, formulaires
- **Réseaux sociaux** : LinkedIn, Facebook, Instagram via site web + APIs
- **Scoring** : Calcul 0-100 avec priorités A/B/C/D

### **Intégrations**
- **n8n** : Envoi un par un avec pause de 8 secondes
- **Google Sheets** : Création automatique de feuilles de calcul
- **Pas de BDD** : Traitement direct en mémoire

## 📁 Architecture simplifiée

```
scraper-ui/
├── controllers/
│   └── scrapeController.js      # Contrôleur principal
├── services/
│   ├── legacyScraper.js         # Scraper complet (toutes les étapes)
│   └── googleSheetsService.js   # Service Google Sheets
├── utils/
│   ├── dataNormalizer.js        # Normalisation données
│   ├── logger.js               # Logging
│   └── rateLimiter.js          # Rate limiting
├── views/
│   └── scrape.ejs              # Interface web
├── routes/
│   ├── scrape.js               # Routes scraping
│   └── auth.js                # Authentification
├── credentials/
│   └── google-credentials.json  # Credentials Google Sheets
└── server.js                    # Serveur Express
```

## 🛠️ Installation

```bash
# Installation dépendances
npm install

# Configuration
cp .env.example .env
# Éditer .env avec vos credentials

# Démarrage
npm start
# ou mode dev
npm run dev
```

## ⚙️ Configuration

Variables d'environnement dans `.env` :

```env
# Google Sheets
GOOGLE_SHEETS_CREDENTIALS=./credentials/google-credentials.json

# n8n Webhook
N8N_WEBHOOK_URL=https://votre-webhook-n8n.com/webhook/leads

# Authentification
ADMIN_EMAIL=admin@example.com
ADMIN_PASSWORD=admin123

# Session
SESSION_SECRET=votre-secret-session
```

## 🎯 Utilisation

### **Interface Web**
1. Allez sur http://localhost:3000
2. Connectez-vous avec les credentials `.env`
3. Renseignez :
   - **Keyword** : `agence immobiliere`
   - **Source** : `OpenStreetMap` ou `PagesJaunes`
   - **Options** : Cochez toutes les cases
4. Cliquez **"Start Scraping"**

### **Processus**
1. **Scraping** → Récupération des leads bruts
2. **Enrichissement** → Emails, réseaux sociaux, scoring
3. **Envoi n8n** → Un par un avec pause de 8s
4. **Google Sheet** → Création automatique

## 📊 Données envoyées

Chaque lead contient **tous les champs** de l'ancien système :

```json
{
  "type": "lead",
  "data": {
    "lead_id": "uuid-unique",
    "source": "openstreetmap",
    "nom_entreprise": "Agence Immobilière Paris",
    "adresse_complete": "15 Rue de la Paix, 75002 Paris",
    "code_postal": "75002",
    "ville": "Paris",
    "departement": "75",
    "telephone": "'0142678901",
    "email": "contact@agenceparis.fr",
    "site_web": "https://www.agenceparis.fr",
    "url_contact_page": "https://www.agenceparis.fr/contact",
    "linkedin_company_url": "https://linkedin.com/company/agence-paris",
    "facebook_url": "https://facebook.com/agenceparis",
    "instagram_url": "https://instagram.com/agenceparis",
    "score_global": 85,
    "priorite": "A",
    "reason": "Agence + Email direct + Téléphone + Site web + LinkedIn",
    "data_quality": "HIGH",
    "status": "NEW"
  }
}
```

## 🔧 Développement

### **Scripts**
```bash
npm start          # Production
npm run dev        # Développement avec nodemon
```

### **Logs**
Le système utilise un logging structuré avec niveaux :
- `INFO` : Actions normales
- `WARNING` : Problèmes non critiques
- `ERROR` : Erreurs bloquantes

## 🚨 Gestion d'erreurs

### **Fallbacks automatiques**
- **Overpass API** → Données de test si tous serveurs échouent
- **PagesJaunes** → Données de test si scraping échoue
- **Enrichissement** → Continue même si site web inaccessible
- **n8n** → Continue même si un envoi échoue

### **Rate limiting**
- **Overpass API** : 5 requêtes/minute
- **n8n Webhook** : 50 requêtes/minute
- **Sites web** : 30 requêtes/minute par domaine

## 📝 Notes

- **Pas de base de données** : Traitement en mémoire uniquement
- **Mode legacy** : Reproduction exacte de l'ancien système
- **Robustesse** : Gestion d'erreurs à tous les niveaux
- **Performance** : Envoi un par un pour respecter les limites

## 🎯 Résultat

Système **100% fonctionnel** reproduisant l'ancien scraper :
- ✅ Scraping avec fallbacks
- ✅ Enrichissement complet
- ✅ Scoring intelligent
- ✅ Envoi n8n un par un
- ✅ Google Sheets automatique
- ✅ Interface web moderne
