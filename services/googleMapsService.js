const fetch = require('node-fetch');
const puppeteer = require('puppeteer-extra');
const StealthPlugin = require('puppeteer-extra-plugin-stealth');

puppeteer.use(StealthPlugin());
const { logInfo, logWarning } = require('../utils/logger');

const GOOGLE_PLACES_API_KEY = process.env.GOOGLE_PLACES_API_KEY;

// Rate limiting simple (Places peut coûter cher)
class RateLimiter {
  constructor(options = {}) {
    this.maxPerWindow = options.maxPerWindow || 60;
    this.windowMs = options.windowMs || 60_000;
    this.requests = [];
  }

  async acquire(key) {
    const now = Date.now();
    this.requests = this.requests.filter(t => t > now - this.windowMs);
    if (this.requests.length >= this.maxPerWindow) {
      const waitTime = this.requests[0] + this.windowMs - now;
      logInfo(`⏳ Rate limit (${key}) – attente ${Math.round(waitTime / 1000)}s…`);
      await new Promise(r => setTimeout(r, waitTime));
      return this.acquire(key);
    }
    this.requests.push(now);
    return true;
  }
}

const GMAPS_RATE_LIMITER = new RateLimiter({ maxPerWindow: 30, windowMs: 60 * 1000 });

function normalizePhoneDigits(phone) {
  if (!phone) return '';
  return String(phone).replace(/[^\d]/g, '');
}

function looksLikeSameCity(lead, formattedAddress) {
  if (!lead?.ville || !formattedAddress) return true;
  const v = String(lead.ville).toLowerCase().trim();
  const addr = String(formattedAddress).toLowerCase();
  return v.length < 2 || addr.includes(v);
}

function parseFrenchNumber(str) {
  if (!str) return null;
  const cleaned = String(str).replace(/\s/g, '');
  const m = cleaned.match(/(\d+(?:[.,]\d+)?)/);
  if (!m) return null;
  const n = Number(m[1].replace(',', '.'));
  return Number.isFinite(n) ? n : null;
}

function isPoorLead(lead) {
  // Un lead est considéré comme "pauvre" sauf s'il a déjà de très bonnes données :
  // - Score global élevé (>= 50)
  // - Qualité de données HIGH
  // - A déjà téléphone ET email ET site web
  const hasHighScore = lead.score_global != null && lead.score_global >= 50;
  const hasHighDataQuality = lead.data_quality === 'HIGH';
  const hasAllEssential = lead.telephone && lead.email && lead.site_web;
  
  // Ne PAS enrichir si le lead a déjà de très bonnes données
  if (hasHighScore || hasHighDataQuality || hasAllEssential) {
    return false;
  }
  
  // Sinon, enrichir tous les autres leads
  return true;
}

async function tryAcceptConsent(page) {
  // Best effort: Google consent screen (varie selon pays/compte)
  const selectors = [
    'button[aria-label*="Accepter"]',
    'button:has-text("J\\x27accepte")',
    'button:has-text("Tout accepter")',
    'form [type="submit"]'
  ];
  for (const sel of selectors) {
    try {
      const el = await page.$(sel);
      if (el) {
        await el.click({ delay: 20 }).catch(() => {});
        await new Promise(r => setTimeout(r, 1000));
        return true;
      }
    } catch (_) {}
  }
  return false;
}

async function enrichWithGoogleMapsPuppeteer(lead, options = {}) {
  const { allowWebsiteOverride = false, forceEnrichment = false } = options;
  
  // Vérifier si le lead est "pauvre" (sauf si forcé)
  if (!forceEnrichment && !isPoorLead(lead)) {
    return lead;
  }
  
  if (!lead || !lead.nom_entreprise) return lead;
  if (lead.google_place_id) return lead;

  // Construction de la requête : nom + code postal (si dispo) ou ville
  const name = String(lead.nom_entreprise).trim();
  const cp = lead.code_postal ? String(lead.code_postal).trim() : null;
  const city = lead.ville ? String(lead.ville).trim() : null;
  const locationPart = cp || city || '';
  
  const query = locationPart ? `${name} ${locationPart}` : name;
  const searchUrl = `https://www.google.com/maps/search/${encodeURIComponent(query)}`;

  // Liste d'user-agents aléatoires
  const userAgents = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/91.0.4472.124 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/90.0.4430.212 Safari/537.36",
    "Mozilla/5.0 (Linux; Android 10; SM-G975F) AppleWebKit/537.36 Chrome/91.0.4472.120 Mobile Safari/537.36",
    "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:90.0) Gecko/20100101 Firefox/90.0"
  ];

  let browser;
  try {
    await GMAPS_RATE_LIMITER.acquire('google-maps-web');

    browser = await puppeteer.launch({ headless: 'new', args: ['--no-sandbox', '--disable-setuid-sandbox'] });
    const page = await browser.newPage();
    
    // User-agent aléatoire
    const ua = userAgents[Math.floor(Math.random() * userAgents.length)];
    await page.setUserAgent(ua);

    // Blocage des ressources inutiles
    await page.setRequestInterception(true);
    page.on('request', (req) => {
      const type = req.resourceType();
      if (['stylesheet', 'font', 'image'].includes(type)) {
        req.abort();
      } else {
        req.continue();
      }
    });

    // Aller à la page de recherche
    const response = await page.goto(searchUrl, { waitUntil: 'networkidle2', timeout: 120000 });
    logInfo(`📶 Google Maps HTTP Status: ${response?.status()} pour ${lead.nom_entreprise}`);

    // Gestion des cookies (identique)
    try {
      await page.waitForSelector('button[aria-label="Alle ablehnen"], button[aria-label*="Accepter"], button:has-text("Tout accepter")', { visible: true, timeout: 10000 });
      const consentButton = await page.$('button[aria-label="Alle ablehnen"]') ||
                           await page.$('button[aria-label*="Accepter"]') ||
                           await page.$('button:has-text("Tout accepter")');
      if (consentButton) {
        await consentButton.click({ delay: 100 });
        logInfo("✅ Cookies Google Maps rejetés/acceptés");
        await new Promise(r => setTimeout(r, 3000));
        await page.reload({ waitUntil: 'networkidle2' });
      }
    } catch (err) {
      logInfo("ℹ️ Aucun bouton de consentement Google Maps détecté");
    }

    // Vérification CAPTCHA
    const isCaptcha = await page.evaluate(() => {
      return !!document.querySelector('form[action*="sorry"]') || document.title.includes("unusual traffic");
    });

    if (isCaptcha) {
      logWarning("⚠️ CAPTCHA Google Maps détecté pour : " + lead.nom_entreprise);
      return lead;
    }

    // Attendre que la liste des résultats apparaisse (ou qu'on soit directement sur une fiche)
    await new Promise(r => setTimeout(r, 5000));

    // Vérifier si on est sur une page de liste de résultats
    const hasList = await page.evaluate(() => {
      return document.querySelectorAll('.Nv2PK, [data-result-index], [role="article"]').length > 0;
    });

    let selectedUrl = null;

    if (hasList) {
      logInfo("🔍 Liste de résultats détectée, recherche du meilleur correspondant...");
      
      // Récupérer les informations des premiers résultats (par ex. les 5 premiers)
      const results = await page.evaluate(() => {
        const items = [];
        const nodes = document.querySelectorAll('.Nv2PK, [data-result-index], [role="article"]');
        for (let i = 0; i < Math.min(nodes.length, 5); i++) {
          const node = nodes[i];
          const title = node.querySelector('.qBF1Pd, .fontHeadlineSmall, [data-attrid="title"], h3')?.innerText?.trim() || '';
          const address = node.querySelector('.W4Efsd span, [data-dtype="d3gf"]')?.innerText?.trim() || '';
          // Récupérer le lien vers la fiche (souvent un attribut href sur le conteneur ou un lien interne)
          const link = node.querySelector('a[href*="/place/"]')?.getAttribute('href') || null;
          items.push({ title, address, link });
        }
        return items;
      });

      // Calculer un score de similarité pour chaque résultat
      const nameLower = name.toLowerCase();
      const cpLower = cp ? cp.toLowerCase() : '';
      const cityLower = city ? city.toLowerCase() : '';

      let bestScore = -1;
      let bestResult = null;

      for (const res of results) {
        let score = 0;
        const titleLower = res.title.toLowerCase();
        const addressLower = res.address.toLowerCase();

        // Le titre doit contenir le nom (ou une partie significative)
        if (titleLower.includes(nameLower) || nameLower.includes(titleLower)) {
          score += 10;
        }
        // Bonus si le code postal est présent dans l'adresse
        if (cpLower && addressLower.includes(cpLower)) {
          score += 20;
        } else if (cityLower && addressLower.includes(cityLower)) {
          score += 15;
        }
        // Si le titre et l'adresse sont très similaires, on augmente le score
        // (on peut ajouter d'autres heuristiques)

        if (score > bestScore) {
          bestScore = score;
          bestResult = res;
        }
      }

      if (bestResult && bestResult.link) {
        // Construire l'URL complète (parfois le lien est relatif)
        const placeUrl = bestResult.link.startsWith('http') ? bestResult.link : `https://www.google.com${bestResult.link}`;
        logInfo(`✅ Résultat sélectionné: ${bestResult.title} (score ${bestScore})`);
        
        // Aller directement à l'URL de la fiche (évite un clic)
        await page.goto(placeUrl, { waitUntil: 'networkidle2', timeout: 60000 });
        selectedUrl = placeUrl;
      } else {
        // Fallback : cliquer sur le premier résultat
        logWarning("Aucun résultat pertinent trouvé, clic sur le premier");
        await page.click('.Nv2PK, [data-result-index], [role="article"]');
        await page.waitForFunction(() => {
          return window.location.href.includes('/place/') || document.querySelector('.RcCslf') !== null;
        }, { timeout: 15000 });
        selectedUrl = page.url();
      }
    } else {
      // Probablement déjà sur une page de détail
      logInfo("ℹ️ Page de détail détectée directement");
      selectedUrl = page.url();
    }

    // Attendre que la page de détail soit bien chargée
    await new Promise(r => setTimeout(r, 3000));

    // Extraire les informations depuis la fiche détaillée (sélecteurs spécifiques)
    const data = await page.evaluate(() => {
      const out = {
        placeUrl: window.location.href,
        website: null,
        phone: null,
        address: null,
        title: null
      };

      // Sélecteurs pour la fiche détaillée
      const titleSel = document.querySelector('h1, .DUwDvf, .fontHeadlineLarge');
      if (titleSel) out.title = titleSel.innerText;

      const websiteSel = document.querySelector('a[data-item-id="authority"], a[href*="http"]:not([href*="google"])');
      if (websiteSel) out.website = websiteSel.href || websiteSel.getAttribute('href');

      const phoneSel = document.querySelector('button[data-item-id="phone"], a[href^="tel:"]');
      if (phoneSel) {
        out.phone = phoneSel.innerText || phoneSel.getAttribute('aria-label');
      }

      const addressSel = document.querySelector('button[data-item-id="address"], [data-item-id="address"]');
      if (addressSel) {
        out.address = addressSel.innerText || addressSel.getAttribute('aria-label');
      }

      // Fallback si les sélecteurs spécifiques échouent
      if (!out.website) {
        const fallbackWebsite = document.querySelector('a[href*="http"]:not([href*="google"])');
        out.website = fallbackWebsite?.href || null;
      }
      if (!out.phone) {
        const fallbackPhone = document.querySelector('[aria-label*="téléphone"], [aria-label*="phone"]');
        out.phone = fallbackPhone?.innerText || null;
      }

      return out;
    });

    // --- Nettoyage du téléphone : extraire uniquement le numéro ---
    if (data.phone) {
      // Extraire le premier groupe de chiffres (au moins 10 chiffres) avec +, espaces, tirets, parenthèses
      const phoneMatch = data.phone.match(/[\d\s\+\(\)\-]{10,}/);
      if (phoneMatch) {
        data.phone = phoneMatch[0].trim();
        logInfo(`📞 Téléphone nettoyé: ${data.phone}`);
      } else {
        // Si aucun format valide, on ignore
        data.phone = null;
      }
    }

    // --- Résolution des URLs de redirection Google ---
    if (data.website && data.website.includes('google.com/url?q=')) {
      try {
        const urlParams = new URLSearchParams(new URL(data.website).search);
        const realUrl = urlParams.get('q');
        if (realUrl) {
          data.website = realUrl;
          logInfo(`🔗 URL de redirection Google résolue: ${realUrl}`);
        }
      } catch (e) {
        logWarning(`Impossible de parser l'URL de redirection: ${data.website}`);
      }
    }

    // Mise à jour du lead
    if (data.phone) {
      if (!lead.telephone || (data.phone.length > lead.telephone.length && data.phone.match(/\d/))) {
        lead.telephone = data.phone;
      }
    }
    if (data.website && !data.website.includes('google.com/maps') && (allowWebsiteOverride || !lead.site_web)) {
      lead.site_web = data.website;
    }
    // Optionnel : stocker l'URL de la fiche Google Maps
    if (data.placeUrl && data.placeUrl.includes('/place/')) {
      lead.google_maps_url = data.placeUrl;
    }

    // --- Extraction du code postal et de la ville depuis l'adresse Google Maps ---
    if (data.address) {
      // Recherche d'un code postal français (5 chiffres)
      const cpMatch = data.address.match(/\b(\d{5})\b/);
      if (cpMatch) {
        const cp = cpMatch[1];
        if (!lead.code_postal) {
          lead.code_postal = cp;
          logInfo(`📮 Code postal trouvé: ${cp}`);
        }
        // La ville est souvent après le code postal
        const parts = data.address.split(cp);
        if (parts.length > 1) {
          // Prendre la partie après le code postal, enlever les séparateurs
          let cityPart = parts[1].trim().replace(/^[,\s-]+/, '').split(',')[0].trim();
          // Nettoyer (enlever les caractères inutiles)
          cityPart = cityPart.replace(/[^\w\s-]/g, '').trim();
          if (cityPart && !lead.ville) {
            lead.ville = cityPart;
            logInfo(`🏙️ Ville trouvée: ${cityPart}`);
          }
        }
      }
    }

    // Log détaillé des enrichissements
    const enrichissements = [];
    if (data.phone && (!lead.telephone || data.phone !== lead.telephone)) {
      enrichissements.push(`📞 téléphone: ${data.phone}`);
    }
    if (data.website && !data.website.includes('google.com/maps') && (allowWebsiteOverride || !lead.site_web)) {
      enrichissements.push(`🌐 site web: ${data.website}`);
    }

    logInfo(`🗺️ Google Maps (web) enrichi: ${lead.nom_entreprise}`, {
      enrichissements: enrichissements.length > 0 ? enrichissements.join(', ') : 'aucun nouvel ajout',
      selectedUrl,
      newPhone: !!(data.phone && (!lead.telephone || data.phone !== lead.telephone)),
      newWebsite: !!(data.website && !data.website.includes('google.com/maps') && (allowWebsiteOverride || !lead.site_web)),
      foundData: {
        phone: !!data.phone,
        website: !!data.website && !data.website.includes('google.com/maps'),
        address: !!data.address,
        title: !!data.title
      }
    });

  } catch (err) {
    logWarning(`Google Maps web enrichment error: ${err.message}`, { company: lead?.nom_entreprise });
  } finally {
    if (browser) await browser.close().catch(() => {});
  }
  return lead;
}

async function placesTextSearch(query) {
  const url = new URL('https://maps.googleapis.com/maps/api/place/textsearch/json');
  url.searchParams.set('query', query);
  url.searchParams.set('key', GOOGLE_PLACES_API_KEY);
  url.searchParams.set('language', 'fr');
  url.searchParams.set('region', 'fr');
  const res = await fetch(url.toString());
  const data = await res.json();
  return data;
}

async function placeDetails(placeId) {
  const url = new URL('https://maps.googleapis.com/maps/api/place/details/json');
  url.searchParams.set('place_id', placeId);
  url.searchParams.set('fields', [
    'place_id',
    'name',
    'formatted_address',
    'rating',
    'user_ratings_total',
    'website',
    'formatted_phone_number',
    'international_phone_number',
    'opening_hours'
  ].join(','));
  url.searchParams.set('key', GOOGLE_PLACES_API_KEY);
  url.searchParams.set('language', 'fr');
  const res = await fetch(url.toString());
  const data = await res.json();
  return data;
}

/**
 * Enrichit un lead via Google Places (si la clé est configurée).
 * N'est PAS une source de scraping, uniquement une source d'enrichissement.
 * Ne s'active que pour les leads considérés comme "pauvres" en données.
 */
async function enrichWithGoogleMaps(lead, options = {}) {
  const { allowWebsiteOverride = false, forceEnrichment = false } = options;
  
  // Vérifier si le lead est "pauvre" (sauf si forcé)
  if (!forceEnrichment && !isPoorLead(lead)) {
    return lead;
  }
  
  logInfo(`🎯 Google Maps enrichment: ${lead.nom_entreprise}`, {
    score: lead.score_global,
    dataQuality: lead.data_quality,
    hasPhone: !!lead.telephone,
    hasEmail: !!lead.email,
    hasWebsite: !!lead.site_web,
    skipReason: !isPoorLead(lead) ? 'déjà de bonnes données' : 'lead à enrichir'
  });
  
  // Sans clé API → fallback Puppeteer (best-effort)
  if (!GOOGLE_PLACES_API_KEY) {
    return enrichWithGoogleMapsPuppeteer(lead, { allowWebsiteOverride });
  }
  if (!lead || !lead.nom_entreprise) return lead;

  // Déjà enrichi
  if (lead.google_place_id) return lead;

  const name = String(lead.nom_entreprise).trim();
  const city = (lead.ville ? String(lead.ville).trim() : '');
  const dept = (lead.departement ? String(lead.departement).trim() : '');
  const hint = [city, dept].filter(Boolean).join(' ');

  const query = [name, hint].filter(Boolean).join(' ');

  try {
    await GMAPS_RATE_LIMITER.acquire('google-places');

    const searchData = await placesTextSearch(query);
    if (searchData.status !== 'OK' || !Array.isArray(searchData.results) || searchData.results.length === 0) {
      return lead;
    }

    const candidate = searchData.results[0];
    const placeId = candidate.place_id;
    if (!placeId) return lead;

    await GMAPS_RATE_LIMITER.acquire('google-places');
    const detailsData = await placeDetails(placeId);
    if (detailsData.status !== 'OK' || !detailsData.result) return lead;

    const r = detailsData.result;

    // Heuristique légère pour éviter des faux positifs : ville dans l'adresse
    if (!looksLikeSameCity(lead, r.formatted_address || '')) return lead;

    // Si on a un téléphone sur le lead, on compare un suffixe de digits pour fiabiliser
    const leadPhone = normalizePhoneDigits(lead.telephone);
    const gPhone = normalizePhoneDigits(r.formatted_phone_number || r.international_phone_number);
    if (leadPhone && gPhone) {
      const suffixLen = Math.min(8, leadPhone.length, gPhone.length);
      if (suffixLen >= 6 && leadPhone.slice(-suffixLen) !== gPhone.slice(-suffixLen)) {
        return lead; // mismatch → on ne touche pas
      }
    }

    lead.google_place_id = r.place_id || placeId;
    if (r.rating != null) lead.google_rating = r.rating;
    if (r.user_ratings_total != null) lead.google_reviews_count = r.user_ratings_total;
    if (r.website && (allowWebsiteOverride || !lead.site_web)) lead.site_web = r.website;
    if ((r.formatted_phone_number || r.international_phone_number) && !lead.telephone) {
      lead.telephone = r.formatted_phone_number || r.international_phone_number;
    }

    // Log détaillé des enrichissements Google Maps
    const enrichissements = [];
    if (r.rating != null) enrichissements.push(`⭐ ${r.rating} étoiles`);
    if (r.user_ratings_total != null) enrichissements.push(`📝 ${r.user_ratings_total} avis`);
    if (r.website && (allowWebsiteOverride || !lead.site_web)) enrichissements.push(`🌐 site web: ${r.website}`);
    if ((r.formatted_phone_number || r.international_phone_number) && !lead.telephone) {
      enrichissements.push(`📞 téléphone: ${r.formatted_phone_number || r.international_phone_number}`);
    }

    logInfo(`🗺️ Google Maps enrichi: ${lead.nom_entreprise}`, {
      enrichissements: enrichissements.length > 0 ? enrichissements.join(', ') : 'aucun nouvel ajout',
      rating: lead.google_rating || null,
      reviews: lead.google_reviews_count || null,
      newPhone: !!((r.formatted_phone_number || r.international_phone_number) && !lead.telephone),
      newWebsite: !!(r.website && (allowWebsiteOverride || !lead.site_web))
    });
  } catch (err) {
    logWarning(`Google Maps enrichment error: ${err.message}`, { company: lead.nom_entreprise });
  }

  return lead;
}

module.exports = { 
  enrichWithGoogleMaps,
  isPoorLead
};

