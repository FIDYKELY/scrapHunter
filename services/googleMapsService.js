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

  const name = String(lead.nom_entreprise).trim();
  const hintParts = [];
  if (lead.adresse_complete) hintParts.push(String(lead.adresse_complete).trim());
  if (lead.ville) hintParts.push(String(lead.ville).trim());
  if (lead.departement) hintParts.push(String(lead.departement).trim());
  const query = [name, ...hintParts].filter(Boolean).join(' ');

  // URL directe de recherche (souvent plus stable et évite certaines pages intermédiaires)
  const url = `https://www.google.com/maps/search/${encodeURIComponent(query)}`;

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

    browser = await puppeteer.launch({
      headless: 'new',
      args: ['--no-sandbox', '--disable-setuid-sandbox']
    });
    
    const page = await browser.newPage();
    
    // User-agent aléatoire
    const ua = userAgents[Math.floor(Math.random() * userAgents.length)];
    await page.setUserAgent(ua);

    // Blocage des ressources inutiles pour accélérer
    await page.setRequestInterception(true);
    page.on('request', (req) => {
      const type = req.resourceType();
      if (['stylesheet', 'font', 'image'].includes(type)) {
        req.abort();
      } else {
        req.continue();
      }
    });

    const response = await page.goto(url, {
      waitUntil: 'networkidle2',
      timeout: 120000
    });

    logInfo(`📶 Google Maps HTTP Status: ${response?.status()} pour ${lead.nom_entreprise}`);

    // Gestion des cookies (version allemande comme dans votre exemple)
    try {
      await page.waitForSelector('button[aria-label="Alle ablehnen"], button[aria-label*="Accepter"], button:has-text("Tout accepter")', { 
        visible: true, 
        timeout: 10000 
      });
      
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

    await new Promise(r => setTimeout(r, 8000));

    // Vérification CAPTCHA
    const isCaptcha = await page.evaluate(() => {
      return !!document.querySelector('form[action*="sorry"]') || document.title.includes("unusual traffic");
    });

    if (isCaptcha) {
      logWarning("⚠️ CAPTCHA Google Maps détecté pour : " + lead.nom_entreprise);
      return lead;
    }

    // Extraction des données avec sélecteurs multiples et robustes
    const data = await page.evaluate(() => {
      const out = {
        placeUrl: window.location.href,
        ratingText: null,
        reviewsText: null,
        website: null,
        phone: null,
        address: null,
        title: null
      };

      // Utilisation des sélecteurs de votre exemple + alternatives
      const nodes = document.querySelectorAll('.Nv2PK, [data-result-index], [role="article"]');
      if (nodes.length > 0) {
        // On est sur une liste de résultats, on prend le premier
        const firstNode = nodes[0];
        
        // Titre - plusieurs sélecteurs possibles
        out.title = firstNode.querySelector('.qBF1Pd, .fontHeadlineSmall, [data-attrid="title"], h3')?.innerText?.trim() || null;
        
        // Adresse - sélecteurs multiples
        const addressSelectors = ['.W4Efsd span', '[data-dtype="d3gf"]', '[data-attrid="address"]', '.fontBodyMedium'];
        for (const selector of addressSelectors) {
          const addr = Array.from(firstNode.querySelectorAll(selector))
            .map(e => e.textContent)
            .find(t => t?.match(/\d{1,3}\s\w+/) || t?.includes('Rue') || t?.includes('Bd') || t?.includes('Avenue') || t?.includes('Saint'));
          if (addr) {
            out.address = addr;
            break;
          }
        }
        
        // Téléphone - sélecteurs multiples
        const phoneSelectors = ['.UsdlK', '[data-dtype="d3ph"]', '[data-attrid="phone"]', '[aria-label*="téléphone"]', '[aria-label*="phone"]'];
        for (const selector of phoneSelectors) {
          const phoneEl = firstNode.querySelector(selector);
          if (phoneEl) {
            const phoneText = phoneEl.innerText || phoneEl.getAttribute('aria-label');
            if (phoneText && phoneText.match(/[\d\s().-]{10,}/)) {
              out.phone = phoneText.trim();
              break;
            }
          }
        }
        
        // Site web - sélecteurs multiples
        const websiteSelectors = ['a[href*="http"]', '[data-attrid="website"]', '[data-dtype="d3cw"]'];
        for (const selector of websiteSelectors) {
          const websiteEl = firstNode.querySelector(selector);
          if (websiteEl) {
            const href = websiteEl.getAttribute('href');
            if (href && href.startsWith('http')) {
              out.website = href;
              break;
            }
          }
        }
        
        // Si on n'a pas trouvé de téléphone/site web, essayer sur toute la page
        if (!out.phone || !out.website) {
          const allText = document.body.innerText;
          
          // Recherche de téléphone dans tout le texte
          if (!out.phone) {
            const phoneMatch = allText.match(/(?:0\d{9}|[+]\d{11}|\d{2}\s\d{2}\s\d{2}\s\d{2}\s\d{2})/);
            if (phoneMatch) out.phone = phoneMatch[0];
          }
          
          // Recherche de site web dans tout le texte
          if (!out.website) {
            const websiteMatch = allText.match(/https?:\/\/[^\s\)]+/g);
            if (websiteMatch && websiteMatch.length > 0) {
              out.website = websiteMatch[0];
            }
          }
        }
      } else {
        // On est sur une page de détail - sélecteurs plus larges
        const ratingEl = Array.from(document.querySelectorAll('[aria-label]'))
          .find(el => (el.getAttribute('aria-label') || '').toLowerCase().includes('étoile'));
        if (ratingEl) out.ratingText = ratingEl.getAttribute('aria-label');

        const reviewsEl = Array.from(document.querySelectorAll('[aria-label]'))
          .find(el => (el.getAttribute('aria-label') || '').toLowerCase().includes('avis'));
        if (reviewsEl) out.reviewsText = reviewsEl.getAttribute('aria-label');

        // Site web - sélecteurs multiples
        const websiteSelectors = ['a[data-attrid="website"]', '[data-dtype="d3cw"]', 'a[href*="http"]:not([href*="google"])'];
        for (const selector of websiteSelectors) {
          const websiteEl = document.querySelector(selector);
          if (websiteEl) {
            const href = websiteEl.getAttribute('href');
            if (href && href.startsWith('http') && !href.includes('google')) {
              out.website = href;
              break;
            }
          }
        }
        
        // Téléphone - sélecteurs multiples
        const phoneSelectors = ['[data-dtype="d3ph"]', '[data-attrid="phone"]', '[aria-label*="téléphone"]', '[aria-label*="phone"]'];
        for (const selector of phoneSelectors) {
          const phoneEl = document.querySelector(selector);
          if (phoneEl) {
            const phoneText = phoneEl.innerText || phoneEl.getAttribute('aria-label');
            if (phoneText && phoneText.match(/[\d\s().-]{10,}/)) {
              out.phone = phoneText.trim();
              break;
            }
          }
        }
        
        // Adresse - sélecteurs multiples
        const addressSelectors = ['[data-dtype="d3gf"]', '[data-attrid="address"]', '[aria-label*="adresse"]'];
        for (const selector of addressSelectors) {
          const addrEl = document.querySelector(selector);
          if (addrEl) {
            const addrText = addrEl.innerText || addrEl.getAttribute('aria-label');
            if (addrText && addrText.length > 10) {
              out.address = addrText.trim();
              break;
            }
          }
        }
      }

      return out;
    });

    // Traitement des données trouvées
    if (data.phone) {
      // Ajouter le téléphone si pas existant OU si le nouveau est meilleur (plus long)
      if (!lead.telephone || (data.phone.length > lead.telephone.length && data.phone.match(/\d/))) {
        lead.telephone = data.phone;
      }
    }
    
    // Ne PAS utiliser les URLs Google Maps comme sites web
    if (data.website && !data.website.includes('google.com/maps') && (allowWebsiteOverride || !lead.site_web)) {
      lead.site_web = data.website;
    }

    // Log détaillé des enrichissements avec debug
    const enrichissements = [];
    
    // Vérifier si le téléphone a été ajouté/mis à jour
    if (data.phone && (!lead.telephone || data.phone !== lead.telephone)) {
      enrichissements.push(`📞 téléphone: ${data.phone}`);
    }
    
    // Vérifier si le site web a été ajouté (non-Google Maps)
    if (data.website && !data.website.includes('google.com/maps') && (allowWebsiteOverride || !lead.site_web)) {
      enrichissements.push(`🌐 site web: ${data.website}`);
    }

    // Log de debug complet pour analyser ce qui est trouvé
    logInfo(`🗺️ Google Maps (web) enrichi: ${lead.nom_entreprise}`, {
      enrichissements: enrichissements.length > 0 ? enrichissements.join(', ') : 'aucun nouvel ajout',
      newPhone: !!(data.phone && (!lead.telephone || data.phone !== lead.telephone)),
      newWebsite: !!(data.website && !data.website.includes('google.com/maps') && (allowWebsiteOverride || !lead.site_web)),
      foundData: {
        phone: !!data.phone,
        website: !!data.website && !data.website.includes('google.com/maps'),
        address: !!data.address,
        title: !!data.title
      },
      debugData: {
        phoneFound: data.phone || null,
        websiteFound: data.website || null,
        addressFound: data.address || null,
        titleFound: data.title || null,
        existingPhone: lead.telephone || null,
        existingWebsite: lead.site_web || null,
        phoneUpdated: data.phone !== lead.telephone,
        websiteIgnored: data.website?.includes('google.com/maps') || false
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

