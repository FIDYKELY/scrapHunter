const fetch = require('node-fetch');
const puppeteer = require('puppeteer');
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
  // Un lead est considéré comme "pauvre" si :
  // 1. Score global bas (< 30) OU pas de score du tout
  // 2. Qualité de données LOW
  // 3. Manque d'informations essentielles (téléphone, email, site web)
  const hasLowScore = (lead.score_global != null && lead.score_global < 30) || lead.score_global == null;
  const hasLowDataQuality = lead.data_quality === 'LOW';
  const missingEssential = (!lead.telephone && !lead.email && !lead.site_web);
  
  return hasLowScore || hasLowDataQuality || missingEssential;
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
        await page.waitForTimeout(1000);
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

  let browser;
  try {
    await GMAPS_RATE_LIMITER.acquire('google-maps-web');

    browser = await puppeteer.launch({
      headless: 'new',
      args: ['--no-sandbox', '--disable-setuid-sandbox']
    });
    const page = await browser.newPage();
    await page.setUserAgent('Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36');

    await page.goto(url, { waitUntil: 'networkidle2', timeout: 120000 });
    await tryAcceptConsent(page);

    // Attendre soit un panneau de place, soit une liste de résultats
    await page.waitForTimeout(2000);

    // Si liste de résultats, cliquer le premier résultat visible
    const clickedFirst = await page.evaluate(() => {
      const candidates = Array.from(document.querySelectorAll('a[href*="/maps/place/"], a[href*="/maps?"]'));
      const first = candidates.find(a => a instanceof HTMLAnchorElement && a.offsetParent !== null);
      if (first) {
        (first).click();
        return true;
      }
      return false;
    }).catch(() => false);

    if (clickedFirst) await page.waitForTimeout(2500);

    const data = await page.evaluate(() => {
      const out = {
        placeUrl: window.location.href,
        ratingText: null,
        reviewsText: null,
        website: null,
        phone: null,
        address: null
      };

      // Rating + reviews: souvent dans des éléments avec aria-label contenant "étoiles" / "avis"
      const ratingEl = Array.from(document.querySelectorAll('[aria-label]'))
        .find(el => (el.getAttribute('aria-label') || '').toLowerCase().includes('étoile'));
      if (ratingEl) out.ratingText = ratingEl.getAttribute('aria-label');

      const reviewsEl = Array.from(document.querySelectorAll('[aria-label]'))
        .find(el => {
          const a = (el.getAttribute('aria-label') || '').toLowerCase();
          return a.includes('avis') && /\d/.test(a);
        });
      if (reviewsEl) out.reviewsText = reviewsEl.getAttribute('aria-label');

      // Website: bouton "Site Web"
      const websiteBtn = Array.from(document.querySelectorAll('a[aria-label], button[aria-label]'))
        .find(el => (el.getAttribute('aria-label') || '').toLowerCase().includes('site'));
      if (websiteBtn) {
        const href = websiteBtn instanceof HTMLAnchorElement ? websiteBtn.href : null;
        if (href && href.startsWith('http')) out.website = href;
      }

      // Phone: aria-label contient "Téléphone"
      const phoneEl = Array.from(document.querySelectorAll('[aria-label]'))
        .find(el => (el.getAttribute('aria-label') || '').toLowerCase().includes('téléphone'));
      if (phoneEl) out.phone = phoneEl.getAttribute('aria-label');

      // Address: aria-label contient "Adresse"
      const addrEl = Array.from(document.querySelectorAll('[aria-label]'))
        .find(el => (el.getAttribute('aria-label') || '').toLowerCase().includes('adresse'));
      if (addrEl) out.address = addrEl.getAttribute('aria-label');

      return out;
    });

    // Heuristique ville
    if (data.address && !looksLikeSameCity(lead, data.address)) return lead;

    // Phone match heuristique
    const leadPhone = normalizePhoneDigits(lead.telephone);
    const gPhone = normalizePhoneDigits(data.phone);
    if (leadPhone && gPhone) {
      const suffixLen = Math.min(8, leadPhone.length, gPhone.length);
      if (suffixLen >= 6 && leadPhone.slice(-suffixLen) !== gPhone.slice(-suffixLen)) {
        return lead;
      }
    }

    // Tenter d'extraire un pseudo place id depuis l'URL (pas garanti)
    lead.google_place_id = lead.google_place_id || null;

    const rating = parseFrenchNumber(data.ratingText);
    if (rating != null) lead.google_rating = rating;

    const reviews = parseFrenchNumber(data.reviewsText);
    if (reviews != null) lead.google_reviews_count = Math.trunc(reviews);

    if (data.website && (allowWebsiteOverride || !lead.site_web)) {
      lead.site_web = data.website;
    }
    if (data.phone && !lead.telephone) {
      // Ex: "Téléphone : 01 23 45 67 89"
      const m = String(data.phone).match(/(\+?\d[\d\s().-]{6,})/);
      lead.telephone = m ? m[1].trim() : data.phone;
    }

    logInfo(`🗺️ Google Maps (web) enrichi: ${lead.nom_entreprise}`, {
      rating: lead.google_rating || null,
      reviews: lead.google_reviews_count || null
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
  
  logInfo(`🎯 Google Maps enrichment pour lead pauvre: ${lead.nom_entreprise}`, {
    score: lead.score_global,
    dataQuality: lead.data_quality,
    hasPhone: !!lead.telephone,
    hasEmail: !!lead.email,
    hasWebsite: !!lead.site_web
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

    logInfo(`🗺️ Google Maps enrichi: ${lead.nom_entreprise}`, {
      rating: lead.google_rating || null,
      reviews: lead.google_reviews_count || null
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

