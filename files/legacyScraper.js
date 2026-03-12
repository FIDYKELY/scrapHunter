// legacyScraper.js
// Version consolidée — intègre toute la logique de :
//   1-scraping-openstreetmap.js, 1-scraping-pagesjaunes.js,
//   2-enrichissement-site.js, 3-enrichissement-reseaux.js,
//   4-scoring.js, realtime-lead-processor.js, index.js
// Mode sans BDD : scraping réel -> enrichissement -> scoring -> envoi n8n

const { v4: uuidv4 } = require('uuid');
const puppeteer = require('puppeteer');
const fetch = require('node-fetch');
const fs = require('fs');
const path = require('path');

// --- CANCELLATION STATE ---
const cancelledBatches = new Set();
// --------------------------

/**
 * Attente capable d'être interrompue
 * Retourne true si annulé, false sinon
 */
async function cancellableSleep(ms, crawlBatchId) {
  if (!crawlBatchId) {
    await new Promise(r => setTimeout(r, ms));
    return false;
  }
  const start = Date.now();
  while (Date.now() - start < ms) {
    if (cancelledBatches.has(crawlBatchId)) return true;
    const remaining = ms - (Date.now() - start);
    await new Promise(r => setTimeout(r, Math.min(1000, remaining)));
  }
  return cancelledBatches.has(crawlBatchId);
}

// ─────────────────────────────────────────────
// LOGGER
// ─────────────────────────────────────────────
const { logInfo, logWarning, logError } = require('../utils/logger');
const { checkCompanyStatus } = require('./societeService');

// ─────────────────────────────────────────────
// RATE LIMITER
// ─────────────────────────────────────────────
class RateLimiter {
  constructor(options = {}) {
    this.maxPerWindow = options.maxPerWindow || 50;
    this.windowMs = options.windowMs || 60000;
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

// ─────────────────────────────────────────────
// CONFIGURATION
// ─────────────────────────────────────────────
const N8N_WEBHOOK_URL = process.env.N8N_WEBHOOK_URL || 'https://n8n.trouvezpourmoi.com/webhook/leads';
const N8N_RATE_LIMITER = new RateLimiter({ maxPerWindow: 50, windowMs: 60 * 1000 });
const OSM_RATE_LIMITER = new RateLimiter({ maxPerWindow: 5, windowMs: 60 * 1000 });
const PJ_RATE_LIMITER = new RateLimiter({ maxPerWindow: 15, windowMs: 60 * 60 * 1000 });
const SOCIAL_RATE_LIMITER = new RateLimiter({ maxPerWindow: 30, windowMs: 60 * 1000 });

// Per-domain limiters map (enrichissement site web)
const domainLimiters = new Map();
function getDomainLimiter(domain) {
  if (!domainLimiters.has(domain)) {
    domainLimiters.set(domain, new RateLimiter({ maxPerWindow: 30, windowMs: 60 * 1000 }));
  }
  return domainLimiters.get(domain);
}

// ─────────────────────────────────────────────
// CONSTANTES OSM / PAGESJAUNES
// ─────────────────────────────────────────────
const OVERPASS_SERVERS = [
  'https://overpass-api.de/api/interpreter',
  'https://overpass.kumi.systems/api/interpreter',
  'https://overpass.nchc.org.tw/api/interpreter'
];

const OSM_TAGS = [
  'office=real_estate',
  'shop=estate_agent',
  'office=property_management',
  'real_estate=agency',
  'real_estate=agent',
  'office=estate_agent'
];

const REAL_ESTATE_KEYWORDS = [
  'immobilier', 'immobilière', 'agence immobili', 'real estate',
  'property', 'estate agent', 'gestion immobili', 'syndic',
  'location', 'location appartement', 'location maison'
];

const DEPARTEMENTS_FRANCE = [
  '01', '02', '03', '04', '05', '06', '07', '08', '09', '10',
  '11', '12', '13', '14', '15', '16', '17', '18', '19', '2A', '2B',
  '21', '22', '23', '24', '25', '26', '27', '28', '29', '30',
  '31', '32', '33', '34', '35', '36', '37', '38', '39', '40',
  '41', '42', '43', '44', '45', '46', '47', '48', '49', '50',
  '51', '52', '53', '54', '55', '56', '57', '58', '59', '60',
  '61', '62', '63', '64', '65', '66', '67', '68', '69', '70',
  '71', '72', '73', '74', '75', '76', '77', '78', '79', '80',
  '81', '82', '83', '84', '85', '86', '87', '88', '89', '90',
  '91', '92', '93', '94', '95', '971', '972', '973', '974', '976'
];

// Mapping département → ville (format PagesJaunes, minuscules)
const DEPT_VILLE_MAPPING = {
  '01': 'bourg-en-bresse', '02': 'laon', '03': 'moulins', '04': 'digne', '05': 'gap',
  '06': 'nice', '07': 'le-puy-en-velay', '08': 'charleville-mezieres', '09': 'foix', '10': 'troyes',
  '11': 'carcassonne', '12': 'rodez', '13': 'marseille', '14': 'caen', '15': 'aurillac',
  '16': 'angouleme', '17': 'la-rochelle', '18': 'bourges', '19': 'tulle', '2A': 'ajaccio',
  '2B': 'bastia', '21': 'dijon', '22': 'saint-brieuc', '23': 'gueret', '24': 'perigueux',
  '25': 'besancon', '26': 'valence', '27': 'evreux', '28': 'chartres', '29': 'quimper',
  '30': 'nimes', '31': 'toulouse', '32': 'auch', '33': 'bordeaux', '34': 'montpellier',
  '35': 'rennes', '36': 'chateauroux', '37': 'tours', '38': 'grenoble', '39': 'lons-le-saunier',
  '40': 'mont-de-marsan', '41': 'blois', '42': 'saint-etienne', '43': 'le-puy-en-velay', '44': 'nantes',
  '45': 'orleans', '46': 'cahors', '47': 'agen', '48': 'mende', '49': 'angers', '50': 'coutances',
  '51': 'chalon-en-champagne', '52': 'chaumont', '53': 'laval', '54': 'nancy', '55': 'bar-le-duc',
  '56': 'vannes', '57': 'metz', '58': 'nevers', '59': 'lille', '60': 'beauvais',
  '61': 'alencon', '62': 'lens', '63': 'clermont-ferrand', '64': 'pau', '65': 'tarbes',
  '66': 'perpignan', '67': 'strasbourg', '68': 'colmar', '69': 'lyon', '70': 'vesoul',
  '71': 'macon', '72': 'le-mans', '73': 'chambery', '74': 'annecy', '75': 'paris',
  '76': 'rouen', '77': 'melun', '78': 'versailles', '79': 'niort', '80': 'amiens',
  '81': 'albi', '82': 'montauban', '83': 'toulon', '84': 'avignon', '85': 'la-roche-sur-yon',
  '86': 'poitiers', '87': 'limoges', '88': 'epinal', '89': 'auxerre', '90': 'belfort',
  '91': 'evry', '92': 'nanterre', '93': 'bobigny', '94': 'creteil', '95': 'cergy-pontoise',
  '971': 'pointe-a-pitre', '972': 'fort-de-france', '973': 'cayenne', '974': 'saint-denis', '976': 'mamoudzou'
};

// Bounding boxes des départements (format: sud,ouest,nord,est)
const DEPT_BBOX = {
  '75': '48.815,2.224,48.902,2.470', '92': '48.755,2.145,48.945,2.305',
  '93': '48.815,2.315,49.015,2.575', '94': '48.725,2.325,48.895,2.515',
  '95': '48.795,1.875,49.175,2.485', '77': '48.345,2.415,48.985,3.215',
  '78': '48.595,1.785,48.995,2.245', '91': '48.425,1.985,48.735,2.555',
  '69': '45.715,4.685,45.815,4.915', '13': '43.175,5.215,43.375,5.525',
  '31': '43.515,1.325,43.665,1.555', '33': '44.765,-0.685,44.925,-0.465',
  '59': '50.575,2.885,50.755,3.175', '06': '43.615,7.105,43.775,7.335',
  '34': '43.395,3.045,43.735,3.925', '44': '47.125,-2.415,47.475,-1.345',
  '35': '47.985,-2.245,48.715,-1.425', '38': '45.015,5.425,45.945,6.025',
  '57': '48.845,6.045,49.535,7.435', '67': '48.445,7.345,49.125,8.235',
  '68': '47.445,6.845,48.275,7.525', '83': '43.025,5.885,43.425,6.415',
  '30': '43.475,2.985,44.305,4.635', '29': '47.625,-4.795,48.535,-2.885',
  '22': '48.285,-3.245,48.765,-1.945', '56': '47.375,-3.445,47.965,-2.025',
  '85': '46.265,-1.845,46.945,-0.875', '49': '47.025,-0.965,47.845,0.285',
  '72': '47.945,0.045,48.525,0.945', '71': '46.265,3.545,47.145,5.365',
  '42': '45.415,3.825,46.045,4.945', '73': '45.025,5.645,45.845,6.825',
  '74': '45.795,5.845,46.225,6.925', '01': '45.765,4.745,46.345,5.825',
  '02': '48.845,2.985,49.925,4.245', '03': '46.025,2.645,46.825,3.625',
  '04': '43.845,5.725,44.425,6.545', '05': '44.425,5.845,45.345,6.825',
  '07': '44.425,3.845,45.245,4.825', '08': '49.245,3.845,50.125,5.425',
  '09': '42.725,1.445,43.245,2.825', '10': '47.845,3.845,48.625,4.825',
  '11': '42.945,1.845,43.425,3.025', '12': '44.025,1.845,44.825,3.025',
  '14': '48.845,-0.945,49.425,0.425', '15': '44.725,1.845,45.425,3.025',
  '16': '45.025,-0.445,45.825,0.425', '17': '45.845,-1.245,46.245,0.425',
  '18': '46.845,1.845,47.625,3.025', '19': '45.025,1.445,45.825,2.825',
  '2A': '41.725,8.445,42.625,9.425', '2B': '42.245,8.845,43.025,9.825',
  '21': '47.025,3.845,47.825,5.025', '23': '45.845,1.445,46.425,2.825',
  '24': '44.725,0.445,45.425,1.825', '25': '46.845,5.845,47.625,7.025',
  '26': '44.425,4.445,45.245,5.825', '27': '48.425,0.445,49.225,1.825',
  '28': '48.025,0.845,48.825,2.025', '32': '42.845,0.045,43.625,1.425',
  '36': '46.025,0.845,46.825,2.025', '37': '46.845,0.045,47.625,1.425',
  '39': '46.025,4.845,46.825,6.025', '40': '43.425,-1.445,44.225,0.045',
  '41': '47.425,0.845,48.225,2.025', '43': '44.845,2.845,45.625,4.025',
  '45': '47.425,1.845,48.225,3.025', '46': '44.025,1.445,44.825,2.825',
  '47': '44.025,-0.445,44.825,1.425', '48': '44.025,2.845,44.825,4.025',
  '50': '48.845,-1.945,49.625,-0.845', '51': '48.425,3.445,49.225,5.025',
  '52': '47.425,4.845,48.225,6.025', '53': '47.425,-0.945,48.225,0.425',
  '54': '48.425,5.445,49.225,7.025', '55': '48.425,4.845,49.225,6.025',
  '58': '46.845,2.845,47.625,4.025', '60': '49.025,1.845,49.825,3.025',
  '61': '48.425,-0.445,49.225,1.425', '62': '50.425,1.445,51.225,4.025',
  '63': '45.425,2.445,46.225,3.825', '64': '42.845,-1.445,43.625,0.425',
  '65': '42.845,0.045,43.625,1.425', '66': '42.425,1.845,43.225,3.025',
  '70': '47.425,5.445,48.225,6.625', '76': '49.025,0.445,49.825,1.825',
  '79': '46.425,-0.445,47.225,0.425', '80': '49.425,1.845,50.225,3.025',
  '81': '43.425,1.445,44.225,2.825', '82': '43.845,0.845,44.625,2.025',
  '84': '43.625,4.445,44.425,5.825', '86': '46.025,0.045,46.825,1.425',
  '87': '45.425,0.845,46.225,2.025', '88': '48.025,5.445,48.825,6.625',
  '89': '47.425,2.845,48.225,4.025', '90': '47.425,6.445,48.225,7.625',
  '971': '15.845,-61.845,16.445,-60.845', '972': '14.345,-61.245,14.845,-60.445',
  '973': '3.845,-54.445,5.845,-51.445', '974': '20.845,55.245,21.245,55.845',
  '976': '12.645,45.045,12.945,45.445'
};

// Emails patterns
const EMAIL_PATTERNS = [/[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}/g];

// ─────────────────────────────────────────────
// NORMALISATION
// ─────────────────────────────────────────────
function normalizePhone(phone) {
  if (!phone) return '';
  return phone.replace(/[^\d+]/g, '');
}

function formatPhoneForSheets(phone) {
  if (!phone) return null;
  let cleaned = phone.replace(/[^\d+]/g, '');
  if (cleaned.startsWith('33') && !cleaned.startsWith('+33')) cleaned = '+' + cleaned;
  if (cleaned.startsWith('0') && cleaned.length === 10) cleaned = '+33' + cleaned.substring(1);
  return cleaned;
}

function normalizeEmail(email) {
  if (!email) return null;
  return email.toLowerCase().trim();
}

function normalizeDomain(website) {
  if (!website) return '';
  try {
    const url = new URL(website.startsWith('http') ? website : 'https://' + website);
    return url.hostname.toLowerCase().replace('www.', '');
  } catch { return ''; }
}

function normalizeNameCity(name, address) {
  if (!name && !address) return '';
  return ((name || '') + ' ' + (address || '')).toLowerCase().trim();
}

function extractDomain(website) {
  return normalizeDomain(website);
}

// ─────────────────────────────────────────────
// 4-SCORING.JS — logique de scoring
// ─────────────────────────────────────────────
function isDirectEmail(email) {
  if (!email) return false;
  const lower = email.toLowerCase();
  const generic = ['contact@', 'info@', 'hello@', 'agency@', 'service@'];
  return !generic.some(p => lower.startsWith(p));
}

function isGenericEmail(email) {
  if (!email) return false;
  const lower = email.toLowerCase();
  const generic = ['contact@', 'info@', 'hello@', 'agency@', 'service@'];
  return generic.some(p => lower.startsWith(p));
}

function isMobilePhone(phone) {
  if (!phone) return false;
  const digits = phone.replace(/[^\d]/g, '');
  return /^0[67]\d{8}$/.test(digits) ||
    /^(\+33|0033)[67]\d{8}$/.test(phone.replace(/[^\d+]/g, ''));
}

function calculateScore(lead) {
  let score = 0;
  const reasons = [];

  // 1. Profil cible (30 pts max)
  if (lead.type_profil === 'INDEPENDANT') {
    score += 20; reasons.push('Indépendant');
  } else if (lead.type_profil === 'MULTI_AGENCE') {
    score += 15; reasons.push('Multi-agence');
  } else if (lead.type_profil === 'AGENCE_SIMPLE' || lead.type_profil === 'AGENCE') {
    score += 5; reasons.push('Agence');
  }

  // 2. Email (25 pts max)
  if (lead.email && isDirectEmail(lead.email)) {
    score += 25; reasons.push('Email direct');
  } else if (lead.email && isGenericEmail(lead.email)) {
    score += 15; reasons.push('Email générique');
  } else if (lead.url_contact_form) {
    score += 5; reasons.push('Formulaire contact');
  }

  // 3. LinkedIn (15 pts)
  if (lead.linkedin_company_url) { score += 15; reasons.push('LinkedIn entreprise'); }

  // 4. Téléphone (10 pts max)
  if (lead.telephone) {
    score += 5; reasons.push('Téléphone fixe');
    if (isMobilePhone(lead.telephone)) { score += 5; reasons.push('Portable'); }
  }

  // 5. Réseaux sociaux (10 pts max)
  if (lead.facebook_url) { score += 5; reasons.push('Facebook'); }
  if (lead.instagram_url) { score += 5; reasons.push('Instagram'); }

  // 6. Site web (5 pts)
  if (lead.site_web) { score += 5; reasons.push('Site web'); }

  // 7. Google Business (5 pts)
  if (lead.google_place_id) { score += 5; reasons.push('Fiche Google'); }

  const priorite = score >= 70 ? 'A' : score >= 50 ? 'B' : score >= 30 ? 'C' : 'D';

  return {
    score_global: Math.min(score, 100),
    priorite,
    reason: reasons.join(' + ')
  };
}

function withUpdatedScore(lead) {
  const scoring = calculateScore(lead);
  return { ...lead, ...scoring };
}

// ─────────────────────────────────────────────
// OSM — helpers
// ─────────────────────────────────────────────
function getDepartmentBbox(dept) {
  if (DEPT_BBOX[dept]) return DEPT_BBOX[dept];
  return '41.0,-5.0,51.0,10.0'; // France métropolitaine par défaut
}

function buildOverpassQuery(bbox) {
  const keywordsPattern = REAL_ESTATE_KEYWORDS.join('|');
  return `
    [out:json][timeout:600];
    (
      node["name"~"${keywordsPattern}", i](${bbox});
      way["name"~"${keywordsPattern}", i](${bbox});
      relation["name"~"${keywordsPattern}", i](${bbox});
    );
    out body;
    >;
    out skel qt;
  `;
}

function splitDepartmentBbox(bbox, maxSplits = 4) {
  const [south, west, north, east] = bbox.split(',').map(parseFloat);
  const latRange = north - south;
  const lngRange = east - west;
  const bboxes = [];
  const latSplits = Math.min(Math.ceil(Math.sqrt(maxSplits)), 2);
  const lngSplits = Math.min(Math.ceil(maxSplits / latSplits), 2);
  for (let i = 0; i < latSplits; i++) {
    for (let j = 0; j < lngSplits; j++) {
      bboxes.push(
        `${south + latRange * i / latSplits},${west + lngRange * j / lngSplits},` +
        `${south + latRange * (i + 1) / latSplits},${west + lngRange * (j + 1) / lngSplits}`
      );
    }
  }
  return bboxes;
}

async function executeOverpassQuery(query, serverIndex = 0) {
  const server = OVERPASS_SERVERS[serverIndex];
  await OSM_RATE_LIMITER.acquire('overpass-api');

  const controller = new AbortController();
  const timeoutId = setTimeout(() => controller.abort(), 120000);

  try {
    const response = await fetch(server, {
      method: 'POST',
      headers: { 'Content-Type': 'text/plain', 'User-Agent': 'RealEstateScraper/1.0' },
      body: query,
      signal: controller.signal
    });
    clearTimeout(timeoutId);

    // Vérifier le Content-Type avant de parser en JSON
    // Certains serveurs renvoient du XML ou HTML en cas d'erreur même avec status 200
    const contentType = response.headers.get('content-type') || '';
    if (contentType.includes('xml') || contentType.includes('html')) {
      const bodyText = await response.text();
      throw new Error(`Overpass returned non-JSON response (${contentType.split(';')[0].trim()}): ${bodyText.substring(0, 100)}`);
    }

    if (!response.ok) throw new Error(`Overpass API error: ${response.status} ${response.statusText}`);
    const data = await response.json();
    if (data.remark && data.remark.includes('runtime error')) throw new Error('Query too large, need to split');
    return data;
  } catch (error) {
    clearTimeout(timeoutId);
    // Tentative sur le serveur suivant pour: timeout, erreurs réseau, erreurs HTTP, XML/HTML retourné
    const shouldFallback = serverIndex < OVERPASS_SERVERS.length - 1 && (
      error.name === 'AbortError' ||
      error.message.includes('504') ||
      error.message.includes('429') ||
      error.message.includes('503') ||
      error.message.includes('timeout') ||
      error.message.includes('non-JSON') ||
      error.message.includes('xml') ||
      error.message.includes('Overpass API error') ||
      error.message.includes('failed')
    );
    if (shouldFallback) {
      logWarning(`Changement serveur Overpass: ${server} -> ${OVERPASS_SERVERS[serverIndex + 1]} (raison: ${error.message.substring(0, 80)})`);
      return executeOverpassQuery(query, serverIndex + 1);
    }
    throw error;
  }
}

function buildAddressFromTags(tags) {
  const parts = [];
  if (tags['addr:housenumber']) parts.push(tags['addr:housenumber']);
  if (tags['addr:street']) parts.push(tags['addr:street']);
  if (tags['addr:postcode']) parts.push(tags['addr:postcode']);
  if (tags['addr:city']) parts.push(tags['addr:city']);
  else if (tags['addr:place']) parts.push(tags['addr:place']);
  return parts.join(', ');
}

function buildLeadFromOsmResult(element, dept, crawlBatchId) {
  const tags = element.tags || {};
  let lat = null, lng = null;
  if (element.lat && element.lon) { lat = element.lat; lng = element.lon; }
  else if (element.center) { lat = element.center.lat; lng = element.center.lon; }

  const nom_entreprise = tags.name || tags.operator || tags.brand || 'Agence immobilière';
  const telephone = formatPhoneForSheets(tags.phone || tags['contact:phone'] || null);
  const site_web = tags.website || tags['contact:website'] || null;
  const email = normalizeEmail(tags.email || tags['contact:email'] || null);
  const facebook_url = tags['contact:facebook'] || tags.facebook || null;
  const instagram_url = tags['contact:instagram'] || tags.instagram || null;
  const linkedin_company_url = tags['contact:linkedin'] || tags.linkedin || null;
  const adresse_complete = buildAddressFromTags(tags);
  const code_postal = tags['addr:postcode'] || null;
  const ville = tags['addr:city'] || tags['addr:place'] || null;
  const source_url = `https://www.openstreetmap.org/${element.type}/${element.id}`;
  const phone_norm = normalizePhone(tags.phone || tags['contact:phone'] || '');
  const domain_norm = site_web ? normalizeDomain(site_web) : '';
  const name_city_norm = normalizeNameCity(nom_entreprise, adresse_complete);

  let type_profil = 'AGENCE';
  const name = (tags.name || '').toLowerCase();
  const operator = (tags.operator || '').toLowerCase();
  if (name.includes('orpi') || name.includes('century 21') || name.includes('laforêt') ||
    name.includes('guy hoquet') || name.includes('era') || name.includes('fnaim') ||
    operator.includes('orpi') || operator.includes('century 21')) {
    type_profil = 'AGENCE_RESEAU';
  }
  if (name.includes('indépendant') || name.includes('independant') ||
    operator.includes('indépendant') || operator.includes('independant')) {
    type_profil = 'INDEPENDANT';
  }

  const missing = [];
  if (!tags.phone && !tags['contact:phone']) missing.push('telephone');
  if (!email) missing.push('email');
  if (!site_web) missing.push('site_web');

  let data_quality = 'LOW';
  if ((tags.phone || tags['contact:phone']) && (email || site_web)) data_quality = 'HIGH';
  else if (tags.phone || tags['contact:phone'] || email || site_web) data_quality = 'MEDIUM';

  return {
    lead_id: uuidv4(),
    source: 'openstreetmap',
    source_url,
    type_profil,
    nom_entreprise,
    date_import: new Date().toISOString(),
    crawl_batch_id: crawlBatchId,
    adresse_complete, code_postal, ville, departement: dept, lat, lng,
    telephone, email, site_web,
    url_contact_page: null, url_contact_form: null,
    linkedin_company_url, facebook_url, instagram_url,
    google_place_id: null, google_rating: null, google_reviews_count: null,
    phone_norm, domain_norm, name_city_norm,
    is_duplicate: false, duplicate_of_lead_id: null,
    data_quality, missing,
    score_global: null, priorite: null, reason: null,
    status: 'NEW', assigned_to: null, last_action_date: null,
    notes: tags.shop ? `OSM Tags: ${tags.shop}` : null
  };
}

/**
 * Scrape un département OSM avec retry et gestion des zones
 */
async function scrapeDepartmentOSM(dept, crawlBatchId) {
  let retryCount = 0;
  const maxRetries = 3;

  while (retryCount < maxRetries) {
    try {
      const bbox = getDepartmentBbox(dept);
      let bboxes = [bbox];

      if (['13', '69', '59', '75', '92', '93', '94'].includes(dept)) {
        bboxes = splitDepartmentBbox(bbox, 4);
        logInfo(`Département ${dept} divisé en ${bboxes.length} zones`);
      }

      let totalElements = [];

      for (let i = 0; i < bboxes.length; i++) {
        const zoneBbox = bboxes[i];
        let zoneRetry = 0;
        while (zoneRetry < 2) {
          try {
            logInfo(`Requête Overpass dept=${dept} zone ${i + 1}/${bboxes.length}`);
            const query = buildOverpassQuery(zoneBbox);
            const data = await executeOverpassQuery(query);
            if (data.elements && data.elements.length > 0) {
              totalElements = totalElements.concat(data.elements);
              logInfo(`Zone ${i + 1}/${bboxes.length}: ${data.elements.length} éléments`);
            }
            break;
          } catch (err) {
            zoneRetry++;
            logWarning(`Erreur zone ${i + 1} (tentative ${zoneRetry}/2): ${err.message}`);
            if (zoneRetry >= 2) {
              try {
                const data = await executeOverpassQuery(buildOverpassQuery(zoneBbox));
                if (data.elements) totalElements = totalElements.concat(data.elements);
              } catch (e2) { logError(`Échec complet zone ${i + 1}: ${e2.message}`); }
            } else {
              await new Promise(r => setTimeout(r, 10000 * zoneRetry));
            }
          }
        }
        if (cancelledBatches.has(crawlBatchId)) break;
        if (i < bboxes.length - 1) {
          if (await cancellableSleep(15000, crawlBatchId)) break;
        }
      }

      if (cancelledBatches.has(crawlBatchId)) {
        logWarning(`🛑 Scraping OSM pour le département ${dept} annulé.`);
        return [];
      }

      const leads = [];
      for (const element of totalElements) {
        try {
          const lead = buildLeadFromOsmResult(element, dept, crawlBatchId);
          leads.push(lead);
          logInfo(`✅ Lead OSM: ${lead.nom_entreprise}`, {
            telephone: lead.telephone ? '✓' : '✗',
            email: lead.email ? '✓' : '✗',
            site: lead.site_web ? '✓' : '✗'
          });
        } catch (err) { logError(`Erreur traitement élément OSM: ${err.message}`); }
      }

      logInfo(`Département ${dept} OSM terminé`, { elements: totalElements.length, leads: leads.length });
      return leads;

    } catch (error) {
      retryCount++;
      logError(`Erreur dept ${dept} OSM (tentative ${retryCount}/${maxRetries}): ${error.message}`);
      if (retryCount >= maxRetries) return [];
      await cancellableSleep(30000 * retryCount, crawlBatchId);
    }
  }
  return [];
}

/**
 * Scrape OSM pour une liste de départements
 */
async function scrapeOpenStreetMap(departments = DEPARTEMENTS_FRANCE, crawlBatchId) {
  logInfo('Début scraping OpenStreetMap', { depts: departments });
  const allLeads = [];
  for (let i = 0; i < departments.length; i++) {
    if (cancelledBatches.has(crawlBatchId)) {
      logWarning(`🛑 Scraping OpenStreetMap annulé pour le batch ${crawlBatchId}`);
      break;
    }
    const dept = departments[i];
    const leads = await scrapeDepartmentOSM(dept, crawlBatchId);
    allLeads.push(...leads);
    if (i < departments.length - 1) {
      const delay = 30000 + Math.random() * 30000;
      logInfo(`Pause entre départements: ${Math.round(delay)}ms`);
      if (await cancellableSleep(delay, crawlBatchId)) break;
    }
  }
  logInfo(`Scraping OSM terminé: ${allLeads.length} leads`);
  return allLeads;
}

// ─────────────────────────────────────────────
// PAGESJAUNES — détail d'agence
// ─────────────────────────────────────────────
async function scrapeAgencyDetail(url, crawlBatchId) {
  if (crawlBatchId && cancelledBatches.has(crawlBatchId)) return null;

  const browser = await puppeteer.launch({
    headless: 'new',
    args: ['--no-sandbox', '--disable-setuid-sandbox']
  });

  try {
    const page = await browser.newPage();
    await page.setUserAgent('Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36');

    try {
      await page.goto(url, { waitUntil: 'networkidle2', timeout: 60000 });
    } catch (err) {
      logWarning(`Timeout networkidle2 pour ${url}, fallback domcontentloaded`);
      await page.goto(url, { waitUntil: 'domcontentloaded', timeout: 30000 });
    }

    try {
      await page.waitForSelector(
        '.zone-coordonnees, #blocCoordonnees, .bi-header, h1, [class*="coordonnee"]',
        { timeout: 10000 }
      );
    } catch (_) { logWarning(`Timeout sélecteur pour ${url}, on continue…`); }

    await new Promise(r => setTimeout(r, 2000));

    const details = await page.evaluate(() => {
      const result = {
        nom_entreprise: null, type_profil: 'AGENCE',
        telephone: null, email: null, site_web: null,
        a_formulaire_contact: false,
        adresse_complete: null, code_postal: null, ville: null,
        facebook_url: null, instagram_url: null, linkedin_company_url: null,
        tags: [], accessibilite: null, avis_note: null, avis_nombre: null
      };

      // 1. Nom
      const titleSelectors = ['h1[class*="denomination"]', '.denomination', 'h1', '[class*="company-name"]'];
      for (const sel of titleSelectors) {
        const el = document.querySelector(sel);
        if (el) { result.nom_entreprise = el.textContent.trim(); break; }
      }

      // 2. Téléphone
      const phoneSelectors = [
        '.coord-numero', '.bi-phone .bi-text', '[data-phone]',
        'a[href^="tel:"]', 'span[class*="phone"]', '[class*="telephone"]'
      ];
      for (const sel of phoneSelectors) {
        const el = document.querySelector(sel);
        if (el) {
          result.telephone = el.tagName === 'A' && el.href
            ? el.href.replace('tel:', '')
            : (el.textContent || el.getAttribute('data-phone') || '').trim();
          if (result.telephone) break;
        }
      }

      // 3. Formulaire mail
      if (document.querySelector('a[title*="mail"], a.btn_mail')) result.a_formulaire_contact = true;

      // 4. Site web
      const websiteSelectors = [
        '.lvs-container a[target="_blank"]', '.bi-website a',
        'a[href*="http"]:not([href*="pagesjaunes"])', '[class*="website"] a'
      ];
      for (const sel of websiteSelectors) {
        const el = document.querySelector(sel);
        if (el) {
          const txt = el.querySelector('.value')?.textContent.trim() || el.href || '';
          if (txt) {
            result.site_web = txt.startsWith('http') ? txt : 'http://' + txt;
            break;
          }
        }
      }

      // 5. Adresse
      const addressEl = document.querySelector(
        '.address-container .streetAddress, .address-container .noTrad, .bi-address, [class*="adresse"]'
      );
      if (addressEl) {
        result.adresse_complete = addressEl.textContent.trim();
        const cp = result.adresse_complete.match(/\b(\d{5})\b/);
        if (cp) {
          result.code_postal = cp[1];
          const parts = result.adresse_complete.split(cp[1]);
          if (parts.length > 1) result.ville = parts[1].trim().replace(/[,\s]+$/, '');
        }
      }

      // 6. Réseaux sociaux
      document.querySelectorAll('a[href]').forEach(link => {
        const href = link.href.toLowerCase();
        if (href.includes('facebook.com/') && !href.includes('share')) result.facebook_url = link.href;
        else if (href.includes('instagram.com/')) result.instagram_url = link.href;
        else if (href.includes('linkedin.com/company/')) result.linkedin_company_url = link.href;
      });

      // 7. Tags
      document.querySelectorAll('.bi-tags-list .bi-tag, .tags-list li').forEach(tag => {
        const t = tag.textContent.trim();
        if (t) result.tags.push(t);
      });

      // 8. Notes
      const noteEl = document.querySelector('.bi-note .note_moyenne');
      if (noteEl) {
        const m = noteEl.textContent.match(/(\d+[.,]?\d*)/);
        if (m) result.avis_note = parseFloat(m[1].replace(',', '.'));
      }
      const avisEl = document.querySelector('.bi-rating, [class*="nb-avis"]');
      if (avisEl) {
        const m = avisEl.textContent.match(/(\d+)/);
        if (m) result.avis_nombre = parseInt(m[1]);
      }

      return result;
    });

    await browser.close();
    return details;

  } catch (err) {
    logWarning(`Erreur scrapeAgencyDetail: ${err.message}`, { url });
    await browser.close();
    return null;
  }
}

function buildLeadFromPagesJaunesDetail(details, dept, crawlBatchId, sourceUrl = '') {
  const telephone = formatPhoneForSheets(details.telephone);
  const tags = (details.tags || []).join(' ').toLowerCase();
  let type_profil = 'AGENCE';
  if (tags.includes('orpi') || tags.includes('century 21') || tags.includes('laforêt') ||
    tags.includes('guy hoquet') || tags.includes('era') || tags.includes('fnaim')) {
    type_profil = 'AGENCE_RESEAU';
  }
  if (tags.includes('indépendant') || tags.includes('independant')) type_profil = 'INDEPENDANT';

  return {
    lead_id: uuidv4(),
    source: 'pagesjaunes',
    source_url: sourceUrl,
    type_profil,
    nom_entreprise: details.nom_entreprise,
    date_import: new Date().toISOString(),
    crawl_batch_id: crawlBatchId,
    adresse_complete: details.adresse_complete,
    code_postal: details.code_postal,
    ville: details.ville,
    departement: dept,
    lat: null, lng: null,
    telephone,
    email: normalizeEmail(details.email),
    site_web: details.site_web,
    url_contact_page: null,
    url_contact_form: details.a_formulaire_contact ? 'Présent sur PagesJaunes' : null,
    linkedin_company_url: details.linkedin_company_url,
    facebook_url: details.facebook_url,
    instagram_url: details.instagram_url,
    google_place_id: null,
    google_rating: details.avis_note || null,
    google_reviews_count: details.avis_nombre || null,
    phone_norm: normalizePhone(telephone || ''),
    domain_norm: normalizeDomain(details.site_web || ''),
    name_city_norm: normalizeNameCity(details.nom_entreprise || '', details.adresse_complete || ''),
    is_duplicate: false, duplicate_of_lead_id: null,
    data_quality: details.telephone ? 'MEDIUM' : 'LOW',
    missing: details.email ? [] : ['email'],
    score_global: null, priorite: null, reason: null,
    status: 'NEW', assigned_to: null, last_action_date: null,
    notes: details.tags?.length > 0 ? `Tags: ${details.tags.join(', ')}` : null
  };
}

/**
 * Scrape PagesJaunes pour une liste de départements
 */
async function scrapePagesJaunes(departments = [], crawlBatchId, options = {}) {
  const { maxPagesPerDept = 1 } = options;
  const targetDepts = departments.length > 0 ? departments : ['75', '69', '13', '31', '06', '92', '93', '94'];

  logInfo(`PagesJaunes — ${targetDepts.length} département(s)`, { depts: targetDepts });
  const allLeads = [];

  for (const dept of targetDepts) {
    if (cancelledBatches.has(crawlBatchId)) {
      logWarning(`🛑 Scraping PagesJaunes annulé pour le batch ${crawlBatchId}`);
      break;
    }

    const browser = await puppeteer.launch({
      headless: 'new',
      args: ['--no-sandbox', '--disable-setuid-sandbox']
    });

    try {
      const page = await browser.newPage();
      await page.setUserAgent('Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36');

      let baseUrl;
      if (dept === '75') baseUrl = 'https://www.pagesjaunes.fr/annuaire/chercherlespros?quoiqui=agence+immobiliere&ou=paris-75';
      else if (dept === '69') baseUrl = 'https://www.pagesjaunes.fr/annuaire/chercherlespros?quoiqui=agence+immobiliere&ou=lyon-69';
      else if (dept === '13') baseUrl = 'https://www.pagesjaunes.fr/annuaire/chercherlespros?quoiqui=agence+immobiliere&ou=marseille-13';
      else {
        const city = DEPT_VILLE_MAPPING[dept] || dept.toLowerCase();
        baseUrl = `https://www.pagesjaunes.fr/annuaire/chercherlespros?quoiqui=agence+immobiliere&ou=${city}-${dept}`;
      }

      let pageNum = 1;
      let hasNextPage = true;

      while (pageNum <= maxPagesPerDept && hasNextPage) {
        if (cancelledBatches.has(crawlBatchId)) {
          logWarning(`🛑 Scraping PagesJaunes annulé au milieu pour le batch ${crawlBatchId}`);
          break;
        }

        const pageUrl = pageNum === 1 ? baseUrl : `${baseUrl}&page=${pageNum}`;
        logInfo(`PagesJaunes dept=${dept} page=${pageNum}`, { url: pageUrl });

        await PJ_RATE_LIMITER.acquire('pagesjaunes');

        try {
          await page.goto(pageUrl, { waitUntil: 'networkidle2', timeout: 60000 });

          try {
            await page.waitForSelector('.bi-list, .bi-liste, li[id^="bi-"]', { timeout: 10000 });
          } catch (_) { logWarning(`Timeout .bi-list dept=${dept}`); }

          if (await cancellableSleep(2000, crawlBatchId)) throw new Error('CANCELLED');

          // Récupérer les liens des fiches
          const listingItems = await page.evaluate(() => {
            const items = [];
            document.querySelectorAll('li[id^="bi-"]').forEach(item => {
              if (item.classList.contains('pjts_pub-bloc')) return;
              const linkEl = item.querySelector('a.bi-denomination');
              const relUrl = linkEl ? linkEl.getAttribute('href') : null;
              const nomApercu = item.querySelector('h3')?.textContent?.trim() || null;
              if (relUrl && !relUrl.startsWith('#')) {
                items.push({ source_url: new URL(relUrl, window.location.origin).href, nom_apercu: nomApercu });
              }
            });
            return items;
          });

          logInfo(`dept=${dept} page=${pageNum}: ${listingItems.length} agences trouvées`);

          for (const item of listingItems) {
            if (cancelledBatches.has(crawlBatchId)) break;

            logInfo(`Traitement: ${item.nom_apercu} — ${item.source_url}`);
            const details = await scrapeAgencyDetail(item.source_url, crawlBatchId);
            if (details) {
              const lead = buildLeadFromPagesJaunesDetail(details, dept, crawlBatchId, item.source_url);
              allLeads.push(lead);
              logInfo(`✅ Lead PJ: ${lead.nom_entreprise}`, {
                telephone: lead.telephone ? '✓' : '✗',
                email: lead.email ? '✓' : '✗',
                site: lead.site_web ? '✓' : '✗'
              });
            }
            // Pause aléatoire entre les fiches
            if (await cancellableSleep(2000 + Math.random() * 3000, crawlBatchId)) break;
          }

          hasNextPage = await page.evaluate(() => !!document.querySelector('a#pagination-next, a.next'));
          pageNum++;

        } catch (err) {
          logError(`Erreur PJ dept=${dept} page=${pageNum}: ${err.message}`);
          hasNextPage = false;
        }

        if (pageNum <= maxPagesPerDept) {
          if (await cancellableSleep(15000 + Math.random() * 15000, crawlBatchId)) break;
        }
      }
    } catch (err) {
      logError(`Erreur browser PJ dept=${dept}: ${err.message}`);
    } finally {
      await browser.close();
    }
  }

  logInfo(`PagesJaunes terminé: ${allLeads.length} leads`);
  return allLeads;
}

// ─────────────────────────────────────────────
// 2-ENRICHISSEMENT-SITE.JS
// ─────────────────────────────────────────────
function mergeEnrichmentData(target, source) {
  if (source.emails?.length) target.emails.push(...source.emails);
  if (source.contactPage && !target.contactPage) target.contactPage = source.contactPage;
  if (source.contactForm) target.contactForm = true;
  Object.keys(source.social || {}).forEach(k => {
    if (source.social[k] && !target.social[k]) target.social[k] = source.social[k];
  });
}

async function scrapePageForContacts(url) {
  const browser = await puppeteer.launch({
    headless: 'new',
    args: ['--no-sandbox', '--disable-setuid-sandbox']
  });

  try {
    const page = await browser.newPage();
    await page.setUserAgent('Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36');

    try {
      await page.goto(url, { waitUntil: 'domcontentloaded', timeout: 15000 });
    } catch (err) {
      if (err.message && err.message.includes('net::ERR_')) {
        logWarning(`Erreur réseau pour ${url}: ${err.message}`);
        await browser.close();
        return { emails: [], contactPage: null, contactForm: false, social: {} };
      }
      throw err;
    }

    await new Promise(r => setTimeout(r, 1500));

    const result = await page.evaluate(() => {
      const r = { emails: [], contactPage: null, contactForm: false, social: {} };

      const emailRegex = /[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}/g;
      const allMatches = [
        ...(document.body.textContent.match(emailRegex) || []),
        ...(document.documentElement.outerHTML.match(emailRegex) || [])
      ];
      r.emails = [...new Set(allMatches.map(e => e.toLowerCase().trim()))];

      // Réseaux sociaux
      document.querySelectorAll('a[href]').forEach(a => {
        const href = a.href.toLowerCase();
        if (href.includes('facebook.com/') && !href.includes('share') && !href.includes('sharer'))
          r.social.facebook = a.href;
        else if (href.includes('instagram.com/')) r.social.instagram = a.href;
        else if (href.includes('linkedin.com/company/')) r.social.linkedin = a.href;
      });

      // Formulaire de contact
      r.contactForm = Array.from(document.querySelectorAll('form')).some(form => {
        const html = form.outerHTML.toLowerCase();
        const action = form.getAttribute('action') || '';
        return html.includes('email') || html.includes('message') || html.includes('contact') ||
          action.toLowerCase().includes('contact') ||
          !!form.querySelector('input[type="email"]') ||
          !!form.querySelector('textarea[name*="message"]') ||
          !!form.querySelector('input[name*="email"]') ||
          !!form.querySelector('button[type="submit"]');
      });

      // Page de contact
      const url2 = window.location.href.toLowerCase();
      const title = document.title.toLowerCase();
      const h1 = document.querySelector('h1')?.textContent.toLowerCase() || '';
      if (url2.includes('contact') || title.includes('contact') || h1.includes('contact') ||
        h1.includes('nous contacter') || h1.includes('contactez')) {
        r.contactPage = window.location.href;
      }

      return r;
    });

    await browser.close();
    return result;

  } catch (error) {
    await browser.close();
    throw error;
  }
}

async function enrichWebsite(lead) {
  if (!lead.site_web || lead.site_web === '#') return lead;

  logInfo(`🔍 Enrichissement site web: ${lead.nom_entreprise}`);
  const site = lead.site_web.startsWith('http') ? lead.site_web : `https://${lead.site_web}`;

  try {
    const domain = new URL(site).hostname;
    const limiter = getDomainLimiter(domain);
    await limiter.acquire(domain);

    const enrichment = {
      emails: [], contactPage: null, contactForm: false,
      social: { facebook: null, instagram: null, linkedin: null }
    };

    // 1) Page d'accueil
    try {
      const home = await scrapePageForContacts(site);
      mergeEnrichmentData(enrichment, home);
      logInfo(`Page d'accueil analysée: ${enrichment.emails.length} email(s)`);
    } catch (err) { logWarning(`Impossible d'ouvrir ${site}: ${err.message}`); }

    // 2) Pages prioritaires
    const PRIORITY_PATHS = ['/contact', '/contactez-nous', '/nous-contacter', '/mentions-legales', '/about', '/a-propos'];
    const origin = new URL(site).origin;
    for (const p of PRIORITY_PATHS) {
      if (enrichment.emails.length > 0 && enrichment.contactForm) break;
      const url = `${origin}${p}`;
      try {
        const pageData = await scrapePageForContacts(url);
        mergeEnrichmentData(enrichment, pageData);
        await new Promise(r => setTimeout(r, 1500 + Math.random() * 2000));
      } catch (err) {
        if (!String(err).includes('404')) logWarning(`Erreur sur ${url}: ${err.message}`);
      }
    }

    enrichment.emails = Array.from(new Set(enrichment.emails));

    if (enrichment.emails.length > 0) {
      const direct = enrichment.emails.find(e => !/^(contact|info|admin|bonjour|hello|commercial)/i.test(e.split('@')[0]));
      lead.email = normalizeEmail(direct || enrichment.emails[0]);
      logInfo(`✅ Email: ${lead.email}`);
    }
    if (enrichment.contactPage) { lead.url_contact_page = enrichment.contactPage; logInfo(`✅ Page contact: ${enrichment.contactPage}`); }
    if (enrichment.contactForm) { lead.url_contact_form = origin; logInfo(`✅ Formulaire contact trouvé`); }
    if (enrichment.social.facebook && !lead.facebook_url) { lead.facebook_url = enrichment.social.facebook; }
    if (enrichment.social.instagram && !lead.instagram_url) { lead.instagram_url = enrichment.social.instagram; }
    if (enrichment.social.linkedin && !lead.linkedin_company_url) { lead.linkedin_company_url = enrichment.social.linkedin; }

    if (lead.telephone) lead.phone_norm = normalizePhone(lead.telephone);
    lead.domain_norm = normalizeDomain(lead.site_web);

    return lead;

  } catch (error) {
    logError(`Erreur enrichissement site: ${error.message}`);
    return lead;
  }
}

// ─────────────────────────────────────────────
// 3-ENRICHISSEMENT-RESEAUX.JS
// ─────────────────────────────────────────────
function extractFromLinks(html) {
  const social = { linkedin: null, facebook: null, instagram: null };

  const linkedinLinks = html.match(/https?:\/\/(www\.)?linkedin\.com\/company\/[^"'\s]+/gi);
  if (linkedinLinks) social.linkedin = linkedinLinks[0];

  const fbLinks = html.match(/https?:\/\/(www\.)?facebook\.com\/[^"'\s]+/gi);
  if (fbLinks) {
    social.facebook = fbLinks.find(u => !u.includes('/share') && !u.includes('/sharer') && !u.includes('/plugins')) || null;
  }

  const igLinks = html.match(/https?:\/\/(www\.)?instagram\.com\/[^"'\s]+/gi);
  if (igLinks) {
    social.instagram = igLinks.find(u => !u.includes('/p/') && !u.includes('/explore/')) || null;
  }

  return social;
}

async function extractSocialFromWebsite(website) {
  const social = { linkedin: null, facebook: null, instagram: null };
  try {
    await SOCIAL_RATE_LIMITER.acquire('website');
    const browser = await puppeteer.launch({ headless: 'new', args: ['--no-sandbox', '--disable-setuid-sandbox'] });
    try {
      const page = await browser.newPage();
      await page.setUserAgent('Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36');
      await page.goto(website, { waitUntil: 'domcontentloaded', timeout: 30000 });
      await new Promise(r => setTimeout(r, 1500));

      const found = await page.evaluate(() => {
        const s = { facebook: null, instagram: null, linkedin: null };
        document.querySelectorAll('a[href]').forEach(a => {
          const href = a.href.toLowerCase();
          if (href.includes('facebook.com/') && !href.includes('share') && !href.includes('sharer'))
            s.facebook = a.href;
          if (href.includes('instagram.com/')) s.instagram = a.href;
          if (href.includes('linkedin.com/company/')) s.linkedin = a.href;
        });
        return s;
      });

      Object.assign(social, found);
      await browser.close();
    } catch (err) { await browser.close(); throw err; }
  } catch (error) { logWarning(`Erreur extraction sociale site: ${error.message}`); }
  return social;
}

async function searchWithFreeApis(lead) {
  const social = { linkedin: null, facebook: null, instagram: null };
  try {
    await SOCIAL_RATE_LIMITER.acquire('clearbit');
    const domain = lead.domain_norm || (lead.site_web ? extractDomain(lead.site_web) : null);
    if (domain) {
      const response = await fetch(
        `https://autocomplete.clearbit.com/v1/companies/suggest?query=${domain.split('.')[0]}`,
        { headers: { 'User-Agent': 'Mozilla/5.0' }, timeout: 10000 }
      );
      if (response.ok) {
        const data = await response.json();
        if (data && data.length > 0 && data[0].linkedin?.handle) {
          social.linkedin = `https://linkedin.com/company/${data[0].linkedin.handle}`;
          logInfo(`✅ LinkedIn via Clearbit: ${social.linkedin}`);
        }
      }
    }
  } catch (err) { logWarning(`Erreur Clearbit: ${err.message}`); }
  return social;
}

async function enrichSocial(lead) {
  try {
    logInfo(`🔗 Enrichissement réseaux sociaux: ${lead.nom_entreprise}`);

    if (lead.site_web && lead.site_web !== '#') {
      const s = await extractSocialFromWebsite(lead.site_web);
      if (s.linkedin && !lead.linkedin_company_url) { lead.linkedin_company_url = s.linkedin; }
      if (s.facebook && !lead.facebook_url) { lead.facebook_url = s.facebook; }
      if (s.instagram && !lead.instagram_url) { lead.instagram_url = s.instagram; }
    }

    if (!lead.linkedin_company_url || !lead.facebook_url || !lead.instagram_url) {
      const api = await searchWithFreeApis(lead);
      if (api.linkedin && !lead.linkedin_company_url) lead.linkedin_company_url = api.linkedin;
      if (api.facebook && !lead.facebook_url) lead.facebook_url = api.facebook;
      if (api.instagram && !lead.instagram_url) lead.instagram_url = api.instagram;
    }

    logInfo(`✅ Réseaux enrichis: ${lead.nom_entreprise}`, {
      linkedin: lead.linkedin_company_url ? '✓' : '✗',
      facebook: lead.facebook_url ? '✓' : '✗',
      instagram: lead.instagram_url ? '✓' : '✗'
    });
    return lead;
  } catch (error) {
    logWarning(`Erreur enrichissement social ${lead.nom_entreprise}: ${error.message}`);
    return lead;
  }
}

// ─────────────────────────────────────────────
// ENVOI N8N (un par un)
// ─────────────────────────────────────────────
// sendToN8n now optionally accepts a sheetId returned by /create-sheet route
async function sendToN8n(lead, sheetId = null) {
  await N8N_RATE_LIMITER.acquire('n8n');

  const payload = {
    type: 'lead',
    sheetId: sheetId || lead.sheetId || null,
    data: {
      lead_id: lead.lead_id,
      source: lead.source,
      source_url: lead.source_url || null,
      type_profil: lead.type_profil || 'AGENCE',
      nom_entreprise: lead.nom_entreprise,
      date_import: lead.date_import,
      crawl_batch_id: lead.crawl_batch_id,
      adresse_complete: lead.adresse_complete || '',
      code_postal: lead.code_postal || null,
      ville: lead.ville || null,
      departement: lead.departement || null,
      lat: lead.lat || null,
      lng: lead.lng || null,
      telephone: lead.telephone || '',
      email: lead.email || null,
      site_web: lead.site_web || null,
      url_contact_page: lead.url_contact_page || null,
      url_contact_form: lead.url_contact_form || null,
      linkedin_company_url: lead.linkedin_company_url || null,
      facebook_url: lead.facebook_url || null,
      instagram_url: lead.instagram_url || null,
      google_place_id: lead.google_place_id || null,
      google_rating: lead.google_rating || null,
      google_reviews_count: lead.google_reviews_count || null,
      phone_norm: lead.phone_norm || '',
      domain_norm: lead.domain_norm || '',
      name_city_norm: lead.name_city_norm || '',
      is_duplicate: lead.is_duplicate || false,
      duplicate_of_lead_id: lead.duplicate_of_lead_id || null,
      data_quality: lead.data_quality || 'MEDIUM',
      missing: lead.missing || [],
      score_global: lead.score_global || null,
      priorite: lead.priorite || null,
      reason: lead.reason || null,
      siret: lead.siret || null,
      siren: lead.siren || null,
      statut_juridique: lead.statut_juridique || null,
      societe_url: lead.societe_url || null,
      status: lead.status || 'NEW',
      assigned_to: lead.assigned_to || null,
      last_action_date: lead.last_action_date || null,
      notes: lead.notes || null,
      keyword: lead.keyword || null
    }
  };

  const response = await fetch(N8N_WEBHOOK_URL, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', 'User-Agent': 'ScrapingUI/1.0' },
    body: JSON.stringify(payload)
  });

  if (!response.ok) throw new Error(`n8n error: ${response.status} ${response.statusText}`);
  logInfo(`✅ Lead envoyé à n8n: ${lead.nom_entreprise}`);
  return true;
}

// ─────────────────────────────────────────────
// DONNÉES DE TEST (fallback)
// ─────────────────────────────────────────────
function generateTestData(keyword, count = 5) {
  const departments = ['75', '92', '93', '94', '69', '13', '31', '33'];
  const DISPLAY_CITIES = {
    '75': 'Paris', '92': 'Nanterre', '93': 'Bobigny', '94': 'Créteil',
    '69': 'Lyon', '13': 'Marseille', '31': 'Toulouse', '33': 'Bordeaux'
  };
  const leads = [];
  for (let i = 0; i < count; i++) {
    const dept = departments[i % departments.length];
    const city = DISPLAY_CITIES[dept] || dept;
    const tel = formatPhoneForSheets(`0${i + 1}${String(i).repeat(8)}`);
    leads.push({
      lead_id: uuidv4(),
      source: 'TEST_DATA',
      source_url: null,
      type_profil: i % 3 === 0 ? 'INDEPENDANT' : 'AGENCE',
      nom_entreprise: `Test Agency ${i + 1} - ${city}`,
      date_import: new Date().toISOString(),
      crawl_batch_id: `test-batch-${Date.now()}`,
      adresse_complete: `${i + 1} Rue Test, ${dept}000 ${city}`,
      code_postal: `${dept}000`, ville: city, departement: dept,
      lat: 48.8566 + i * 0.01, lng: 2.3522 + i * 0.01,
      telephone: tel,
      email: i % 2 === 0 ? `contact@test${i}.fr` : `info@test${i}.com`,
      site_web: `https://www.test${i}.fr`,
      url_contact_page: i % 2 === 0 ? `https://www.test${i}.fr/contact` : null,
      url_contact_form: i % 3 === 0 ? `https://www.test${i}.fr` : null,
      linkedin_company_url: i % 2 === 0 ? `https://www.linkedin.com/company/test${i}` : null,
      facebook_url: i % 3 === 0 ? `https://www.facebook.com/test${i}` : null,
      instagram_url: i % 4 === 0 ? `https://www.instagram.com/test${i}` : null,
      google_place_id: null, google_rating: null, google_reviews_count: null,
      phone_norm: normalizePhone(tel || ''),
      domain_norm: `test${i}.fr`,
      name_city_norm: `test agency ${i} ${city}`.toLowerCase(),
      is_duplicate: false, duplicate_of_lead_id: null,
      data_quality: 'MEDIUM', missing: [],
      score_global: null, priorite: null, reason: null,
      status: 'NEW', assigned_to: null, last_action_date: null,
      notes: `Test data for ${keyword}`, keyword
    });
  }
  return leads;
}

// ─────────────────────────────────────────────
// PROCESSUS PRINCIPAL
// ─────────────────────────────────────────────
/**
 * Traite un seul lead : enrichissement site → enrichissement social → scoring → envoi n8n
 */
async function processLead(lead, options = {}) {
  const {
    enableWebsiteEnrichment = true,
    enableSocialEnrichment = true,
    enableN8nSending = true,
    crawlBatchId = null
  } = options;

  if (crawlBatchId && cancelledBatches.has(crawlBatchId)) {
    throw new Error('Process cancelled');
  }

  logInfo(`🚀 Traitement: ${lead.nom_entreprise}`, { leadId: lead.lead_id });

  // Étape 1 — Enrichissement site web
  if (enableWebsiteEnrichment && lead.site_web && (!lead.email || lead.email.trim() === '')) {
    try {
      lead = await enrichWebsite(lead);
      if (crawlBatchId && cancelledBatches.has(crawlBatchId)) throw new Error('CANCELLED');
    } catch (err) { logWarning(`Erreur enrichissement site: ${err.message}`); }
  }

  // Étape 2 — Enrichissement réseaux sociaux
  if (enableSocialEnrichment) {
    try {
      lead = await enrichSocial(lead);
      if (crawlBatchId && cancelledBatches.has(crawlBatchId)) throw new Error('CANCELLED');
    } catch (err) { logWarning(`Erreur enrichissement social: ${err.message}`); }
  }

  // Étape 3 — Scoring
  lead = withUpdatedScore(lead);
  logInfo(`📊 Score: ${lead.score_global} (${lead.priorite}) — ${lead.nom_entreprise}`);

  // Étape 4 — Vérification activité sur societe.com + récupération SIRET
  if (crawlBatchId && cancelledBatches.has(crawlBatchId)) throw new Error('CANCELLED');
  try {
    const societeInfo = await checkCompanyStatus(lead);

    // Enrichir le lead avec les infos récupérées
    lead.siret            = societeInfo.siret || null;
    lead.siren            = societeInfo.siren || null;
    lead.statut_juridique = societeInfo.statut;       // 'ACTIVE' | 'INACTIVE' | 'UNKNOWN' | 'NOT_FOUND'
    lead.societe_url      = societeInfo.sourceUrl || null;

    if (!societeInfo.active) {
      // Société inactive ou radiée → on skip l'envoi et on marque le lead
      logWarning(`🚫 Société inactive/radiée — lead ignoré: "${lead.nom_entreprise}" (${societeInfo.statut})`);
      lead.status = 'SKIPPED_INACTIVE';
      lead.last_action_date = new Date().toISOString();
      return lead; // ← sortie anticipée, pas d'envoi n8n ni HubSpot
    }

    logInfo(`✅ Société active — SIRET: ${lead.siret || lead.siren || 'N/A'} — "${lead.nom_entreprise}"`);
  } catch (err) {
    // En cas d'erreur imprévue on laisse passer le lead (on ne bloque pas)
    logWarning(`Erreur vérification societe.com: ${err.message} — lead conservé`);
    lead.siret = null;
    lead.siren = null;
    lead.statut_juridique = 'UNKNOWN';
  }

  // Étape 5 — Envoi n8n (uniquement si société active)
  if (enableN8nSending) {
    try {
      await sendToN8n(lead, lead.sheetId);
      lead.status = 'SENT_TO_N8N';
      lead.last_action_date = new Date().toISOString();
    } catch (err) {
      logError(`Erreur envoi n8n: ${err.message}`);
      lead.status = 'N8N_ERROR';
      lead.last_action_date = new Date().toISOString();
    }
  }

  return lead;
}

/**
 * Point d'entrée principal
 * @param {string} keyword    — mot-clé de recherche
 * @param {string[]} sources  — tableau de sources ['OpenStreetMap'] etc.
 * @param {string[]} departments — liste de départements, ex: ['75','69']
 * @param {Object} options    — options de traitement
 */
async function mainProcess(keyword, sources, departments = [], options = {}) {
  const {
    enableWebsiteEnrichment = true,
    enableSocialEnrichment = true,
    enableN8nSending = true,
    concurrency = 2,
    delayBetweenLeads = 8000,
    maxPagesPerDept = 1,
    sheetId = null,
    crawlBatchId = uuidv4()
  } = options;

  logInfo(`🚀 Démarrage — keyword="${keyword}" sources="${sources.join(',')}" depts="${departments.join(',') || 'ALL'}"`);

  let rawLeads = [];

  // ── SCRAPING ──
  for (const source of sources) {
    if (cancelledBatches.has(crawlBatchId)) {
      logWarning(`Scraping global annulé avant de démarrer ${source}`);
      break;
    }

    let sourceLeads = [];
    if (source === 'OpenStreetMap') {
      sourceLeads = await scrapeOpenStreetMap(
        departments.length > 0 ? departments : DEPARTEMENTS_FRANCE,
        crawlBatchId
      );
    } else if (source === 'PagesJaunes') {
      sourceLeads = await scrapePagesJaunes(departments, crawlBatchId, { maxPagesPerDept });
    } else if (source === 'TEST_DATA') {
      sourceLeads = generateTestData(keyword, 5);
    } else {
      logWarning(`Source invalide ou non reconnue: ${source}`);
    }

    rawLeads.push(...sourceLeads);
  }

  // attach sheetId to raw leads when available
  if (sheetId && rawLeads.length) {
    rawLeads.forEach(l => { l.sheetId = sheetId; });
    logInfo(`📄 sheetId ${sheetId} ajouté à ${rawLeads.length} leads`);
  }
  logInfo(`✅ ${rawLeads.length} leads bruts récupérés`);

  if (rawLeads.length === 0) {
    logWarning('Aucun lead trouvé, retour vide');
    return { leads: [], total: 0, successful: 0, failed: 0 };
  }

  const results = { total: rawLeads.length, successful: 0, failed: 0, leads: [] };
  let wasCancelled = false;

  // ── TRAITEMENT PAR LOTS ──
  for (let i = 0; i < rawLeads.length; i += concurrency) {
    if (cancelledBatches.has(crawlBatchId)) {
      logWarning(`🛑 Scraping annulé au cours de l'enrichissement. Retour des leads bruts.`);
      wasCancelled = true;
      // Si on annule, on remplit results avec les leads bruts non encore traités pour l'affichage
      if (results.leads.length === 0) {
        results.leads = rawLeads;
        results.successful = rawLeads.length;
      }
      break;
    }
    const batch = rawLeads.slice(i, i + concurrency);

    const batchResults = await Promise.allSettled(
      batch.map(async (lead, idx) => {
        if (cancelledBatches.has(crawlBatchId)) return Promise.reject(new Error('Cancelled'));
        if (idx > 0) {
          if (await cancellableSleep(2000, crawlBatchId)) return Promise.reject(new Error('CANCELLED'));
        }

        return processLead(lead, { enableWebsiteEnrichment, enableSocialEnrichment, enableN8nSending, crawlBatchId });
      })
    );

    for (const res of batchResults) {
      if (res.status === 'fulfilled') {
        results.successful++;
        results.leads.push(res.value);
        logInfo(`✅ Lead traité: ${res.value.nom_entreprise}`, {
          score: res.value.score_global,
          priorite: res.value.priorite
        });
      } else {
        results.failed++;
        logError(`❌ Erreur lead: ${res.reason?.message}`);
      }
    }

    if (i + concurrency < rawLeads.length) {
      logInfo(`⏳ Pause ${delayBetweenLeads}ms avant le prochain lot…`);
      if (await cancellableSleep(delayBetweenLeads, crawlBatchId)) break;
    }

    const progress = Math.round(((i + concurrency) / rawLeads.length) * 100);
    logInfo(`📊 Progression: ${Math.min(progress, 100)}% (${Math.min(i + concurrency, rawLeads.length)}/${rawLeads.length})`, {
      réussis: results.successful, erreurs: results.failed
    });
  }

  logInfo(`🎉 Terminé: ${results.successful}/${results.total} leads traités avec succès`);

  // Exposer l'info d'annulation au caller (controller) de façon fiable
  // (le controller ne doit pas dépendre du Set, car il est nettoyé ici)
  if (!wasCancelled && cancelledBatches.has(crawlBatchId)) wasCancelled = true;
  results.cancelled = wasCancelled;

  // Nettoyage de l'id d'annulation (évite fuite mémoire)
  cancelledBatches.delete(crawlBatchId);

  return results;
}

function cancelScrape(batchId) {
  if (batchId) {
    cancelledBatches.add(batchId);
    logInfo(`🛑 Signal d'annulation reçu pour le batch ${batchId}`);
  }
}

function isCancelled(batchId) {
  return batchId ? cancelledBatches.has(batchId) : false;
}

// ─────────────────────────────────────────────
// EXPORTS
// ─────────────────────────────────────────────
module.exports = {
  mainProcess,
  cancelScrape,
  isCancelled, // Exporter la fonction de vérification
  scrapeOpenStreetMap,
  scrapePagesJaunes,
  enrichWebsite,
  enrichSocial,
  calculateScore,
  withUpdatedScore,
  sendToN8n,
  generateTestData,
  cancelledBatches, // Exporter pour vérification dans le contrôleur
  // Helpers utilitaires
  normalizePhone,
  formatPhoneForSheets,
  normalizeEmail,
  normalizeDomain,
  normalizeNameCity,
  isDirectEmail,
  isGenericEmail,
  isMobilePhone
};