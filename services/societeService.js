// services/societeService.js
// Vérifie si une entreprise est active et récupère son SIRET
// via l'API officielle du gouvernement français :
//   https://recherche-entreprises.api.gouv.fr
//
// Logique de décision :
//   - Erreur réseau / timeout           → UNKNOWN   + active:true  (pb technique, on garde)
//   - Résultat trouvé, etat:'A'         → ACTIVE    + active:true
//   - Résultat trouvé, etat:'F'         → INACTIVE  + active:false (fermée = rejetée)
//   - Résultat trouvé, etat:null        → UNKNOWN   + active:true  (données incomplètes, on garde)
//   - Aucun résultat (même après retry) → NOT_FOUND + active:false (inexistant INSEE = rejeté)

const fetch = require('node-fetch');
const { logInfo, logWarning, logError } = require('../utils/logger');

const MIN_DELAY_MS = 200; // 5 req/s max (limite officielle : 7/s)
let _lastCall = 0;

async function _throttle() {
  const elapsed = Date.now() - _lastCall;
  if (elapsed < MIN_DELAY_MS) {
    await new Promise(r => setTimeout(r, MIN_DELAY_MS - elapsed));
  }
  _lastCall = Date.now();
}

// ── Nettoyage MINIMAL du nom : seulement les caractères qui cassent l'URL ──
// On ne supprime PAS les formes juridiques — elles peuvent aider l'API
// à trouver la bonne société. On enlève juste les caractères spéciaux.
function _cleanName(name) {
  if (!name) return '';
  return name
    .replace(/[«»""'']/g, '"')   // normaliser les guillemets
    .replace(/[&+*()[\]{}]/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

// ── Sélection du meilleur établissement ─────────────────────────────
function _pickBestEtablissement(matching_etablissements) {
  if (!matching_etablissements || matching_etablissements.length === 0) return null;
  // 1. Siège actif
  const siegeActif = matching_etablissements.find(e => e.est_siege && e.etat_administratif === 'A');
  if (siegeActif) return siegeActif;
  // 2. N'importe quel établissement actif
  const actif = matching_etablissements.find(e => e.etat_administratif === 'A');
  if (actif) return actif;
  // 3. Siège même fermé
  const siege = matching_etablissements.find(e => e.est_siege);
  return siege || matching_etablissements[0];
}

// ── Appel API avec throttle ──────────────────────────────────────────
// IMPORTANT : on n'utilise PAS &minimal=true — ce paramètre réduit les
// résultats retournés et cause des NOT_FOUND sur des sociétés existantes.
async function _apiSearch(query, extraParams = '') {
  await _throttle();

  const url = `https://recherche-entreprises.api.gouv.fr/search?q=${encodeURIComponent(query)}&per_page=5${extraParams}`;

  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), 10000);

  try {
    const res = await fetch(url, {
      signal: controller.signal,
      headers: {
        'Accept': 'application/json',
        'User-Agent': 'ScrapingTool/1.0'
      }
    });
    clearTimeout(timeout);
    if (!res.ok) return null;
    return await res.json();
  } catch (err) {
    clearTimeout(timeout);
    throw err;
  }
}

// ── Stratégies de recherche progressives ────────────────────────────
// On essaie plusieurs formulations pour maximiser les chances de trouver
// la société, surtout pour les noms de franchise type "Orpi Daveau ..."
function _buildSearchStrategies(lead) {
  const cleaned = _cleanName(lead.nom_entreprise);
  const city = lead.ville || '';
  const cp = lead.code_postal ? `&code_postal=${encodeURIComponent(lead.code_postal)}` : '';

  const strategies = [];

  // 1. Nom complet + ville + code postal
  if (city) strategies.push({ query: `${cleaned} ${city}`, extra: cp });

  // 2. Nom complet seul (sans ville)
  strategies.push({ query: cleaned, extra: cp });

  // 3. Nom complet sans code postal
  if (city && cp) strategies.push({ query: `${cleaned} ${city}`, extra: '' });

  // 4. Nom court : supprimer le préfixe de franchise (Orpi, Century 21, Guy Hoquet...)
  // ex: "Orpi Daveau Conseil Immobilier" → "Daveau Conseil Immobilier"
  const withoutFranchise = cleaned
    .replace(/^(orpi|century\s*21|c21|guy\s*hoquet|stéphane\s*plaza|era|laforêt|laforet|foncia|nexity|square\s*habitat)\s+/i, '')
    .trim();
  if (withoutFranchise && withoutFranchise !== cleaned) {
    strategies.push({ query: withoutFranchise, extra: cp });
    if (city) strategies.push({ query: `${withoutFranchise} ${city}`, extra: '' });
  }

  return strategies;
}

/**
 * Vérifie l'activité d'une entreprise via l'API Recherche Entreprises
 *
 * @param {Object} lead  — lead avec au moins nom_entreprise, et optionnellement ville/code_postal
 * @returns {Promise<{
 *   active: boolean,
 *   siret: string|null,
 *   siren: string|null,
 *   statut: 'ACTIVE'|'INACTIVE'|'UNKNOWN'|'NOT_FOUND',
 *   nom_officiel: string|null,
 *   sourceUrl: string|null
 * }>}
 */
async function checkCompanyStatus(lead) {
  const result = {
    active: false,
    siret: null,
    siren: null,
    statut: 'NOT_FOUND',
    nom_officiel: null,
    sourceUrl: null
  };

  if (!lead.nom_entreprise) return result;

  const strategies = _buildSearchStrategies(lead);

  logInfo(`🔍 INSEE API — recherche: "${_cleanName(lead.nom_entreprise)}"`, { leadId: lead.lead_id });

  let apiData = null;

  try {
    // ── Essayer les stratégies une par une jusqu'à trouver un résultat ──
    for (const { query, extra } of strategies) {
      if (query.length < 3) continue;

      const data = await _apiSearch(query, extra);
      if (data && data.results && data.results.length > 0) {
        apiData = data;
        if (query !== _cleanName(lead.nom_entreprise)) {
          logInfo(`🔄 Trouvé via stratégie alternative: "${query}"`, { leadId: lead.lead_id });
        }
        break;
      }
    }

    // ── Aucun résultat après toutes les stratégies ───────────────────
    if (!apiData || !apiData.results || apiData.results.length === 0) {
      logWarning(`🚫 INSEE — introuvable dans le registre: "${lead.nom_entreprise}"`);
      result.statut = 'NOT_FOUND';
      result.active = false;
      return result;
    }

    // ── Résultat trouvé : extraire les infos ─────────────────────────
    const unite = apiData.results[0];
    const etablissement = _pickBestEtablissement(unite.matching_etablissements);

    result.siren = unite.siren || null;
    result.nom_officiel = unite.nom_complet || unite.denomination || null;
    result.sourceUrl = unite.siren
      ? `https://annuaire-entreprises.data.gouv.fr/entreprise/${unite.siren}`
      : null;

    if (etablissement) {
      result.siret = etablissement.siret || null;
    }

    // ── Déterminer le statut ─────────────────────────────────────────
    const etat = unite.etat_administratif ||
      (etablissement && etablissement.etat_administratif) || null;

    if (etat === 'A') {
      result.statut = 'ACTIVE';
      result.active = true;
    } else if (etat === 'F') {
      result.statut = 'INACTIVE';
      result.active = false;
    } else {
      // etat null/inconnu mais société trouvée → on garde, pas de faux rejet
      result.statut = 'UNKNOWN';
      result.active = true;
    }

    logInfo(
      `📋 INSEE — "${lead.nom_entreprise}" → ${result.statut} | SIRET: ${result.siret || result.siren || 'N/A'} | Officiel: "${result.nom_officiel || 'N/A'}"`,
      { leadId: lead.lead_id }
    );

    return result;

  } catch (err) {
    if (err.name === 'AbortError') {
      logWarning(`INSEE API timeout pour "${lead.nom_entreprise}" — lead conservé`);
    } else {
      logError(`INSEE API erreur pour "${lead.nom_entreprise}": ${err.message}`);
    }
    // Erreur technique → on ne punit pas le lead
    result.statut = 'UNKNOWN';
    result.active = true;
    return result;
  }
}

module.exports = { checkCompanyStatus };
