// services/societeService.js
// Vérifie si une entreprise est active et récupère son SIRET
// via l'API officielle du gouvernement français :
//   https://recherche-entreprises.api.gouv.fr
//
// ✅ Gratuite, sans clé API, sans CAPTCHA
// ✅ Données INSEE mises à jour quotidiennement
// ✅ 7 requêtes/seconde autorisées
// ✅ Champ etatAdministratif : "A" = actif, "F" = fermé

const fetch = require('node-fetch');
const { logInfo, logWarning, logError } = require('../utils/logger');

// ── Rate limiter : max 5 req/s (en dessous de la limite de 7) ────────
const MIN_DELAY_MS = 200; // 200ms entre chaque appel = ~5/s
let _lastCall = 0;

async function _throttle() {
  const elapsed = Date.now() - _lastCall;
  if (elapsed < MIN_DELAY_MS) {
    await new Promise(r => setTimeout(r, MIN_DELAY_MS - elapsed));
  }
  _lastCall = Date.now();
}

// ── Nettoyage du nom pour la recherche ───────────────────────────────
function _cleanName(name) {
  if (!name) return '';
  return name
    .replace(/\b(SARL|SAS|SA|SCI|SASU|EURL|EI|EIRL|SNC|GIE|SCM|SCP|SELARL|SELAS|SCA|SCOP|SCIC)\b/gi, '')
    .replace(/[&+*()[\]{}]/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

// ── Sélection du meilleur établissement parmi les résultats ──────────
function _pickBestEtablissement(matching_etablissements) {
  if (!matching_etablissements || matching_etablissements.length === 0) return null;
  const siegeActif = matching_etablissements.find(e => e.est_siege && e.etat_administratif === 'A');
  if (siegeActif) return siegeActif;
  const actif = matching_etablissements.find(e => e.etat_administratif === 'A');
  if (actif) return actif;
  const siege = matching_etablissements.find(e => e.est_siege);
  return siege || matching_etablissements[0];
}

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

  const cleanedName = _cleanName(lead.nom_entreprise);
  if (!cleanedName) return result;

  const cityPart = lead.ville ? ` ${lead.ville}` : '';
  const query = encodeURIComponent(`${cleanedName}${cityPart}`);
  const cpParam = lead.code_postal ? `&code_postal=${encodeURIComponent(lead.code_postal)}` : '';
  const apiUrl = `https://recherche-entreprises.api.gouv.fr/search?q=${query}${cpParam}&per_page=5&minimal=true`;

  logInfo(`🔍 INSEE API — recherche: "${cleanedName}${cityPart}"`, { leadId: lead.lead_id });

  try {
    await _throttle();

    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), 10000);

    let apiData;
    try {
      const res = await fetch(apiUrl, {
        signal: controller.signal,
        headers: {
          'Accept': 'application/json',
          'User-Agent': 'ScrapingTool/1.0'
        }
      });
      clearTimeout(timeout);

      if (!res.ok) {
        logWarning(`INSEE API HTTP ${res.status} pour "${lead.nom_entreprise}"`);
        result.statut = 'UNKNOWN';
        result.active = true;
        return result;
      }

      apiData = await res.json();
    } catch (err) {
      clearTimeout(timeout);
      throw err;
    }

    if (!apiData.results || apiData.results.length === 0) {
      logWarning(`INSEE API — aucun résultat pour "${lead.nom_entreprise}"`);
      result.statut = 'UNKNOWN';
      result.active = true;
      return result;
    }

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

    const etat = unite.etat_administratif || (etablissement && etablissement.etat_administratif) || null;

    if (etat === 'A') {
      result.statut = 'ACTIVE';
      result.active = true;
    } else if (etat === 'F') {
      result.statut = 'INACTIVE';
      result.active = false;
    } else {
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
    result.statut = 'UNKNOWN';
    result.active = true;
    return result;
  }
}

module.exports = { checkCompanyStatus };
