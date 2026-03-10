const fetch = require('node-fetch');
const BASE_URL     = 'https://api.hubapi.com';
const ACCESS_TOKEN = process.env.HUBSPOT_ACCESS_TOKEN;

const { logInfo, logWarning, logError } = require('../utils/logger');

// ─────────────────────────────────────────────
// HELPER HTTP
// ─────────────────────────────────────────────
async function hsRequest(method, endpoint, body = null) {
  if (!ACCESS_TOKEN) throw new Error('HUBSPOT_ACCESS_TOKEN manquant dans .env');
  const opts = {
    method,
    headers: { Authorization: `Bearer ${ACCESS_TOKEN}`, 'Content-Type': 'application/json' }
  };
  if (body) opts.body = JSON.stringify(body);
  const res = await fetch(`${BASE_URL}${endpoint}`, opts);
  if (!res.ok) {
    const txt = await res.text();
    throw new Error(`HubSpot ${res.status}: ${txt}`);
  }
  return res.json();
}

// ─────────────────────────────────────────────
// AUTO-PROVISION DES PROPRIÉTÉS CUSTOM
// ─────────────────────────────────────────────

const CUSTOM_COMPANY_PROPS = [
  'lead_departement', 'lead_source', 'lead_type_profil', 'lead_score',
  'lead_priorite', 'linkedin_company_page', 'facebook_company_page',
  'instagram_company_page', 'external_lead_id', 'crawl_batch_id',
  'google_rating', 'google_reviews_count'
];

const CUSTOM_CONTACT_PROPS = [
  'external_lead_id', 'lead_score', 'lead_priorite', 'lead_source'
];

const PROP_LABELS = {
  lead_departement:       'Département',
  lead_source:            'Source scraping',
  lead_type_profil:       'Type profil',
  lead_score:             'Score lead',
  lead_priorite:          'Priorité lead',
  linkedin_company_page:  'LinkedIn Company',
  facebook_company_page:  'Facebook Company',
  instagram_company_page: 'Instagram Company',
  external_lead_id:       'ID lead externe',
  crawl_batch_id:         'Batch ID crawl',
  google_rating:          'Note Google',
  google_reviews_count:   'Nb avis Google',
};

let propertiesProvisioned = false;

/**
 * Récupère le premier groupName disponible pour un objet HubSpot
 * (évite l'erreur "group doesn't exist")
 */
async function getDefaultGroup(objectType) {
  try {
    const res = await hsRequest('GET', `/crm/v3/properties/${objectType}/groups`);
    const groups = res.results || [];
    // Préférer un groupe "custom" ou "information", sinon prendre le premier
    const preferred = groups.find(g =>
      g.name.includes('information') || g.name.includes('custom') || g.name.includes('companyinformation')
    );
    const group = preferred || groups[0];
    if (group) {
      logInfo(`HubSpot: groupe utilisé pour ${objectType}: ${group.name}`);
      return group.name;
    }
  } catch (err) {
    logWarning(`HubSpot: impossible de lister les groupes ${objectType} — ${err.message}`);
  }
  // Valeurs par défaut connues de HubSpot
  return objectType === 'companies' ? 'companyinformation' : 'contactinformation';
}

async function provisionPropsForObject(objectType, propNames) {
  // 1. Récupérer les propriétés existantes
  let existing = new Set();
  try {
    const res = await hsRequest('GET', `/crm/v3/properties/${objectType}`);
    (res.results || []).forEach(p => existing.add(p.name));
    logInfo(`HubSpot: ${existing.size} propriétés existantes pour ${objectType}`);
  } catch (err) {
    logWarning(`HubSpot: impossible de lister les propriétés ${objectType} — ${err.message}`);
    return;
  }

  const missing = propNames.filter(n => !existing.has(n));
  if (missing.length === 0) {
    logInfo(`HubSpot: toutes les propriétés ${objectType} sont déjà présentes`);
    return;
  }

  logInfo(`HubSpot: ${missing.length} propriété(s) à créer pour ${objectType}: ${missing.join(', ')}`);

  // 2. Récupérer un groupName valide
  const groupName = await getDefaultGroup(objectType);

  // 3. Créer chaque propriété manquante
  for (const name of missing) {
    try {
      const payload = {
        name,
        label:     PROP_LABELS[name] || name,
        type:      'string',
        fieldType: 'text',
        groupName
      };
      await hsRequest('POST', `/crm/v3/properties/${objectType}`, payload);
      logInfo(`HubSpot: ✅ propriété créée — ${objectType}.${name}`);
    } catch (err) {
      // Si elle existe déjà (race condition ou cache) on ignore
      if (
        err.message.includes('PROPERTY_EXISTS') ||
        err.message.includes('already exists') ||
        err.message.includes('409')
      ) {
        logInfo(`HubSpot: propriété déjà existante (ok) — ${objectType}.${name}`);
      } else {
        logError(`HubSpot: ❌ impossible de créer ${objectType}.${name} — ${err.message}`);
      }
    }
  }
}

async function ensureCustomProperties() {
  if (propertiesProvisioned) return;
  logInfo('HubSpot: vérification/création des propriétés custom…');
  await provisionPropsForObject('companies', CUSTOM_COMPANY_PROPS);
  await provisionPropsForObject('contacts',  CUSTOM_CONTACT_PROPS);
  propertiesProvisioned = true;
  logInfo('HubSpot: provisioning terminé ✅');
}

// ─────────────────────────────────────────────
// MAPPING lead → propriétés HubSpot
// ─────────────────────────────────────────────
function buildCompanyProps(lead) {
  const props = {
    name:    lead.nom_entreprise   || 'Agence inconnue',
    city:    lead.ville            || '',
    zip:     lead.code_postal      || '',
    country: 'France',
    address: lead.adresse_complete || '',
    phone:   lead.telephone        || '',
    website: lead.site_web         || ''
  };
  if (lead.departement)          props.lead_departement       = lead.departement;
  if (lead.source)               props.lead_source            = lead.source;
  if (lead.type_profil)          props.lead_type_profil       = lead.type_profil;
  if (lead.score_global != null) props.lead_score             = String(lead.score_global);
  if (lead.priorite)             props.lead_priorite          = lead.priorite;
  if (lead.linkedin_company_url) props.linkedin_company_page  = lead.linkedin_company_url;
  if (lead.facebook_url)         props.facebook_company_page  = lead.facebook_url;
  if (lead.instagram_url)        props.instagram_company_page = lead.instagram_url;
  if (lead.lead_id)              props.external_lead_id       = lead.lead_id;
  if (lead.crawl_batch_id)       props.crawl_batch_id         = lead.crawl_batch_id;
  if (lead.google_rating)        props.google_rating          = String(lead.google_rating);
  return props;
}

function buildContactProps(lead) {
  const props = {
    firstname:      lead.nom_entreprise || 'Contact',
    lastname:       '',
    company:        lead.nom_entreprise || '',
    phone:          lead.telephone      || '',
    city:           lead.ville          || '',
    zip:            lead.code_postal    || '',
    country:        'France',
    hs_lead_status: 'NEW'
  };
  if (lead.email)                props.email            = lead.email;
  if (lead.lead_id)              props.external_lead_id = lead.lead_id;
  if (lead.score_global != null) props.lead_score       = String(lead.score_global);
  if (lead.priorite)             props.lead_priorite    = lead.priorite;
  if (lead.source)               props.lead_source      = lead.source;
  return props;
}

// ─────────────────────────────────────────────
// DÉDUPLICATION
// ─────────────────────────────────────────────
async function findExistingCompany(lead) {
  try {
    if (lead.lead_id) {
      const r = await hsRequest('POST', '/crm/v3/objects/companies/search', {
        filterGroups: [{ filters: [{ propertyName: 'external_lead_id', operator: 'EQ', value: lead.lead_id }] }],
        properties: ['hs_object_id', 'name']
      });
      if (r.results?.length) return r.results[0];
    }
    if (lead.nom_entreprise) {
      const r = await hsRequest('POST', '/crm/v3/objects/companies/search', {
        filterGroups: [{ filters: [
          { propertyName: 'name', operator: 'EQ', value: lead.nom_entreprise },
          { propertyName: 'city', operator: 'EQ', value: lead.ville || '' }
        ]}],
        properties: ['hs_object_id', 'name']
      });
      if (r.results?.length) return r.results[0];
    }
  } catch (err) {
    logWarning(`HubSpot: recherche company impossible — ${err.message}`);
  }
  return null;
}

async function findExistingContact(email) {
  if (!email) return null;
  try {
    return await hsRequest('GET',
      `/crm/v3/objects/contacts/${encodeURIComponent(email)}?idProperty=email&properties=hs_object_id,email`
    );
  } catch (err) {
    if (err.message.includes('404')) return null;
    logWarning(`HubSpot: recherche contact impossible — ${err.message}`);
    return null;
  }
}

// ─────────────────────────────────────────────
// UPSERT
// ─────────────────────────────────────────────
async function upsertCompany(lead) {
  const properties = buildCompanyProps(lead);
  const existing   = await findExistingCompany(lead);
  if (existing) {
    const r = await hsRequest('PATCH', `/crm/v3/objects/companies/${existing.id}`, { properties });
    return { ...r, _action: 'updated' };
  }
  const r = await hsRequest('POST', '/crm/v3/objects/companies', { properties });
  return { ...r, _action: 'created' };
}

async function upsertContact(lead) {
  if (!lead.email && !lead.telephone) return null;
  const properties = buildContactProps(lead);
  const existing   = await findExistingContact(lead.email);
  if (existing) {
    const r = await hsRequest('PATCH', `/crm/v3/objects/contacts/${existing.id}`, { properties });
    return { ...r, _action: 'updated' };
  }
  const r = await hsRequest('POST', '/crm/v3/objects/contacts', { properties });
  return { ...r, _action: 'created' };
}

async function associateContactToCompany(contactId, companyId) {
  try {
    await hsRequest('PUT',
      `/crm/v3/objects/contacts/${contactId}/associations/companies/${companyId}/contact_to_company`,
      {}
    );
  } catch (err) {
    logWarning(`HubSpot: association contact→company impossible — ${err.message}`);
  }
}

// ─────────────────────────────────────────────
// API PUBLIQUE
// ─────────────────────────────────────────────
async function sendLeadToHubSpot(lead) {
  if (!ACCESS_TOKEN) return { success: false, error: 'HUBSPOT_ACCESS_TOKEN manquant' };
  try {
    const company = await upsertCompany(lead);
    const contact = await upsertContact(lead);
    if (contact && company) await associateContactToCompany(contact.id, company.id);
    return { success: true, companyId: company?.id, contactId: contact?.id, action: company?._action };
  } catch (err) {
    logError(`HubSpot erreur pour "${lead.nom_entreprise}": ${err.message}`);
    return { success: false, error: err.message };
  }
}

async function sendLeadsToHubSpot(leads) {
  const stats = { created: 0, updated: 0, failed: 0, errors: [] };

  // Provision des propriétés AVANT le premier envoi
  await ensureCustomProperties();

  logInfo(`HubSpot: envoi de ${leads.length} lead(s)`);
  for (let i = 0; i < leads.length; i++) {
    try {
      const r = await sendLeadToHubSpot(leads[i]);
      if (r.success) {
        r.action === 'updated' ? stats.updated++ : stats.created++;
      } else {
        stats.failed++;
        if (r.error) stats.errors.push(`${leads[i].nom_entreprise}: ${r.error}`);
      }
    } catch (err) {
      stats.failed++;
      stats.errors.push(`${leads[i].nom_entreprise}: ${err.message}`);
    }
    if (i < leads.length - 1) await new Promise(r => setTimeout(r, 150));
  }

  logInfo('HubSpot batch terminé', stats);
  return stats;
}

async function checkConnection() {
  if (!ACCESS_TOKEN) return { connected: false, message: 'Token manquant dans .env' };
  try {
    await hsRequest('GET', '/crm/v3/objects/companies?limit=1');
    return { connected: true, message: 'Connexion OK' };
  } catch (err) {
    return { connected: false, message: err.message };
  }
}

module.exports = { sendLeadToHubSpot, sendLeadsToHubSpot, checkConnection };
