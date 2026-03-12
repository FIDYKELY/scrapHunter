// services/hubspotService.js
// Stratégie : on essaie d'abord avec toutes les propriétés custom.
// Si HubSpot répond 400 PROPERTY_DOESNT_EXIST, on retente avec les champs
// natifs uniquement (name, city, phone, website…) — ça fonctionne toujours.
// Les propriétés custom sont stockées dans une note HubSpot en fallback.

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

function isPropertyError(err) {
  return err.message.includes('PROPERTY_DOESNT_EXIST');
}

// ─────────────────────────────────────────────
// PROPRIÉTÉS CUSTOM CONNUES
// Liste des noms qu'on essaie d'envoyer mais qui peuvent ne pas exister.
// ─────────────────────────────────────────────
const CUSTOM_PROPS = new Set([
  'lead_departement', 'lead_source', 'lead_type_profil', 'lead_score',
  'lead_priorite', 'linkedin_company_page', 'facebook_company_page',
  'instagram_company_page', 'external_lead_id', 'crawl_batch_id',
  'google_rating', 'google_reviews_count',
  'siret', 'siren', 'statut_juridique', 'societe_url'
]);

// Retourne un objet sans les propriétés custom
function stripCustomProps(props) {
  return Object.fromEntries(
    Object.entries(props).filter(([k]) => !CUSTOM_PROPS.has(k))
  );
}

// Formate les propriétés custom en texte pour une note HubSpot
function buildNoteText(lead) {
  const lines = ['📊 Données scrapHunter'];
  if (lead.source)               lines.push(`Source: ${lead.source}`);
  if (lead.departement)          lines.push(`Département: ${lead.departement}`);
  if (lead.score_global != null) lines.push(`Score: ${lead.score_global}/100`);
  if (lead.priorite)             lines.push(`Priorité: ${lead.priorite}`);
  if (lead.type_profil)          lines.push(`Profil: ${lead.type_profil}`);
  if (lead.lead_id)              lines.push(`ID: ${lead.lead_id}`);
  if (lead.crawl_batch_id)       lines.push(`Batch: ${lead.crawl_batch_id}`);
  if (lead.google_rating)        lines.push(`Note Google: ${lead.google_rating}⭐`);
  if (lead.linkedin_company_url) lines.push(`LinkedIn: ${lead.linkedin_company_url}`);
  if (lead.facebook_url)         lines.push(`Facebook: ${lead.facebook_url}`);
  if (lead.instagram_url)        lines.push(`Instagram: ${lead.instagram_url}`);
  if (lead.siret)                lines.push(`SIRET: ${lead.siret}`);
  if (lead.siren)                lines.push(`SIREN: ${lead.siren}`);
  if (lead.statut_juridique)     lines.push(`Statut INSEE: ${lead.statut_juridique}`);
  if (lead.societe_url)          lines.push(`Fiche INSEE: ${lead.societe_url}`);
  return lines.join('\n');
}

// ─────────────────────────────────────────────
// SANITISATION DES DONNÉES SCRAPÉES
// ─────────────────────────────────────────────

const FAKE_EMAIL_EXTENSIONS = /\.(webp|png|jpg|jpeg|gif|svg|ico|pdf|css|js|woff|ttf|frnum|htm|html|php|asp)$/i;
const VALID_EMAIL_RE = /^[^\s@]+@[^\s@]+\.[a-zA-Z]{2,6}$/;

function sanitizeEmail(raw) {
  if (!raw || typeof raw !== 'string') return null;
  const email = raw.trim().toLowerCase();
  if (!VALID_EMAIL_RE.test(email))       return null;
  if (FAKE_EMAIL_EXTENSIONS.test(email)) return null;
  const domain = email.split('@')[1] || '';
  if (/^\d+x\d+/.test(domain))         return null;
  return email;
}

function sanitizeName(raw) {
  if (!raw || typeof raw !== 'string') return 'Agence inconnue';
  const firstLine = raw
    .split(/\n/)
    .map(l => l.trim())
    .find(l => l.length > 0);
  return firstLine || 'Agence inconnue';
}

// ─────────────────────────────────────────────
// MAPPING lead → propriétés HubSpot
// ─────────────────────────────────────────────
function buildCompanyProps(lead) {
  const props = {
    name:    sanitizeName(lead.nom_entreprise),
    city:    lead.ville            || '',
    zip:     lead.code_postal      || '',
    country: 'France',
    address: lead.adresse_complete || '',
    phone:   lead.telephone        || '',
    website: lead.site_web         || ''
  };
  // Propriétés custom (peuvent échouer si non créées dans le portail)
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
  if (lead.siret)                props.siret                  = lead.siret;
  if (lead.siren)                props.siren                  = lead.siren;
  if (lead.statut_juridique)     props.statut_juridique        = lead.statut_juridique;
  if (lead.societe_url)          props.societe_url             = lead.societe_url;
  return props;
}

function buildContactProps(lead) {
  const props = {
    firstname:      sanitizeName(lead.nom_entreprise) || 'Contact',
    lastname:       '',
    company:        sanitizeName(lead.nom_entreprise) || '',
    phone:          lead.telephone      || '',
    city:           lead.ville          || '',
    zip:            lead.code_postal    || '',
    country:        'France',
    hs_lead_status: 'NEW'
  };
  const cleanEmail = sanitizeEmail(lead.email);
  if (cleanEmail)                props.email            = cleanEmail;
  else if (lead.email)           logWarning(`HubSpot: email invalide ignoré pour ${sanitizeName(lead.nom_entreprise)}: ${lead.email}`);
  // Propriétés custom contact
  if (lead.lead_id)              props.external_lead_id = lead.lead_id;
  if (lead.score_global != null) props.lead_score       = String(lead.score_global);
  if (lead.priorite)             props.lead_priorite    = lead.priorite;
  if (lead.source)               props.lead_source      = lead.source;
  return props;
}

// ─────────────────────────────────────────────
// CRÉATION AVEC FALLBACK AUTOMATIQUE
// Tente avec toutes les props → si PROPERTY_DOESNT_EXIST → retente sans custom
// ─────────────────────────────────────────────
async function createWithFallback(objectType, properties, lead) {
  try {
    return await hsRequest('POST', `/crm/v3/objects/${objectType}`, { properties });
  } catch (err) {
    if (!isPropertyError(err)) throw err; // autre erreur → on remonte

    logWarning(`HubSpot: propriétés custom inconnues pour ${objectType}, envoi en mode dégradé`);
    const nativeOnly = stripCustomProps(properties);
    const obj = await hsRequest('POST', `/crm/v3/objects/${objectType}`, { properties: nativeOnly });

    // Stocker les données custom dans une note associée à la company
    if (objectType === 'companies') {
      await createNoteForCompany(obj.id, buildNoteText(lead)).catch(e =>
        logWarning(`HubSpot: impossible de créer la note — ${e.message}`)
      );
    }
    return { ...obj, _degraded: true };
  }
}

async function updateWithFallback(objectType, id, properties) {
  try {
    return await hsRequest('PATCH', `/crm/v3/objects/${objectType}/${id}`, { properties });
  } catch (err) {
    if (!isPropertyError(err)) throw err;
    logWarning(`HubSpot: propriétés custom inconnues pour PATCH ${objectType}, envoi natif seulement`);
    const nativeOnly = stripCustomProps(properties);
    return await hsRequest('PATCH', `/crm/v3/objects/${objectType}/${id}`, { properties: nativeOnly });
  }
}

// ─────────────────────────────────────────────
// NOTE DE FALLBACK (données custom en texte)
// ─────────────────────────────────────────────
async function createNoteForCompany(companyId, noteBody) {
  const note = await hsRequest('POST', '/crm/v3/objects/notes', {
    properties: {
      hs_note_body:      noteBody,
      hs_timestamp:      String(Date.now())
    }
  });
  // Associer la note à la company
  await hsRequest('PUT',
    `/crm/v3/objects/notes/${note.id}/associations/companies/${companyId}/note_to_company`,
    {}
  );
  logInfo(`HubSpot: note créée pour company ${companyId}`);
}

// ─────────────────────────────────────────────
// DÉDUPLICATION
// ─────────────────────────────────────────────
async function findExistingCompany(lead) {
  try {
    // Cherche par nom + ville (external_lead_id peut ne pas exister comme propriété)
    if (lead.nom_entreprise) {
      const r = await hsRequest('POST', '/crm/v3/objects/companies/search', {
        filterGroups: [{ filters: [
          { propertyName: 'name', operator: 'EQ', value: sanitizeName(lead.nom_entreprise) },
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
    const r = await updateWithFallback('companies', existing.id, properties);
    logInfo(`HubSpot company mise à jour: ${lead.nom_entreprise}`, { id: r.id });
    return { ...r, _action: 'updated' };
  }
  const r = await createWithFallback('companies', properties, lead);
  logInfo(`HubSpot company créée${r._degraded ? ' (mode dégradé)' : ''}: ${lead.nom_entreprise}`, { id: r.id });
  return { ...r, _action: 'created' };
}

async function upsertContact(lead) {
  if (!lead.email && !lead.telephone) return null;

  const properties = buildContactProps(lead);
  const existing   = await findExistingContact(lead.email);

  if (existing) {
    const r = await updateWithFallback('contacts', existing.id, properties);
    return { ...r, _action: 'updated' };
  }
  const r = await createWithFallback('contacts', properties, lead);
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
    return {
      success:   true,
      companyId: company?.id,
      contactId: contact?.id,
      action:    company?._action,
      degraded:  !!company?._degraded
    };
  } catch (err) {
    logError(`HubSpot erreur pour "${lead.nom_entreprise}": ${err.message}`);
    return { success: false, error: err.message };
  }
}

async function sendLeadsToHubSpot(leads) {
  const stats = { created: 0, updated: 0, failed: 0, degraded: 0, errors: [] };

  logInfo(`HubSpot: envoi de ${leads.length} lead(s)`);
  for (let i = 0; i < leads.length; i++) {
    try {
      const r = await sendLeadToHubSpot(leads[i]);
      if (r.success) {
        r.action === 'updated' ? stats.updated++ : stats.created++;
        if (r.degraded) stats.degraded++;
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

  if (stats.degraded > 0) {
    logWarning(`HubSpot: ${stats.degraded} lead(s) envoyé(s) en mode dégradé (propriétés custom manquantes). Pour les activer, voir README.`);
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
