/**
 * Normalise un numéro de téléphone français pour comparaison et Google Sheets.
 * - enlève tous les caractères non numériques
 * - convertit les préfixes 00, +33, 033 vers 0
 * - formatage compatible Google Sheets (pas de formules)
 */
function normalizePhone(phone) {
  if (!phone) return '';
  
  // Garder uniquement les chiffres et le plus pour les formats internationaux
  let digits = phone.replace(/[^\d+]/g, '');
  
  // Gérer tous les formats français
  digits = digits
    .replace(/^00?33/, '0')     // 0033... → 0
    .replace(/^\+33/, '0')      // +33... → 0
    .replace(/^330/, '0');      // 330... → 0 (cas particulier)
  
  // Si commence par 33 et fait 11 chiffres (33123456789 → 0123456789)
  if (digits.startsWith('33') && digits.length === 11) {
    digits = '0' + digits.slice(2);
  }
  
  // Normaliser les numéros DOM-TOM (696, 694, 697...)
  // et s'assurer qu'on a 10 chiffres pour un fixe standard
  if (digits.length === 10 && digits.startsWith('0')) {
    return digits; // Format standard 0XXXXXXXXX
  }
  
  // Si on a 9 chiffres (manque le 0 initial)
  if (digits.length === 9 && !digits.startsWith('0')) {
    return '0' + digits;
  }
  
  // Si le résultat commence par un caractère qui pourrait être interprété comme une formule
  // on le préfixe avec une apostrophe pour forcer le texte dans Google Sheets
  if (digits && (digits.startsWith('=') || digits.startsWith('+') || digits.startsWith('-'))) {
    return "'" + digits;
  }
  
  return digits;
}

/**
 * Formate un numéro de téléphone pour l'affichage dans Google Sheets
 * Évite les erreurs de formule en forçant le format texte
 */
function formatPhoneForSheets(phone) {
  if (!phone) return '';
  
  // Normaliser d'abord
  const normalized = normalizePhone(phone);
  if (!normalized) return '';
  
  // Ajouter une apostrophe au début pour forcer le format texte dans Google Sheets
  // Cela évite que Google Sheets n'interprète les numéros comme des formules
  if (normalized.startsWith('0') || normalized.match(/^\d+$/)) {
    return "'" + normalized;
  }
  
  // Si le numéro contient déjà des caractères spéciaux, le préfixer aussi
  return "'" + normalized;
}

/**
 * Normalise un domaine de site web (sans schéma, sans www, en minuscule).
 */
function normalizeDomain(url) {
  if (!url || url === '#') return '';
  
  try {
    // Nettoyer l'URL
    let cleanUrl = url.trim();
    
    // Enlever les paramètres de tracking courants
    cleanUrl = cleanUrl.split('?')[0].split('#')[0];
    
    const withProtocol = cleanUrl.startsWith('http') ? cleanUrl : `https://${cleanUrl}`;
    const u = new URL(withProtocol);
    let host = u.hostname.toLowerCase();
    
    // Enlever www et sous-domaines courants
    host = host
      .replace(/^www\./, '')
      .replace(/^m\./, '')      // Version mobile
      .replace(/^mobile\./, '');
    
    return host;
  } catch {
    // Fallback basique
    return url.toLowerCase()
      .replace(/^https?:\/\//, '')
      .replace(/^www\./, '')
      .replace(/^m\./, '')
      .split('/')[0]
      .split('?')[0];
  }
}

/**
 * Normalise "nom + ville" pour comparer les doublons.
 */
function normalizeNameCity(name, city) {
  if (!name && !city) return '';
  
  const base = `${name || ''} ${city || ''}`.trim().toLowerCase();
  
  return base
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')  // Enlève les accents
    .replace(/[^a-z0-9]+/g, ' ')      // Remplace les non-alphanum par espace
    .replace(/\b(le|la|les|du|de|d'|l')\b/g, '') // Enlève les articles
    .replace(/\s+/g, ' ')             // Unifie les espaces
    .trim();
}

/**
 * Normalise une adresse email basique : enlève `mailto:`, paramètres, met en minuscule et trim.
 */
function normalizeEmail(email) {
  if (!email) return '';
  
  let e = String(email).trim()
    .replace(/^mailto:/i, '')
    .split('?')[0]  // Enlève les paramètres
    .toLowerCase();
  
  // Gérer les alias Gmail (pensez-y si vous avez beaucoup de comptes Gmail)
  if (e.includes('@gmail.com')) {
    const [local, domain] = e.split('@');
    // Enlève les points dans le local part (p.tit.nom@gmail.com → pititnom@gmail.com)
    const cleanLocal = local.replace(/\./g, '');
    e = `${cleanLocal}@${domain}`;
  }
  
  return e;
}

function extractDomain(url) {
  if (!url) return '';
  try {
    const urlObj = new URL(url.startsWith('http') ? url : `https://${url}`);
    let domain = urlObj.hostname.toLowerCase();
    domain = domain.replace(/^www\./, '');
    return domain;
  } catch {
    // Si l'URL est invalide, on tente un nettoyage basique
    return url.toLowerCase()
      .replace(/^https?:\/\//, '')
      .replace(/^www\./, '')
      .split('/')[0];
  }
}

module.exports = {
  normalizePhone,
  formatPhoneForSheets,
  normalizeDomain,
  normalizeNameCity,
  normalizeEmail,
  extractDomain
};
