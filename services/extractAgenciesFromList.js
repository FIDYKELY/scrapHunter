/**
 * Extrait les agences directement depuis la liste des résultats PagesJaunes
 */
async function extractAgenciesFromList(page) {
  return await page.evaluate(() => {
    const agencies = [];
    
    // Sélecteurs PagesJaunes 2024 - articles/cards
    const agencyBlocks = document.querySelectorAll('article, [class*="card"], [class*="bi-"], .result-item, [data-testid*="listing"]');
    
    agencyBlocks.forEach((block) => {
      // Ignorer les pubs et non-agences
      if (block.classList.contains('ad') || 
          block.classList.contains('sponsored') ||
          block.textContent.toLowerCase().includes('sécurité')) {
        return;
      }
      
      // Nom de l'agence
      const nameEl = block.querySelector('h2, h3, a[href*="/pros/"]');
      let name = null;
      if (nameEl) {
        name = nameEl.textContent.trim();
        // Si c'est un lien, prendre le texte du lien
        if (nameEl.tagName === 'A' && nameEl.href) {
          name = nameEl.textContent.trim();
        }
      }
      
      // Filtrer: doit contenir "immobil" ou être dans le bon contexte
      if (!name || (!name.toLowerCase().includes('immobil') && 
                   !name.toLowerCase().includes('agence') && 
                   !name.toLowerCase().includes('real') && 
                   !name.toLowerCase().includes('property'))) {
        return;
      }
      
      // Téléphone
      const phoneEl = block.querySelector('[class*="tel"], [class*="phone"], a[href^="tel:"]');
      let phone = null;
      if (phoneEl) {
        phone = phoneEl.textContent.trim();
        // Si c'est un lien tel, extraire le numéro
        if (phoneEl.href && phoneEl.href.startsWith('tel:')) {
          phone = phoneEl.href.replace('tel:', '');
        }
      }
      
      // Site web
      const websiteEl = block.querySelector('a[href*="http"]:not([href*="pagesjaunes"])');
      const website = websiteEl ? websiteEl.href : null;
      
      // Email
      const emailEl = block.querySelector('a[href^="mailto:"]');
      const email = emailEl ? emailEl.href.replace('mailto:', '') : null;
      
      // URL de la fiche détaillée
      const detailLink = block.querySelector('a[href*="/pros/"]');
      const detailUrl = detailLink ? detailLink.href : null;
      
      agencies.push({
        name,
        phone,
        website,
        email,
        detailUrl,
        source: 'pagesjaunes'
      });
    });
    
    return agencies;
  });
}

module.exports = {
  extractAgenciesFromList
};
