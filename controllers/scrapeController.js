// controllers/scrapeController.js
const legacyScraper  = require('../services/legacyScraper');
const hubspotService = require('../services/hubspotService');
const { v4: uuidv4 } = require('uuid');

class ScrapeController {

  async startScraping(req, res) {
    const {
      keyword,
      source,
      enableEnrichment    = true,
      enableN8nSending    = true,
      oneByOneProcessing  = true,
      departments         = [],
      sheetId             = null,
      // ── nouvelles options de destination ──
      destGoogleSheets    = true,   // true = comportement actuel (webhook n8n → Google Sheets)
      destHubSpot         = false
    } = req.body;

    if (!keyword || !source) {
      return res.status(400).json({ error: 'Keyword and source are required' });
    }

    // Au moins une destination doit être cochée
    if (!destGoogleSheets && !destHubSpot) {
      return res.status(400).json({ error: 'Veuillez sélectionner au moins une destination (Google Sheets ou HubSpot).' });
    }

    try {
      console.log(`🚀 Starting scraping: keyword="${keyword}", source="${source}", departments="${departments.length > 0 ? departments.join(',') : 'ALL'}"`);
      console.log(`📤 Destinations: Google Sheets=${destGoogleSheets}, HubSpot=${destHubSpot}`);

      req.session.scrapingStatus = {
        isRunning: true,
        keyword,
        source,
        departments,
        startTime: new Date(),
        leadsCount: 0,
        enrichmentEnabled: enableEnrichment,
        n8nEnabled: enableN8nSending && destGoogleSheets,
        oneByOneProcessing,
        destGoogleSheets,
        destHubSpot
      };

      // ── SCRAPING + ENRICHISSEMENT ─────────────────────────────────────
      // On désactive l'envoi n8n interne si Google Sheets n'est pas coché
      const results = await legacyScraper.mainProcess(keyword, source, departments, {
        sheetId,
        enableN8nSending: enableN8nSending && destGoogleSheets
      });

      const leads = results.leads || [];

      req.session.scrapingStatus.leadsCount = results.successful;
      req.session.scrapingStatus.isRunning  = false;
      if (sheetId) req.session.scrapingStatus.sheetId = sheetId;

      // ── HUBSPOT ──────────────────────────────────────────────────────
      let hubspotStats = null;
      if (destHubSpot && leads.length > 0) {
        console.log(`🟠 Envoi HubSpot: ${leads.length} leads`);
        hubspotStats = await hubspotService.sendLeadsToHubSpot(leads);
        console.log(`✅ HubSpot: créés=${hubspotStats.created} mis-à-jour=${hubspotStats.updated} échoués=${hubspotStats.failed}`);
      }

      // ── STATS ─────────────────────────────────────────────────────────
      const stats = this.calculateStats(leads);

      return res.json({
        success:           true,
        message:           `${results.successful} leads traités avec succès`,
        leadsCount:        results.successful,
        keyword,
        source,
        enrichment:        enableEnrichment,
        n8nSending:        enableN8nSending && destGoogleSheets,
        oneByOneProcessing,
        stats,
        hubspot:           hubspotStats,
        processingTime:    Math.round((Date.now() - req.session.scrapingStatus.startTime.getTime()) / 1000)
      });

    } catch (error) {
      console.error('❌ Scraping controller error:', error.message);
      if (req.session.scrapingStatus) {
        req.session.scrapingStatus.isRunning = false;
        req.session.scrapingStatus.error     = error.message;
      }
      return res.status(500).json({ success: false, error: error.message });
    }
  }

  // ─────────────────────────────────────────────
  // Stats identiques à l'original
  // ─────────────────────────────────────────────
  calculateStats(leads) {
    if (!leads || leads.length === 0) {
      return {
        total: 0, withEmail: 0, withPhone: 0,
        withWebsite: 0, withLinkedIn: 0, averageScore: 0,
        priorityDistribution: { A: 0, B: 0, C: 0, D: 0 }
      };
    }

    const stats = {
      total:        leads.length,
      withEmail:    leads.filter(l => l.email).length,
      withPhone:    leads.filter(l => l.telephone).length,
      withWebsite:  leads.filter(l => l.site_web).length,
      withLinkedIn: leads.filter(l => l.linkedin_company_url).length,
      averageScore: Math.round(leads.reduce((s, l) => s + (l.score_global || 0), 0) / leads.length),
      priorityDistribution: { A: 0, B: 0, C: 0, D: 0 }
    };

    leads.forEach(lead => {
      const p = lead.priorite || 'C';
      if (stats.priorityDistribution[p] !== undefined) stats.priorityDistribution[p]++;
    });

    return stats;
  }

  // ─────────────────────────────────────────────
  // Status / Reset / Test webhook — inchangés
  // ─────────────────────────────────────────────
  async getScrapingStatus(req, res) {
    res.json(req.session.scrapingStatus || { isRunning: false, leadsCount: 0 });
  }

  resetScrapingStatus(req, res) {
    req.session.scrapingStatus = { isRunning: false, leadsCount: 0 };
    res.json({ success: true, message: 'Scraping status reset' });
  }

  async testN8nWebhook(req, res) {
    try {
      const testLead = {
        lead_id:     uuidv4(),
        source:      'test',
        name:        'Test Agency',
        telephone:   "'0123456789",
        email:       'test@example.com',
        address:     '123 Test Street, Paris, France',
        website:     'https://example.com',
        score_global: 75,
        priorite:    'A',
        reason:      'Test lead',
        keyword:     'test'
      };
      // sendToN8n est exporté depuis legacyScraper
      const result = await legacyScraper.sendToN8n(testLead);
      res.json({ success: true, message: 'Test webhook envoyé', result });
    } catch (error) {
      console.error('❌ Test webhook error:', error.message);
      res.status(500).json({ success: false, error: error.message });
    }
  }

  // ─────────────────────────────────────────────
  // Healthcheck HubSpot (optionnel, appelé depuis la route /check-hubspot)
  // ─────────────────────────────────────────────
  async checkHubSpot(req, res) {
    const status = await hubspotService.checkConnection();
    res.json(status);
  }
}

module.exports = new ScrapeController();
