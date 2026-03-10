// controllers/scrapeController.js
const legacyScraper  = require('../services/legacyScraper');
const hubspotService = require('../services/hubspotService');
const { v4: uuidv4 } = require('uuid');

class ScrapeController {

  async startScraping(req, res) {
    const {
      keyword,
      source,               // peut être string (compat) ou array
      enableEnrichment    = true,
      enableN8nSending    = true,
      oneByOneProcessing  = true,
      departments         = [],
      sheetId             = null,
      destGoogleSheets    = true,
      destHubSpot         = false
    } = req.body;

    // Normaliser source en tableau (multi-source depuis les checkboxes)
    const sources = Array.isArray(source) ? source : (source ? [source] : []);

    if (!keyword || sources.length === 0) {
      return res.status(400).json({ error: 'Keyword and at least one source are required' });
    }

    // Au moins une destination doit être cochée
    if (!destGoogleSheets && !destHubSpot) {
      return res.status(400).json({ error: 'Veuillez sélectionner au moins une destination (Google Sheets ou HubSpot).' });
    }

    try {
      console.log(`🚀 Starting scraping: keyword="${keyword}", sources="${sources.join(',')}", departments="${departments.length > 0 ? departments.join(',') : 'ALL'}"`);
      console.log(`📤 Destinations: Google Sheets=${destGoogleSheets}, HubSpot=${destHubSpot}`);

      req.session.scrapingStatus = {
        isRunning: true,
        keyword,
        source: sources,
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
      const results = await legacyScraper.mainProcess(keyword, sources, departments, {
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
        source: sources,
        enrichment:        enableEnrichment,
        n8nSending:        enableN8nSending && destGoogleSheets,
        oneByOneProcessing,
        stats,
        hubspot:           hubspotStats,
        processingTime:    Math.round((Date.now() - req.session.scrapingStatus.startTime.getTime()) / 1000)
      });

    } catch (error) {
      console.error('❌ Scraping controller error:', error.message);

      // Update session with error
      if (req.session.scrapingStatus) {
        req.session.scrapingStatus.isRunning = false;
        req.session.scrapingStatus.error = error.message;
      }

      res.status(500).json({
        success: false,
        error: error.message
      });
    }
  }

  calculateStats(leads) {
    if (!leads || leads.length === 0) {
      return {
        total: 0,
        withEmail: 0,
        withPhone: 0,
        withWebsite: 0,
        withLinkedIn: 0,
        avgScore: 0,
        priorityA: 0,
        priorityB: 0,
        priorityC: 0,
        priorityD: 0
      };
    }

    const stats = {
      total: leads.length,
      withEmail: leads.filter(l => l.email).length,
      withPhone: leads.filter(l => l.telephone).length,
      withWebsite: leads.filter(l => l.site_web).length,
      withLinkedIn: leads.filter(l => l.linkedin_company_url).length,
      avgScore: Math.round(leads.reduce((sum, l) => sum + (l.score_global || 0), 0) / leads.length),
      priorityA: leads.filter(l => l.priorite === 'A').length,
      priorityB: leads.filter(l => l.priorite === 'B').length,
      priorityC: leads.filter(l => l.priorite === 'C').length,
      priorityD: leads.filter(l => l.priorite === 'D').length
    };

    return stats;
  }

  async getScrapingStatus(req, res) {
    const status = req.session.scrapingStatus || {
      isRunning: false,
      leadsCount: 0,
      spreadsheetUrl: null
    };

    res.json(status);
  }

  resetScrapingStatus(req, res) {
    req.session.scrapingStatus = {
      isRunning: false,
      leadsCount: 0,
      spreadsheetUrl: null
    };

    res.json({ success: true, message: 'Scraping status reset' });
  }

  stopScraping(req, res) {
    if (req.session.scrapingStatus && req.session.scrapingStatus.crawlBatchId) {
      const batchId = req.session.scrapingStatus.crawlBatchId;
      console.log(`🛑 Stopping scrape batch: ${batchId}`);
      if (typeof legacyScraper.cancelScrape === 'function') {
        legacyScraper.cancelScrape(batchId);
      }
      res.json({ success: true, message: 'Stop signal sent' });
    } else {
      res.status(400).json({ error: 'No active scraping process found in session.' });
    }
  }

  // Nouvelle méthode pour tester uniquement l'envoi n8n
  async testN8nWebhook(req, res) {
    try {
      const testLead = {
        lead_id: uuidv4(),
        source: 'test',
        name: 'Test Agency',
        telephone: "'0123456789",
        email: 'test@example.com',
        address: '123 Test Street, Paris, France',
        website: 'https://example.com',
        score_global: 75,
        priorite: 'A',
        reason: 'Test lead',
        keyword: 'test'
      };

      const result = await legacyScraper.sendToN8n(testLead);

      res.json({
        success: !!result,
        message: result ? 'Test webhook sent successfully' : 'Test webhook failed',
        result: { success: !!result }
      });

    } catch (error) {
      console.error('❌ Test webhook error:', error.message);
      res.status(500).json({
        success: false,
        error: error.message
      });
    }
  }

  async checkHubSpot(req, res) {
    const status = await hubspotService.checkConnection();
    res.json(status);
  }
}

module.exports = new ScrapeController();
