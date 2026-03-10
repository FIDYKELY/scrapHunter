const legacyScraper = require('../services/legacyScraper');
const { v4: uuidv4 } = require('uuid');

class ScrapeController {
  async startScraping(req, res) {
    const { keyword, sources, enableEnrichment = true, enableN8nSending = true, oneByOneProcessing = true, departments = [], sheetId = null } = req.body;

    if (!keyword || !sources || sources.length === 0) {
      return res.status(400).json({
        error: 'Keyword and at least one source are required'
      });
    }

    try {
      const crawlBatchId = uuidv4();
      console.log(`🚀 Starting scraping: batch="${crawlBatchId}", keyword="${keyword}", sources="${sources.join(',')}", departments="${departments.length > 0 ? departments.join(',') : 'ALL'}"`);

      // Initialize scraping status in session
      req.session.scrapingStatus = {
        isRunning: true,
        crawlBatchId,
        keyword,
        sources,
        departments,
        startTime: new Date(),
        leadsCount: 0,
        spreadsheetUrl: null,
        enrichmentEnabled: enableEnrichment,
        n8nEnabled: enableN8nSending,
        oneByOneProcessing: oneByOneProcessing
      };

      // Manually save session so concurrent /stop request can see it
      await new Promise((resolve, reject) => {
        req.session.save((err) => {
          if (err) reject(err);
          else resolve();
        });
      });

      // Utiliser le legacy scraper qui reproduit EXACTEMENT l'ancien système
      console.log(`🔄 Using legacy scraper for ${sources.join(', ')}`);
      const results = await legacyScraper.mainProcess(keyword, sources, departments, { sheetId, crawlBatchId });

      // Update session with results
      req.session.scrapingStatus.leadsCount = results.successful;
      req.session.scrapingStatus.isRunning = false;
      if (sheetId) req.session.scrapingStatus.sheetId = sheetId;

      // Calculer les statistiques
      const stats = this.calculateStats(results.leads);

      // Construct spreadsheet URL from sheetId if available
      const spreadsheetUrl = sheetId ? `https://docs.google.com/spreadsheets/d/${sheetId}` : null;
      if (spreadsheetUrl) req.session.scrapingStatus.spreadsheetUrl = spreadsheetUrl;

      res.json({
        success: true,
        message: `Successfully scraped and processed ${results.successful} leads`,
        leadsCount: results.successful,
        spreadsheetUrl: spreadsheetUrl,
        keyword,
        sources,
        enrichment: enableEnrichment,
        n8nSending: enableN8nSending,
        oneByOneProcessing: oneByOneProcessing,
        stats,
        processingTime: Math.round((Date.now() - req.session.scrapingStatus.startTime.getTime()) / 1000)
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

      const result = await realtimeLeadProcessor.sendSingleLeadToN8n(testLead);

      res.json({
        success: result.success,
        message: result.success ? 'Test webhook sent successfully' : 'Test webhook failed',
        result
      });

    } catch (error) {
      console.error('❌ Test webhook error:', error.message);
      res.status(500).json({
        success: false,
        error: error.message
      });
    }
  }
}

module.exports = new ScrapeController();
