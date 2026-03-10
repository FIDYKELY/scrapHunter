const legacyScraper = require('../services/legacyScraper');
const { v4: uuidv4 } = require('uuid');

class ScrapeController {
  async startScraping(req, res) {
    const { keyword, source, enableEnrichment = true, enableN8nSending = true, oneByOneProcessing = true, departments = [], sheetId = null } = req.body;
    
    if (!keyword || !source) {
      return res.status(400).json({ 
        error: 'Keyword and source are required' 
      });
    }

    try {
      console.log(`🚀 Starting scraping: keyword="${keyword}", source="${source}", departments="${departments.length > 0 ? departments.join(',') : 'ALL'}"`);
      
      // Initialize scraping status in session
      req.session.scrapingStatus = {
        isRunning: true,
        keyword,
        source,
        departments,
        startTime: new Date(),
        leadsCount: 0,
        enrichmentEnabled: enableEnrichment,
        n8nEnabled: enableN8nSending,
        oneByOneProcessing: oneByOneProcessing
      };

      // Utiliser le legacy scraper qui reproduit EXACTEMENT l'ancien système
      console.log(`🔄 Using legacy scraper for ${source}`);
      const results = await legacyScraper.mainProcess(keyword, source, departments, { sheetId });
      
      // Update session with results
      req.session.scrapingStatus.leadsCount = results.successful;
      req.session.scrapingStatus.isRunning = false;
      if (sheetId) req.session.scrapingStatus.sheetId = sheetId;

      // Calculer les statistiques
      const stats = this.calculateStats(results.leads);

      res.json({
        success: true,
        message: `Successfully scraped and processed ${results.successful} leads`,
        leadsCount: results.successful,
        keyword,
        source,
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
        averageScore: 0,
        priorityDistribution: { A: 0, B: 0, C: 0, D: 0 }
      };
    }

    const stats = {
      total: leads.length,
      withEmail: leads.filter(l => l.email).length,
      withPhone: leads.filter(l => l.telephone).length,
      withWebsite: leads.filter(l => l.site_web).length,
      withLinkedIn: leads.filter(l => l.linkedin_company_url).length,
      averageScore: Math.round(leads.reduce((sum, l) => sum + (l.score_global || 0), 0) / leads.length),
      priorityDistribution: { A: 0, B: 0, C: 0, D: 0 }
    };

    // Calculer la distribution des priorités
    leads.forEach(lead => {
      const priority = lead.priorite || 'C';
      if (stats.priorityDistribution[priority] !== undefined) {
        stats.priorityDistribution[priority]++;
      }
    });

    return stats;
  }

  async getScrapingStatus(req, res) {
    const status = req.session.scrapingStatus || {
      isRunning: false,
      leadsCount: 0
    };

    res.json(status);
  }

  resetScrapingStatus(req, res) {
    req.session.scrapingStatus = {
      isRunning: false,
      leadsCount: 0
    };

    res.json({ success: true, message: 'Scraping status reset' });
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
