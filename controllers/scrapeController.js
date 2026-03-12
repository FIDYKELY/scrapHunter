// controllers/scrapeController.js
const legacyScraper = require('../services/legacyScraper');
const hubspotService = require('../services/hubspotService');
const { v4: uuidv4 } = require('uuid');

// ── File d'attente globale (partagée sur toute l'application) ─────────
let globalScrapingActive = false;
const scrapingQueue = []; // [{ queueId, params, status: 'waiting'|'ready' }]

function enqueue(params) {
  const queueId = require('uuid').v4();
  scrapingQueue.push({ queueId, params, status: 'waiting', enqueuedAt: new Date() });
  return { queueId, position: scrapingQueue.length };
}

function dequeueNext() {
  // Marque le premier de la file comme "ready" (c'est son tour)
  if (scrapingQueue.length > 0) {
    scrapingQueue[0].status = 'ready';
  }
}

function removeFromQueue(queueId) {
  const idx = scrapingQueue.findIndex(q => q.queueId === queueId);
  if (idx !== -1) scrapingQueue.splice(idx, 1);
}

function getQueuePosition(queueId) {
  const idx = scrapingQueue.findIndex(q => q.queueId === queueId);
  return idx; // -1 = pas trouvé, 0 = premier (ready), 1+ = en attente
}

class ScrapeController {

  async startScraping(req, res) {
    const {
      keyword,
      source,               // peut être string (compat) ou array
      enableEnrichment = true,
      enableN8nSending = true,
      oneByOneProcessing = true,
      departments = [],
      sheetId = null,
      destGoogleSheets = true,
      destHubSpot = false,
      queueId = null        // présent si l'utilisateur reprend depuis la queue
    } = req.body;

    // Normaliser source en tableau
    const sources = Array.isArray(source) ? source : (source ? [source] : []);

    if (!keyword || sources.length === 0) {
      return res.status(400).json({ error: 'Keyword and at least one source are required' });
    }
    if (!destGoogleSheets && !destHubSpot) {
      return res.status(400).json({ error: 'Veuillez sélectionner au moins une destination (Google Sheets ou HubSpot).' });
    }

    // ── GESTION DE LA FILE D'ATTENTE ───────────────────────────────────
    if (queueId) {
      // Retour d'un client en file d'attente : vérifier double condition
      // 1. Il doit être en position 0 (premier de la file)
      // 2. Le scraping actif doit être libéré (globalScrapingActive === false)
      const pos = getQueuePosition(queueId);
      if (pos === -1) {
        // queueId inconnu (expiré ou annulé) : refuser
        return res.status(409).json({ error: 'queueId inconnu ou expiré' });
      }
      if (pos !== 0 || globalScrapingActive) {
        // Pas encore son tour : renvoyer sa position actuelle
        return res.json({ queued: true, position: pos + 1, queueId });
      }
      // C'est son tour ET le serveur est libre : retirer de la queue et continuer
      removeFromQueue(queueId);
    } else if (globalScrapingActive) {
      // Nouvelle demande alors qu'un scraping est actif : mettre en file
      const queued = enqueue({ keyword, source, enableEnrichment, enableN8nSending,
        oneByOneProcessing, departments, sheetId, destGoogleSheets, destHubSpot });
      console.log(`📋 Scraping en file d'attente — position ${queued.position} (queueId: ${queued.queueId})`);
      return res.json({ queued: true, position: queued.position, queueId: queued.queueId });
    }

    // ── DÉMARRAGE ──────────────────────────────────────────────────────
    globalScrapingActive = true;

    try {
      const batchId = uuidv4();
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
        destHubSpot,
        crawlBatchId: batchId // Store for cancellation
      };

      // Force session save so parallel requests (like /stop or /status) can read it
      // before the long await blocks the request completion.
      await new Promise((resolve, reject) => {
        req.session.save((err) => {
          if (err) return reject(err);
          resolve();
        });
      });

      // ── SCRAPING + ENRICHISSEMENT ─────────────────────────────────────
      // On désactive l'envoi n8n interne si Google Sheets n'est pas coché
      const results = await legacyScraper.mainProcess(keyword, sources, departments, {
        sheetId,
        enableN8nSending: enableN8nSending && destGoogleSheets,
        enableHubSpot:    destHubSpot,   // envoi immédiat lead par lead
        crawlBatchId:     batchId
      });

      const leads = results.leads || [];

      req.session.scrapingStatus.leadsCount = results.successful;
      
      // Vérifier si le processus a été annulé avant de continuer
      const isCancelled = !!results.cancelled || legacyScraper.isCancelled(batchId);
      if (isCancelled) {
        req.session.scrapingStatus.isRunning = false;
        req.session.scrapingStatus.error = 'Scraping arrêté par l\'utilisateur';
        console.log(`🛑 Processus annulé, arrêt avant envoi HubSpot`);
        
        // Libérer la variable globale et la file d'attente
        globalScrapingActive = false;
        dequeueNext();
        
        return res.json({
          success: true,
          message: `${results.successful} leads récupérés mais processus arrêté`,
          leadsCount: results.successful,
          keyword,
          source: sources,
          enrichment: enableEnrichment,
          n8nSending: false, // Pas d'envoi si annulé
          oneByOneProcessing,
          stats: this.calculateStats(leads),
          spreadsheetUrl: sheetId ? `https://docs.google.com/spreadsheets/d/${sheetId}/edit` : null,
          hubspot: null, // Pas d'envoi HubSpot si annulé
          processingTime: Math.round((Date.now() - req.session.scrapingStatus.startTime.getTime()) / 1000),
          cancelled: true // Indiquer que c'est un arrêt manuel
        });
      }
      
      req.session.scrapingStatus.isRunning = false;
      if (sheetId) req.session.scrapingStatus.sheetId = sheetId;

      // ── HUBSPOT ── (envoi déjà effectué lead par lead dans processLead)
      let hubspotStats = null;
      if (destHubSpot && leads.length > 0) {
        const sent    = leads.filter(l => l.hubspot_company_id).length;
        const skipped = leads.filter(l => l.status === 'SKIPPED_INACTIVE').length;
        const failed  = leads.filter(l => !l.hubspot_company_id && l.status !== 'SKIPPED_INACTIVE').length;
        hubspotStats  = { sent, skipped, failed, note: 'Envoi effectué en temps réel' };
        console.log(`🟠 HubSpot: ${sent} lead(s) envoyés en temps réel, ${skipped} ignorés (inactifs), ${failed} échecs`);
      }

      // ── STATS ─────────────────────────────────────────────────────────
      const stats = this.calculateStats(leads);

      // Construct spreadsheetUrl if sheetId exists
      let spreadsheetUrl = null;
      if (sheetId) {
        spreadsheetUrl = `https://docs.google.com/spreadsheets/d/${sheetId}/edit`;
        req.session.scrapingStatus.spreadsheetUrl = spreadsheetUrl;
        await new Promise((resolve) => req.session.save(() => resolve()));
      }

      return res.json({
        success: true,
        message: `${results.successful} leads traités avec succès`,
        leadsCount: results.successful,
        keyword,
        source: sources,
        enrichment: enableEnrichment,
        n8nSending: enableN8nSending && destGoogleSheets,
        oneByOneProcessing,
        stats,
        spreadsheetUrl, // Return to frontend
        hubspot: hubspotStats,
        processingTime: Math.round((Date.now() - req.session.scrapingStatus.startTime.getTime()) / 1000)
      });

    } catch (error) {
      console.error('❌ Scraping controller error:', error.message);

      if (req.session.scrapingStatus) {
        req.session.scrapingStatus.isRunning = false;
        req.session.scrapingStatus.error = error.message;
      }

      res.status(500).json({
        success: false,
        error: error.message
      });
    } finally {
      // ── LIBÉRER LA FILE D'ATTENTE ──────────────────────────────────
      globalScrapingActive = false;
      dequeueNext(); // signale au prochain qu'il peut démarrer
      if (scrapingQueue.length > 0) {
        console.log(`📋 Queue: ${scrapingQueue.length} scraping(s) en attente — prochain: ${scrapingQueue[0].queueId}`);
      }
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
      
      // Mettre à jour l'état de la session pour que le frontend détecte l'arrêt
      req.session.scrapingStatus.isRunning = false;
      req.session.scrapingStatus.error = 'Scraping arrêté par l\'utilisateur';
      
      // Libérer la variable globale pour permettre aux nouveaux scrapings de démarrer
      globalScrapingActive = false;
      
      // Forcer la sauvegarde de la session
      req.session.save((err) => {
        if (err) console.error('Erreur sauvegarde session:', err);
      });
      
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

  async getQueueStatus(req, res) {
    const { queueId } = req.query;
    if (!queueId) return res.status(400).json({ error: 'Missing queueId' });

    const pos = getQueuePosition(queueId);
    if (pos === -1) {
      return res.json({ status: 'not_found' });
    }
    const qItem = scrapingQueue.find(q => q.queueId === queueId);
    
    // Si c'est notre tour et que personne d'autre ne tourne
    if (qItem.status === 'ready' && !globalScrapingActive) {
      return res.json({ status: 'ready' });
    }

    return res.json({ status: 'waiting', position: pos + 1 });
  } // <-- AJOUT DE L'ACCOLADE MANQUANTE ICI

  async removeQueue(req, res) {
    const { queueId } = req.body;
    if (!queueId) return res.status(400).json({ error: 'Missing queueId' });
    
    removeFromQueue(queueId);
    console.log(`📋 Annulation file d'attente pour queueId: ${queueId}`);
    res.json({ success: true });
  }
}

module.exports = new ScrapeController();
