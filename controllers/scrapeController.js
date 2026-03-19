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
      maxPagesPerDept = 0,
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

    // ── DÉMARRAGE ASYNCHRONE ───────────────────────────────────────────
    globalScrapingActive = true;

    const batchId = uuidv4();
    
    // Initialiser la map des statuts si nécessaire
    req.session.scrapingStatuses = req.session.scrapingStatuses || {};
    
    // Créer le statut initial pour ce batch
    req.session.scrapingStatuses[batchId] = {
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
      crawlBatchId: batchId,
      error: null,
      completed: false,
      cancelled: false,
      stats: null,
      spreadsheetUrl: null
    };

    console.log(`🚀 Starting scraping: keyword="${keyword}", sources="${sources.join(',')}", batchId="${batchId}"`);
    console.log(`📤 Destinations: Google Sheets=${destGoogleSheets}, HubSpot=${destHubSpot}`);

    // Sauvegarder la session avant de lancer le processus en arrière-plan
    await new Promise((resolve, reject) => {
      req.session.save((err) => {
        if (err) return reject(err);
        resolve();
      });
    });

    // ── LANCER LE SCRAPING EN ARRIÈRE-PLAN ────────────────────────────
    (async () => {
      try {
        const results = await legacyScraper.mainProcess(keyword, sources, departments, {
          sheetId,
          enableN8nSending: enableN8nSending && destGoogleSheets,
          enableHubSpot: destHubSpot,
          maxPagesPerDept: maxPagesPerDept || 0,
          crawlBatchId: batchId
        });

        const leads = results.leads || [];
        const status = req.session.scrapingStatuses[batchId];
        
        if (status) {
          status.isRunning = false;
          status.completed = true;
          status.leadsCount = results.successful;
          status.cancelled = !!results.cancelled;
          
          if (results.cancelled) {
            status.error = 'Scraping arrêté par l\'utilisateur';
          }

          // Calculer les stats
          status.stats = this.calculateStats(leads);
          
          // URL du spreadsheet si disponible
          if (sheetId) {
            status.spreadsheetUrl = `https://docs.google.com/spreadsheets/d/${sheetId}/edit`;
          }
        }

        // Sauvegarder la session
        await new Promise((resolve) => req.session.save(resolve));
        
        console.log(`✅ Batch ${batchId} terminé: ${results.successful} leads traités`);

      } catch (error) {
        console.error(`❌ Batch ${batchId} erreur:`, error.message);
        
        const status = req.session.scrapingStatuses[batchId];
        if (status) {
          status.isRunning = false;
          status.completed = true;
          status.error = error.message;
          status.cancelled = false;
        }
        
        await new Promise((resolve) => req.session.save(resolve));
      } finally {
        globalScrapingActive = false;
        dequeueNext();
        if (scrapingQueue.length > 0) {
          console.log(`📋 Queue: ${scrapingQueue.length} scraping(s) en attente — prochain: ${scrapingQueue[0].queueId}`);
        }
      }
    })();

    // Réponse immédiate au client avec le batchId
    return res.json({
      success: true,
      message: 'Scraping démarré en arrière-plan',
      batchId,
      queued: false
    });
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

  // ── NOUVELLE MÉTHODE: Statut d'un batch spécifique ────────────────
  async getBatchStatus(req, res) {
    const { batchId } = req.params;
    
    if (!batchId) {
      return res.status(400).json({ error: 'batchId manquant' });
    }

    const statuses = req.session.scrapingStatuses || {};
    const status = statuses[batchId];

    if (!status) {
      return res.status(404).json({ error: 'Batch non trouvé' });
    }

    // Réponse légère avec seulement les infos utiles
    res.json({
      batchId,
      isRunning: status.isRunning,
      completed: status.completed,
      cancelled: status.cancelled,
      error: status.error,
      leadsCount: status.leadsCount,
      stats: status.stats || null,
      startTime: status.startTime,
      spreadsheetUrl: status.spreadsheetUrl || null,
      keyword: status.keyword,
      source: status.source
    });
  }

  // ── MÉTHODE LEGACY: Pour compatibilité avec l'ancien frontend ────
  async getScrapingStatus(req, res) {
    // Chercher le dernier batch actif ou le plus récent
    const statuses = req.session.scrapingStatuses || {};
    
    // Chercher un batch en cours
    let activeBatch = Object.values(statuses).find(s => s.isRunning);
    
    // Sinon prendre le plus récent complété
    if (!activeBatch) {
      const sorted = Object.values(statuses).sort((a, b) => 
        new Date(b.startTime) - new Date(a.startTime)
      );
      activeBatch = sorted[0];
    }

    // Fallback sur l'ancien format pour compatibilité
    const status = activeBatch || req.session.scrapingStatus || {
      isRunning: false,
      leadsCount: 0,
      spreadsheetUrl: null
    };

    res.json(status);
  }

  resetScrapingStatus(req, res) {
    req.session.scrapingStatuses = {};
    req.session.scrapingStatus = { isRunning: false, leadsCount: 0, spreadsheetUrl: null };
    res.json({ success: true, message: 'All scraping statuses reset' });
  }

  // ── MODIFIÉ: Stop scraping avec batchId ──────────────────────────
  stopScraping(req, res) {
    const { batchId } = req.body;

    if (!batchId) {
      // Fallback sur l'ancien comportement si pas de batchId
      if (req.session.scrapingStatus && req.session.scrapingStatus.crawlBatchId) {
        const oldBatchId = req.session.scrapingStatus.crawlBatchId;
        console.log(`🛑 Stopping scrape batch (legacy): ${oldBatchId}`);
        if (typeof legacyScraper.cancelScrape === 'function') {
          legacyScraper.cancelScrape(oldBatchId);
        }
        req.session.scrapingStatus.isRunning = false;
        req.session.scrapingStatus.error = 'Scraping arrêté par l\'utilisateur';
        req.session.save((err) => {
          if (err) console.error('Erreur sauvegarde session:', err);
        });
        return res.json({ success: true, message: 'Stop signal sent' });
      }
      return res.status(400).json({ error: 'batchId manquant' });
    }

    const statuses = req.session.scrapingStatuses || {};
    const status = statuses[batchId];

    if (!status || !status.crawlBatchId) {
      return res.status(404).json({ error: 'Batch non trouvé ou déjà terminé' });
    }

    console.log(`🛑 Stopping scrape batch: ${batchId}`);
    if (typeof legacyScraper.cancelScrape === 'function') {
      legacyScraper.cancelScrape(status.crawlBatchId);
    }

    // Mettre à jour le statut
    status.isRunning = false;
    status.cancelled = true;
    status.error = 'Scraping arrêté par l\'utilisateur';

    req.session.save((err) => {
      if (err) console.error('Erreur sauvegarde session:', err);
    });

    res.json({ success: true, message: 'Stop signal sent' });
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
