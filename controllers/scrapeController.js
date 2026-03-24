// controllers/scrapeController.js
const legacyScraper = require('../services/legacyScraper');
const hubspotService = require('../services/hubspotService');
const scrapingStore = require('../services/scrapingStore');
const { v4: uuidv4 } = require('uuid');

// ── Statut global en mémoire (simple flag, pas critique) ─────────
let globalScrapingActive = false;

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
      const pos = await scrapingStore.getQueuePosition(queueId);
      if (pos === -1) {
        // queueId inconnu (expiré ou annulé) : refuser
        return res.status(409).json({ error: 'queueId inconnu ou expiré' });
      }
      if (pos !== 0 || globalScrapingActive) {
        // Pas encore son tour : renvoyer sa position actuelle
        return res.json({ queued: true, position: pos + 1, queueId });
      }
      // C'est son tour ET le serveur est libre : retirer de la queue et continuer
      await scrapingStore.removeFromQueue(queueId);
    } else if (globalScrapingActive) {
      // Nouvelle demande alors qu'un scraping est actif : mettre en file
      const userEmail = req.session.userEmail;
      const queued = await scrapingStore.enqueue(
        { keyword, source, enableEnrichment, enableN8nSending, oneByOneProcessing, departments, sheetId, destGoogleSheets, destHubSpot },
        userEmail
      );
      console.log(`📋 Scraping en file d'attente — position ${queued.position} (queueId: ${queued.queueId})`);
      return res.json({ queued: true, position: queued.position, queueId: queued.queueId });
    }

    // ── DÉMARRAGE ASYNCHRONE ───────────────────────────────────────────
    globalScrapingActive = true;

    const batchId = uuidv4();
    const userEmail = req.session.userEmail;
    const startTime = new Date();
    
    // Créer le batch en base de données
    await scrapingStore.createBatch(batchId, {
      keyword,
      sources,
      departments,
      startTime,
      enrichmentEnabled: enableEnrichment,
      n8nEnabled: enableN8nSending && destGoogleSheets,
      oneByOneProcessing,
      destGoogleSheets,
      destHubSpot,
      crawlBatchId: batchId,
      sheetId,
      userEmail
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
        
        // Mettre à jour le batch en base
        const updates = {
          is_running: false,
          completed: true,
          leads_count: results.successful,
          cancelled: !!results.cancelled,
          stats: this.calculateStats(leads)
        };
        
        if (results.cancelled) {
          updates.error = 'Scraping arrêté par l\'utilisateur';
        }

        if (sheetId) {
          updates.spreadsheet_url = `https://docs.google.com/spreadsheets/d/${sheetId}/edit`;
        }

        await scrapingStore.updateBatch(batchId, updates);
        
        console.log(`✅ Batch ${batchId} terminé: ${results.successful} leads traités`);

      } catch (error) {
        console.error(`❌ Batch ${batchId} erreur:`, error.message);
        
        await scrapingStore.updateBatch(batchId, {
          is_running: false,
          completed: true,
          error: error.message,
          cancelled: false
        });
      } finally {
        globalScrapingActive = false;
        const nextQueueId = await scrapingStore.dequeueNext();
        if (nextQueueId) {
          console.log(`📋 Prochain scraping prêt: ${nextQueueId}`);
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

    try {
      const status = await scrapingStore.getBatch(batchId);

      if (!status) {
        return res.status(404).json({ error: 'Batch non trouvé' });
      }

      // Réponse légère avec seulement les infos utiles
      res.json({
        batchId: status.batchId,
        isRunning: status.isRunning,
        completed: status.completed,
        cancelled: status.cancelled,
        error: status.error,
        leadsCount: status.leadsCount,
        stats: status.stats || null,
        startTime: status.startTime,
        spreadsheetUrl: status.spreadsheetUrl || null,
        keyword: status.keyword,
        source: status.sources
      });
    } catch (error) {
      console.error('❌ Erreur getBatchStatus:', error.message);
      res.status(500).json({ error: 'Erreur serveur' });
    }
  }

  // ── MÉTHODE LEGACY: Pour compatibilité avec l'ancien frontend ────
  async getScrapingStatus(req, res) {
    try {
      const userEmail = req.session.userEmail;
      
      // Chercher le dernier batch actif ou le plus récent
      const batches = await scrapingStore.getAllBatches(userEmail);
      
      // Chercher un batch en cours
      let activeBatch = batches.find(b => b.isRunning);
      
      // Sinon prendre le plus récent complété
      if (!activeBatch && batches.length > 0) {
        activeBatch = batches[0];
      }

      // Fallback sur l'ancien format pour compatibilité
      const status = activeBatch || req.session.scrapingStatus || {
        isRunning: false,
        leadsCount: 0,
        spreadsheetUrl: null
      };

      res.json(status);
    } catch (error) {
      console.error('❌ Erreur getScrapingStatus:', error.message);
      // Fallback sur l'ancien comportement
      res.json(req.session.scrapingStatus || { isRunning: false, leadsCount: 0, spreadsheetUrl: null });
    }
  }

  async resetScrapingStatus(req, res) {
    // Note: On ne peut pas vraiment "réinitialiser" la base, mais on peut
    // arrêter tous les batches en cours pour cet utilisateur
    req.session.scrapingStatus = { isRunning: false, leadsCount: 0, spreadsheetUrl: null };
    res.json({ success: true, message: 'All scraping statuses reset' });
  }

  // ── MODIFIÉ: Stop scraping avec batchId ──────────────────────────
  async stopScraping(req, res) {
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

    try {
      const status = await scrapingStore.getBatch(batchId);

      if (!status || !status.crawlBatchId) {
        return res.status(404).json({ error: 'Batch non trouvé ou déjà terminé' });
      }

      console.log(`🛑 Stopping scrape batch: ${batchId}`);
      if (typeof legacyScraper.cancelScrape === 'function') {
        legacyScraper.cancelScrape(status.crawlBatchId);
      }

      // Mettre à jour le statut en base
      await scrapingStore.updateBatch(batchId, {
        isRunning: false,
        cancelled: true,
        error: 'Scraping arrêté par l\'utilisateur'
      });

      res.json({ success: true, message: 'Stop signal sent' });
    } catch (error) {
      console.error('❌ Erreur stopScraping:', error.message);
      res.status(500).json({ error: 'Erreur serveur' });
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

    try {
      const pos = await scrapingStore.getQueuePosition(queueId);
      if (pos === -1) {
        return res.json({ status: 'not_found' });
      }
      
      const qItem = await scrapingStore.getQueueItem(queueId);
      
      // Si c'est notre tour et que personne d'autre ne tourne
      if (qItem && qItem.status === 'ready' && !globalScrapingActive) {
        return res.json({ status: 'ready' });
      }

      return res.json({ status: 'waiting', position: pos + 1 });
    } catch (error) {
      console.error('❌ Erreur getQueueStatus:', error.message);
      res.status(500).json({ error: 'Erreur serveur' });
    }
  }

  async removeQueue(req, res) {
    const { queueId } = req.body;
    if (!queueId) return res.status(400).json({ error: 'Missing queueId' });
    
    try {
      await scrapingStore.removeFromQueue(queueId);
      console.log(`📋 Annulation file d'attente pour queueId: ${queueId}`);
      res.json({ success: true });
    } catch (error) {
      console.error('❌ Erreur removeQueue:', error.message);
      res.status(500).json({ error: 'Erreur serveur' });
    }
  }

  // ── IMPORT APIFY ─────────────────────────────────────────────────
  /**
   * POST /scrape/apify-import
   * Body: { records: [...], destGoogleSheets, destHubSpot, sheetId? }
   * Accepte le JSON brut Apify (tableau d'objets ou objet avec une propriété items/results).
   */
  async startApifyImport(req, res) {
    let { records, destGoogleSheets = true, destHubSpot = false, sheetId = null } = req.body;

    // Tolérance : si le JSON est un objet wrapper { items: [...] } ou { results: [...] }
    if (records && !Array.isArray(records)) {
      records = records.items || records.results || records.data || null;
    }

    if (!Array.isArray(records) || records.length === 0) {
      return res.status(400).json({ error: 'Le champ "records" doit être un tableau non vide.' });
    }
    if (!destGoogleSheets && !destHubSpot) {
      return res.status(400).json({ error: 'Veuillez sélectionner au moins une destination.' });
    }
    if (records.length > 5000) {
      return res.status(400).json({ error: 'Maximum 5000 enregistrements par import.' });
    }

    const batchId    = uuidv4();
    const userEmail  = req.session?.userEmail;
    const startTime  = new Date();

    // Créer le batch en base (même structure que startScraping)
    await scrapingStore.createBatch(batchId, {
      keyword:            `apify_import_${records.length}`,
      sources:            ['apify_google_maps'],
      departments:        [],
      startTime,
      enrichmentEnabled:  true,
      n8nEnabled:         destGoogleSheets,
      oneByOneProcessing: true,
      destGoogleSheets,
      destHubSpot,
      crawlBatchId:       batchId,
      sheetId,
      userEmail
    });

    globalScrapingActive = true;

    // Traitement asynchrone — ne bloque pas la réponse HTTP
    (async () => {
      try {
        const results = await legacyScraper.processApifyData(records, {
          enableN8nSending: destGoogleSheets,
          enableHubSpot:    destHubSpot,
          sheetId,
          crawlBatchId:     batchId
        });

        const leads   = results.leads || [];
        const updates = {
          is_running:  false,
          completed:   true,
          leads_count: results.successful,
          cancelled:   !!results.cancelled,
          stats:       this.calculateStats(leads)
        };
        if (results.cancelled) updates.error = 'Import arrêté par l\'utilisateur';
        if (sheetId) updates.spreadsheet_url = `https://docs.google.com/spreadsheets/d/${sheetId}/edit`;

        await scrapingStore.updateBatch(batchId, updates);
        console.log(`✅ Import Apify ${batchId} terminé: ${results.successful} leads`);

      } catch (err) {
        console.error(`❌ Import Apify ${batchId} erreur:`, err.message);
        await scrapingStore.updateBatch(batchId, {
          is_running: false, completed: true, error: err.message, cancelled: false
        });
      } finally {
        globalScrapingActive = false;
        const next = await scrapingStore.dequeueNext();
        if (next) console.log(`📋 Prochain scraping prêt: ${next}`);
      }
    })();

    return res.json({ success: true, message: 'Import Apify démarré en arrière-plan', batchId });
  }
  async getMonitoringData(req, res) {
    try {
      const userEmail = req.session.userEmail;
      const userRole = req.session.userRole;
      
      // Récupérer les batches : tous pour admin, seulement ceux de l'utilisateur pour user
      const batches = (userRole === 'admin')
        ? await scrapingStore.getAllBatches()
        : await scrapingStore.getAllBatches(userEmail);
      
      // Informations sur la file d'attente (admin voit tout, user voit seulement ses entrées)
      let queue = await scrapingStore.listQueue();
      if (userRole !== 'admin') {
        queue = queue.filter(item => item.userEmail === userEmail);
      }
      
      const formattedQueue = queue.map(item => ({
        queueId: item.queueId,
        status: item.status,
        enqueuedAt: item.enqueuedAt,
        keyword: item.params?.keyword,
        source: item.params?.source,
        departments: item.params?.departments
      }));

      res.json({
        success: true,
        globalScrapingActive,
        queue: formattedQueue,
        batches,
        userRole
      });
    } catch (error) {
      console.error('❌ Erreur getMonitoringData:', error.message);
      res.status(500).json({ success: false, error: 'Erreur serveur' });
    }
  }
}

module.exports = new ScrapeController();
