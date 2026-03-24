const express = require('express');
const router = express.Router();
const scrapeController = require('../controllers/scrapeController');
const { logger } = require('../utils/logger');
const axios = require('axios');

// Middleware to check authentication
function isAuthenticated(req, res, next) {
  if (req.session.isAuthenticated) {
    return next();
  }
  res.redirect('/login');
}

// Middleware to check if user is admin
function isAdmin(req, res, next) {
  if (req.session.isAuthenticated && req.session.userRole === 'admin') {
    return next();
  }
  res.status(403).render('error', { 
    error: 'Accès interdit. Vous devez être administrateur.',
    user: { email: req.session.userEmail }
  });
}

// Middleware to check authentication for API endpoints
function isAuthenticatedAPI(req, res, next) {
  if (req.session.isAuthenticated) {
    return next();
  }
  res.status(401).json({ error: 'Authentication required' });
}

// Scrape page (protected route)
router.get('/', isAuthenticated, (req, res) => {
  res.render('scrape', { 
    user: { email: req.session.userEmail, role: req.session.userRole },
    scrapingStatus: req.session.scrapingStatus || null
  });
});

// Start scraping endpoint
router.post('/start', isAuthenticatedAPI, (req, res) => {
  scrapeController.startScraping(req, res);
});

// Stop scraping endpoint
router.post('/stop', isAuthenticatedAPI, (req, res) => {
  scrapeController.stopScraping(req, res);
});

// Test n8n webhook endpoint
router.post('/test-webhook', isAuthenticatedAPI, (req, res) => {
  scrapeController.testN8nWebhook(req, res);
});

// Get scraping status (legacy - dernier batch)
router.get('/status', isAuthenticatedAPI, (req, res) => {
  scrapeController.getScrapingStatus(req, res);
});

// Get batch status (nouveau - batch spécifique)
router.get('/status/:batchId', isAuthenticatedAPI, (req, res) => {
  scrapeController.getBatchStatus(req, res);
});

// Reset scraping status
router.post('/reset', isAuthenticatedAPI, (req, res) => {
  scrapeController.resetScrapingStatus(req, res);
});

// Get logs
router.get('/logs', isAuthenticatedAPI, (req, res) => {
  const since = req.query.since;
  const logs = logger.getLogs(since);
  res.json({ logs });
});
// create sheet via webhook and return the sheet ID
router.post("/create-sheet", isAuthenticatedAPI, async (req, res) => {
  try {
    const { keyword, userEmail } = req.body || {};
    const currentUserEmail = req.session.userEmail || userEmail || null;

    const payload = {
      keyword: keyword || null,
      userEmail: currentUserEmail
    };

    const response = await axios.post(
      "https://n8n.trouvezpourmoi.com/webhook/522ca5c1-15be-44cb-abdd-d9636a503015",
      payload
    );

    const spreadsheetId = response.data.spreadsheetId;

    return res.json({
      spreadsheetId: spreadsheetId
    });

  } catch (error) {
    console.error("Webhook error:", error.message);

    res.status(500).json({
      error: "Impossible de créer le Google Sheet"
    });
  }
});
// Healthcheck HubSpot
router.get('/check-hubspot', isAuthenticatedAPI, (req, res) => {
  scrapeController.checkHubSpot(req, res);
});

// Statut de la file d'attente
router.get('/queue-status', isAuthenticatedAPI, (req, res) => {
  scrapeController.getQueueStatus(req, res);
});

// Annuler une file d'attente
router.post('/queue-cancel', isAuthenticatedAPI, (req, res) => {
  scrapeController.removeQueue(req, res);
});

// API de monitoring (données JSON) - réservée aux admins
router.get('/monitoring', isAuthenticatedAPI, isAdmin, (req, res) => {
  scrapeController.getMonitoringData(req, res);
});

// Page de monitoring (vue EJS) - réservée aux admins
router.get('/monitoring-view', isAuthenticated, isAdmin, (req, res) => {
  res.render('monitoring', { user: { email: req.session.userEmail, role: req.session.userRole } });
});

// routes/scrape.js — ajouter cette ligne avec les autres routes POST
router.post('/apify-import', scrapeController.startApifyImport.bind(scrapeController));

module.exports = router;

