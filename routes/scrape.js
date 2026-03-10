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
    user: { email: req.session.userEmail },
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

// Get scraping status
router.get('/status', isAuthenticatedAPI, (req, res) => {
  scrapeController.getScrapingStatus(req, res);
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
    const response = await axios.post(
      "https://n8n.trouvezpourmoi.com/webhook/522ca5c1-15be-44cb-abdd-d9636a503015"
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

module.exports = router;

