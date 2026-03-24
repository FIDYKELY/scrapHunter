const express = require('express');
const session = require('express-session');
const path = require('path');
require('dotenv').config();

const authRoutes = require('./routes/auth');
const scrapeRoutes = require('./routes/scrape');

const app = express();
const PORT = process.env.PORT || 3000;

// Middleware
// Limite augmentée à 50mb pour supporter les imports JSON Apify volumineux
app.use(express.urlencoded({ extended: true, limit: '50mb' }));
app.use(express.json({ limit: '50mb' }));
app.use(express.static(path.join(__dirname, 'public')));

// Session configuration
app.use(session({
  secret: process.env.SESSION_SECRET || 'fallback-secret-change-this',
  resave: false,
  saveUninitialized: false,
  cookie: { 
    secure: false, // Set to true in production with HTTPS
    maxAge: 24 * 60 * 60 * 1000 // 24 hours
  }
}));

// View engine setup
app.set('view engine', 'ejs');
app.set('views', path.join(__dirname, 'views'));

// Routes
app.use('/', authRoutes);
app.use('/scrape', scrapeRoutes);

// Home route - redirect to login
app.get('/', (req, res) => {
  if (req.session.isAuthenticated) {
    return res.redirect('/scrape');
  }
  res.redirect('/login');
});

// ── Error handling middleware ──────────────────────────────────────────────
// Détecte si la requête est une API (Accept: application/json ou chemin /scrape/*)
// → répond en JSON pour éviter que le frontend reçoive du HTML et crash avec
//   "Unexpected token '<'"
app.use((err, req, res, next) => {
  console.error(err.stack);

  const isApiRequest =
    req.path.startsWith('/scrape/') ||
    (req.headers['accept'] || '').includes('application/json') ||
    (req.headers['content-type'] || '').includes('application/json');

  // PayloadTooLargeError → message explicite en JSON
  if (err.type === 'entity.too.large' || err.status === 413) {
    if (isApiRequest) {
      return res.status(413).json({
        error: 'Fichier trop volumineux. Maximum 50 Mo acceptés.',
        code: 'PAYLOAD_TOO_LARGE'
      });
    }
    return res.status(413).send('Payload too large');
  }

  if (isApiRequest) {
    return res.status(500).json({
      error: err.message || 'Erreur serveur interne',
      code: err.code || 'INTERNAL_ERROR'
    });
  }

  res.status(500).render('error', { 
    error: 'Something went wrong!',
    user: req.session.isAuthenticated ? { email: req.session.userEmail } : null
  });
});

// 404 handler
app.use((req, res) => {
  const isApiRequest =
    req.path.startsWith('/scrape/') ||
    (req.headers['accept'] || '').includes('application/json');

  if (isApiRequest) {
    return res.status(404).json({ error: `Route introuvable : ${req.method} ${req.path}` });
  }

  res.status(404).render('404', { 
    user: req.session.isAuthenticated ? { email: req.session.userEmail } : null
  });
});

app.listen(PORT, () => {
  console.log(`🚀 Scraping UI running on http://localhost:${PORT}`);
});
