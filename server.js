const express = require('express');
const session = require('express-session');
const path = require('path');
require('dotenv').config();

const authRoutes = require('./routes/auth');
const scrapeRoutes = require('./routes/scrape');

const app = express();
const PORT = process.env.PORT || 3000;

// Middleware
app.use(express.urlencoded({ extended: true }));
app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

// Session configuration with enhanced security
const sessionConfig = {
  secret: process.env.SESSION_SECRET,
  resave: false,
  saveUninitialized: false,
  cookie: {
    secure: process.env.NODE_ENV === 'production', // true en prod avec HTTPS
    httpOnly: true, // Empêche l'accès JavaScript au cookie
    maxAge: 24 * 60 * 60 * 1000 // 24 hours
  }
};

// Vérifier que le secret est défini
if (!sessionConfig.secret) {
  console.error('❌ ERREUR: SESSION_SECRET non défini dans les variables d\'environnement');
  console.error('   Créez un fichier .env avec: SESSION_SECRET=votre_secret_long_et_aleatoire');
  process.exit(1);
}

app.use(session(sessionConfig));

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

// Error handling middleware
app.use((err, req, res, next) => {
  console.error(err.stack);
  res.status(500).render('error', { 
    error: 'Something went wrong!',
    user: req.session.isAuthenticated ? { email: req.session.userEmail } : null
  });
});

// 404 handler
app.use((req, res) => {
  res.status(404).render('404', { 
    user: req.session.isAuthenticated ? { email: req.session.userEmail } : null
  });
});

app.listen(PORT, () => {
  console.log(`🚀 Scraping UI running on http://localhost:${PORT}`);
});
