const express = require('express');
const session = require('express-session');
const PgSession = require('connect-pg-simple')(session);
const path = require('path');
require('dotenv').config();

const pool = require('./services/db');
const authRoutes = require('./routes/auth');
const scrapeRoutes = require('./routes/scrape');

const app = express();
const PORT = process.env.PORT || 3000;

// Middleware
app.use(express.urlencoded({ extended: true }));
app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

// Vérifier que les secrets sont définis
if (!process.env.SESSION_SECRET) {
  console.error('❌ ERREUR: SESSION_SECRET non défini dans les variables d\'environnement');
  console.error('   Créez un fichier .env avec: SESSION_SECRET=votre_secret_long_et_aleatoire');
  process.exit(1);
}

// Vérifier les paramètres de base de données
if (!process.env.DB_USER || !process.env.DB_PASSWORD) {
  console.error('❌ ERREUR: Variables de base de données manquantes');
  console.error('   Définissez dans .env: DB_USER=admin DB_PASSWORD=votre_mot_de_passe');
  process.exit(1);
}

// Session configuration avec PostgreSQL
const sessionConfig = {
  store: new PgSession({
    pool: pool,
    tableName: 'scraping_sessions',
    createTableIfMissing: true
  }),
  secret: process.env.SESSION_SECRET,
  resave: false,
  saveUninitialized: false,
  cookie: {
    secure: process.env.NODE_ENV === 'production',
    httpOnly: true,
    maxAge: 24 * 60 * 60 * 1000 // 24 hours
  }
};

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
    user: req.session?.isAuthenticated ? { email: req.session.userEmail } : null
  });
});

// 404 handler
app.use((req, res) => {
  res.status(404).render('404', { 
    user: req.session?.isAuthenticated ? { email: req.session.userEmail } : null
  });
});

app.listen(PORT, () => {
  console.log(`🚀 Scraping UI running on http://localhost:${PORT}`);
});
