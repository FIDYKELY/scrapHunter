const express = require('express');
const router = express.Router();

// Login page
router.get('/login', (req, res) => {
  if (req.session.isAuthenticated) {
    return res.redirect('/scrape');
  }
  res.render('login', { 
    error: req.session.error || null,
    user: null
  });
  delete req.session.error;
});

// Login handler
router.post('/login', (req, res) => {
  const { email, password } = req.body;
  
  // Static authentication (in production, use proper auth)
  const adminEmail = process.env.ADMIN_EMAIL || 'admin@example.com';
  const adminPassword = process.env.ADMIN_PASSWORD || 'admin123';
  
  if (email === adminEmail && password === adminPassword) {
    req.session.isAuthenticated = true;
    req.session.userEmail = email;
    res.redirect('/scrape');
  } else {
    req.session.error = 'Invalid email or password';
    res.redirect('/login');
  }
});

// Logout handler
router.get('/logout', (req, res) => {
  req.session.destroy((err) => {
    if (err) {
      console.error('Session destroy error:', err);
    }
    res.redirect('/login');
  });
});

module.exports = router;
