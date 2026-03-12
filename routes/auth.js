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
  
  const authorizedUsers = [
    {
      email: process.env.ADMIN_EMAIL || 'admin@example.com',
      password: process.env.ADMIN_PASSWORD || 'admin123'
    },
    {
      email: process.env.ADMIN_EMAIL_2 || 'user2@example.com',
      password: process.env.ADMIN_PASSWORD_2 || 'user2123'
    }
  ];
  
  // Vérifier si l'utilisateur existe dans la liste
  const user = authorizedUsers.find(u => u.email === email && u.password === password);
  
  if (user) {
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
