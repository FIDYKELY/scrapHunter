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
      username: process.env.ADMIN_USERNAME || 'admin',
      password: process.env.ADMIN_PASSWORD || 'admin123',
      role: 'admin'
    },
    {
      email: process.env.USER_EMAIL_2 || 'joel@example.com',
      username: process.env.USER_USERNAME_2 || 'joel',
      password: process.env.USER_PASSWORD_2 || 'joel2123',
      role: 'user'
    }
  ];
  
  
  // Vérifier si l'utilisateur existe (par email OU par username)
  const user = authorizedUsers.find(u => 
    (u.email === email || u.username === email) && u.password === password
  );
  
  console.log('✅ User found:', user ? 'YES' : 'NO');
  
  if (user) {
    req.session.isAuthenticated = true;
    req.session.userEmail = user.email;
    req.session.userRole = user.role;
    res.redirect('/scrape');
  } else {
    req.session.error = 'Invalid email, username or password';
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
