const pool = require('./services/db');

async function initDb() {
  try {
    // Table pour les sessions (utilisée par connect-pg-simple)
    await pool.query(`
      CREATE TABLE IF NOT EXISTS scraping_sessions (
        sid VARCHAR NOT NULL PRIMARY KEY,
        sess JSON NOT NULL,
        expire TIMESTAMP NOT NULL
      );
    `);
    console.log('✅ Table scraping_sessions créée');

    // Table pour les batches de scraping
    await pool.query(`
      CREATE TABLE IF NOT EXISTS scraping_batches (
        batch_id VARCHAR(36) PRIMARY KEY,
        keyword TEXT NOT NULL,
        sources TEXT[],
        departments TEXT[],
        start_time TIMESTAMPTZ NOT NULL,
        is_running BOOLEAN DEFAULT TRUE,
        completed BOOLEAN DEFAULT FALSE,
        cancelled BOOLEAN DEFAULT FALSE,
        leads_count INTEGER DEFAULT 0,
        stats JSONB,
        spreadsheet_url TEXT,
        error TEXT,
        enrichment_enabled BOOLEAN,
        n8n_enabled BOOLEAN,
        one_by_one_processing BOOLEAN,
        dest_google_sheets BOOLEAN,
        dest_hubspot BOOLEAN,
        crawl_batch_id VARCHAR(36),
        sheet_id TEXT,
        user_email TEXT
      );
    `);
    console.log('✅ Table scraping_batches créée');

    // Table pour la file d'attente
    await pool.query(`
      CREATE TABLE IF NOT EXISTS scraping_queue (
        queue_id VARCHAR(36) PRIMARY KEY,
        params JSONB NOT NULL,
        status VARCHAR(20) DEFAULT 'waiting',
        enqueued_at TIMESTAMPTZ NOT NULL,
        user_email TEXT
      );
    `);
    console.log('✅ Table scraping_queue créée');

    // Créer des index pour optimiser les performances
    await pool.query(`
      CREATE INDEX IF NOT EXISTS idx_batches_user_email ON scraping_batches(user_email);
    `);
    await pool.query(`
      CREATE INDEX IF NOT EXISTS idx_batches_start_time ON scraping_batches(start_time DESC);
    `);
    await pool.query(`
      CREATE INDEX IF NOT EXISTS idx_queue_status ON scraping_queue(status);
    `);
    await pool.query(`
      CREATE INDEX IF NOT EXISTS idx_queue_enqueued_at ON scraping_queue(enqueued_at);
    `);
    console.log('✅ Index créés');

    console.log('✅ Base de données initialisée avec succès');
  } catch (err) {
    console.error('❌ Erreur initialisation DB:', err);
    process.exit(1);
  } finally {
    await pool.end();
  }
}

// Si ce fichier est exécuté directement
if (require.main === module) {
  initDb();
}

module.exports = initDb;
