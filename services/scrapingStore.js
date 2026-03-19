const pool = require('./db');
const { v4: uuidv4 } = require('uuid');

// ============================================================================
// FILE D'ATTENTE (QUEUE)
// ============================================================================

async function enqueue(params, userEmail) {
  const queueId = uuidv4();
  const enqueuedAt = new Date();
  await pool.query(
    `INSERT INTO scraping_queue (queue_id, params, status, enqueued_at, user_email) 
     VALUES ($1, $2, $3, $4, $5)`,
    [queueId, JSON.stringify(params), 'waiting', enqueuedAt, userEmail]
  );
  
  // Calculer la position dans la file
  const res = await pool.query(
    `SELECT COUNT(*) FROM scraping_queue 
     WHERE enqueued_at < $1 AND status = $2`,
    [enqueuedAt, 'waiting']
  );
  const position = parseInt(res.rows[0].count) + 1;
  return { queueId, position };
}

async function dequeueNext() {
  const res = await pool.query(
    `UPDATE scraping_queue SET status = 'ready'
     WHERE queue_id = (
       SELECT queue_id FROM scraping_queue 
       WHERE status = 'waiting' 
       ORDER BY enqueued_at 
       LIMIT 1
     ) RETURNING queue_id`
  );
  return res.rows[0]?.queue_id || null;
}

async function removeFromQueue(queueId) {
  await pool.query('DELETE FROM scraping_queue WHERE queue_id = $1', [queueId]);
}

async function getQueuePosition(queueId) {
  const item = await pool.query(
    'SELECT enqueued_at FROM scraping_queue WHERE queue_id = $1', 
    [queueId]
  );
  if (item.rowCount === 0) return -1;
  
  const { enqueued_at } = item.rows[0];
  const count = await pool.query(
    `SELECT COUNT(*) FROM scraping_queue 
     WHERE enqueued_at < $1 AND status = $2`,
    [enqueued_at, 'waiting']
  );
  return parseInt(count.rows[0].count);
}

async function getQueueItem(queueId) {
  const res = await pool.query(
    'SELECT * FROM scraping_queue WHERE queue_id = $1', 
    [queueId]
  );
  return res.rows[0] || null;
}

async function listQueue() {
  const res = await pool.query(
    `SELECT queue_id, status, enqueued_at, params, user_email 
     FROM scraping_queue 
     ORDER BY enqueued_at`
  );
  return res.rows.map(row => ({
    queueId: row.queue_id,
    status: row.status,
    enqueuedAt: row.enqueued_at,
    params: row.params,
    userEmail: row.user_email
  }));
}

// ============================================================================
// BATCHES DE SCRAPING
// ============================================================================

async function createBatch(batchId, data) {
  const {
    keyword, sources, departments, startTime, enrichmentEnabled, n8nEnabled,
    oneByOneProcessing, destGoogleSheets, destHubSpot, crawlBatchId, sheetId, userEmail
  } = data;
  
  await pool.query(
    `INSERT INTO scraping_batches (
      batch_id, keyword, sources, departments, start_time, is_running,
      enrichment_enabled, n8n_enabled, one_by_one_processing,
      dest_google_sheets, dest_hubspot, crawl_batch_id, sheet_id, user_email
    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)`,
    [
      batchId, keyword, sources || [], departments || [], startTime, true,
      enrichmentEnabled, n8nEnabled, oneByOneProcessing,
      destGoogleSheets, destHubSpot, crawlBatchId, sheetId, userEmail
    ]
  );
}

async function updateBatch(batchId, updates) {
  const entries = Object.entries(updates);
  if (entries.length === 0) return;
  
  const setClause = entries.map(([key], idx) => {
    // Mapper les clés camelCase vers snake_case
    const dbKey = key.replace(/[A-Z]/g, letter => `_${letter.toLowerCase()}`);
    return `${dbKey} = $${idx + 1}`;
  }).join(', ');
  
  const values = entries.map(([, val]) => {
    // Convertir les objets en JSON pour les champs JSONB
    if (typeof val === 'object' && val !== null && !(val instanceof Date) && !Array.isArray(val)) {
      return JSON.stringify(val);
    }
    return val;
  });
  values.push(batchId);
  
  await pool.query(
    `UPDATE scraping_batches SET ${setClause} WHERE batch_id = $${values.length}`,
    values
  );
}

async function getBatch(batchId) {
  const res = await pool.query(
    'SELECT * FROM scraping_batches WHERE batch_id = $1', 
    [batchId]
  );
  if (res.rowCount === 0) return null;
  
  const row = res.rows[0];
  return formatBatchRow(row);
}

async function getAllBatches(userEmail = null) {
  let query = 'SELECT * FROM scraping_batches';
  const params = [];
  
  if (userEmail) {
    query += ' WHERE user_email = $1';
    params.push(userEmail);
  }
  query += ' ORDER BY start_time DESC';
  
  const res = await pool.query(query, params);
  return res.rows.map(formatBatchRow);
}

// Helper pour formater une ligne de batch
function formatBatchRow(row) {
  return {
    batchId: row.batch_id,
    keyword: row.keyword,
    sources: row.sources,
    departments: row.departments,
    startTime: row.start_time,
    isRunning: row.is_running,
    completed: row.completed,
    cancelled: row.cancelled,
    leadsCount: row.leads_count,
    stats: row.stats,
    spreadsheetUrl: row.spreadsheet_url,
    error: row.error,
    enrichmentEnabled: row.enrichment_enabled,
    n8nEnabled: row.n8n_enabled,
    oneByOneProcessing: row.one_by_one_processing,
    destGoogleSheets: row.dest_google_sheets,
    destHubSpot: row.dest_hubspot,
    crawlBatchId: row.crawl_batch_id,
    sheetId: row.sheet_id,
    userEmail: row.user_email,
    // Calculer le temps écoulé
    elapsedSeconds: row.start_time ? 
      Math.round((Date.now() - new Date(row.start_time).getTime()) / 1000) : 0
  };
}

// ============================================================================
// STATUT GLOBAL (en mémoire pour l'instant, peut être migré en DB si multi-instance)
// ============================================================================

let globalScrapingActive = false;

function setGlobalScrapingActive(active) {
  globalScrapingActive = active;
}

function isGlobalScrapingActive() {
  return globalScrapingActive;
}

// ============================================================================
// EXPORTS
// ============================================================================

module.exports = {
  // Queue
  enqueue,
  dequeueNext,
  removeFromQueue,
  getQueuePosition,
  getQueueItem,
  listQueue,
  // Batches
  createBatch,
  updateBatch,
  getBatch,
  getAllBatches,
  // Global status
  setGlobalScrapingActive,
  isGlobalScrapingActive
};
