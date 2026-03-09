class Logger {
  constructor() {
    this.logLevel = process.env.LOG_LEVEL || 'info';
    this.levels = {
      error: 0,
      warning: 1,
      info: 2,
      debug: 3
    };
    this.logs = []; // Store recent logs
    this.maxLogs = 1000; // Keep last 1000 logs
  }

  shouldLog(level) {
    return this.levels[level] <= this.levels[this.logLevel];
  }

  formatMessage(level, message, meta = {}) {
    const timestamp = new Date().toISOString();
    const metaStr = Object.keys(meta).length > 0 ? JSON.stringify(meta) : '';
    return `[${timestamp}] ${level.toUpperCase()}: ${message} ${metaStr}`;
  }

  addLog(level, message, meta = {}) {
    const logEntry = {
      timestamp: new Date().toISOString(),
      level,
      message,
      meta
    };
    
    this.logs.push(logEntry);
    
    // Keep only the last maxLogs entries
    if (this.logs.length > this.maxLogs) {
      this.logs.shift();
    }
  }

  getLogs(since = null) {
    if (since) {
      return this.logs.filter(log => log.timestamp > since);
    }
    return this.logs.slice(-50); // Return last 50 logs by default
  }

  clearLogs() {
    this.logs = [];
  }

  error(message, meta = {}) {
    this.addLog('error', message, meta);
    if (this.shouldLog('error')) {
      console.error(this.formatMessage('error', message, meta));
    }
  }

  warning(message, meta = {}) {
    this.addLog('warning', message, meta);
    if (this.shouldLog('warning')) {
      console.warn(this.formatMessage('warning', message, meta));
    }
  }

  info(message, meta = {}) {
    this.addLog('info', message, meta);
    if (this.shouldLog('info')) {
      console.log(this.formatMessage('info', message, meta));
    }
  }

  debug(message, meta = {}) {
    this.addLog('debug', message, meta);
    if (this.shouldLog('debug')) {
      console.log(this.formatMessage('debug', message, meta));
    }
  }
}

// Create singleton instance
const logger = new Logger();

// Export convenience functions
const logError = (message, meta) => logger.error(message, meta);
const logWarning = (message, meta) => logger.warning(message, meta);
const logInfo = (message, meta) => logger.info(message, meta);
const logDebug = (message, meta) => logger.debug(message, meta);

module.exports = {
  Logger,
  logger,
  logError,
  logWarning,
  logInfo,
  logDebug
};
