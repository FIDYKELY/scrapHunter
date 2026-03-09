class RateLimiter {
  constructor(options = {}) {
    this.maxPerWindow = options.maxPerWindow || 10;
    this.windowMs = options.windowMs || 60 * 1000; // 1 minute default
    this.perDomain = options.perDomain || false;
    
    this.requests = new Map(); // domain -> array of timestamps
  }

  async acquire(domain = 'default') {
    const key = this.perDomain ? domain : 'default';
    const now = Date.now();
    
    // Initialize if not exists
    if (!this.requests.has(key)) {
      this.requests.set(key, []);
    }
    
    const timestamps = this.requests.get(key);
    
    // Remove old requests outside the window
    const windowStart = now - this.windowMs;
    const validRequests = timestamps.filter(timestamp => timestamp > windowStart);
    this.requests.set(key, validRequests);
    
    // Check if we can make a request
    if (validRequests.length < this.maxPerWindow) {
      validRequests.push(now);
      return; // Allow request immediately
    }
    
    // Calculate wait time
    const oldestRequest = Math.min(...validRequests);
    const waitTime = oldestRequest + this.windowMs - now;
    
    if (waitTime > 0) {
      console.log(`⏳ Rate limiting: waiting ${waitTime}ms for domain: ${key}`);
      await new Promise(resolve => setTimeout(resolve, waitTime));
      
      // After waiting, add the new request
      const updatedTimestamps = this.requests.get(key);
      updatedTimestamps.push(Date.now());
    }
  }

  getStats(domain = 'default') {
    const key = this.perDomain ? domain : 'default';
    const timestamps = this.requests.get(key) || [];
    const now = Date.now();
    const windowStart = now - this.windowMs;
    const recentRequests = timestamps.filter(timestamp => timestamp > windowStart);
    
    return {
      currentRequests: recentRequests.length,
      maxPerWindow: this.maxPerWindow,
      windowMs: this.windowMs,
      resetTime: timestamps.length > 0 ? Math.min(...timestamps) + this.windowMs : now
    };
  }

  reset(domain = 'default') {
    const key = this.perDomain ? domain : 'default';
    this.requests.delete(key);
  }

  resetAll() {
    this.requests.clear();
  }
}

module.exports = RateLimiter;
