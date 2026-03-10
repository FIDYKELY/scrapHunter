// Frontend JavaScript for scraping tool

// Utility functions
const utils = {
    // Format date for display
    formatDate: (date) => {
        return new Date(date).toLocaleString();
    },

    // Validate email
    isValidEmail: (email) => {
        return /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email);
    },

    // Debounce function
    debounce: (func, wait) => {
        let timeout;
        return function executedFunction(...args) {
            const later = () => {
                clearTimeout(timeout);
                func(...args);
            };
            clearTimeout(timeout);
            timeout = setTimeout(later, wait);
        };
    },

    // Show notification
    showNotification: (message, type = 'info') => {
        const notification = document.createElement('div');
        notification.className = `notification ${type}`;
        notification.textContent = message;
        
        // Style the notification
        Object.assign(notification.style, {
            position: 'fixed',
            top: '20px',
            right: '20px',
            padding: '1rem',
            borderRadius: '5px',
            color: 'white',
            fontWeight: '500',
            zIndex: '1000',
            minWidth: '250px',
            boxShadow: '0 4px 6px rgba(0, 0, 0, 0.1)'
        });

        // Set background color based on type
        const colors = {
            success: '#28a745',
            error: '#dc3545',
            warning: '#ffc107',
            info: '#007bff'
        };
        notification.style.background = colors[type] || colors.info;

        document.body.appendChild(notification);

        // Remove after 3 seconds
        setTimeout(() => {
            if (notification.parentNode) {
                notification.parentNode.removeChild(notification);
            }
        }, 3000);
    }
};

// API helper
const api = {
    // Make authenticated requests
    request: async (url, options = {}) => {
        try {
            const response = await fetch(url, {
                headers: {
                    'Content-Type': 'application/json',
                    ...options.headers
                },
                ...options
            });

            if (!response.ok) {
                throw new Error(`HTTP ${response.status}: ${response.statusText}`);
            }

            return await response.json();
        } catch (error) {
            console.error('API request failed:', error);
            throw error;
        }
    },

    // Get scraping status
    getStatus: () => api.request('/scrape/status'),

    // Start scraping
    startScraping: (keyword, source) => 
        api.request('/scrape/start', {
            method: 'POST',
            body: JSON.stringify({ keyword, source })
        }),

    // Reset status
    resetStatus: () => 
        api.request('/scrape/reset', {
            method: 'POST'
        })
};

// Form validation
const validator = {
    validateScrapingForm: (keyword, source) => {
        const errors = [];

        if (!keyword || keyword.trim().length < 2) {
            errors.push('Keyword must be at least 2 characters long');
        }

        if (!source) {
            errors.push('Please select a source');
        }

        // Check for potentially problematic keywords
        const problematicKeywords = ['<script', 'javascript:', 'data:'];
        if (problematicKeywords.some(pk => keyword.toLowerCase().includes(pk))) {
            errors.push('Invalid keyword detected');
        }

        return errors;
    }
};

// Export for use in other files
if (typeof module !== 'undefined' && module.exports) {
    module.exports = { utils, api, validator };
}
