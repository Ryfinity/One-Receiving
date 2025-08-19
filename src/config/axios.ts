const axios = require('axios');

process.env.NODE_TLS_REJECT_UNAUTHORIZED = "0";

const instance = axios.create({
    baseURL: process.env.API_BASE_URL || 'http://localhost:8000',
    timeout: 10000,
    headers: {
        'Content-Type': 'application/json',
        'Accept': 'application/json'
    }
});

instance.interceptors.request.use(
    (config: any) => {
        // console.log(`Request made with ${config.method.toUpperCase()} method to ${config.url}`);
        return config;
    },
    (error: any) => {
        console.error('Error in request:', error);
        return Promise.reject(error);
    }
);

module.exports = instance;