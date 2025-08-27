const sql = require("mssql");

const poolPromise = sql.connect({
    user: 'RADS_USER',
    password: 'DCpdt2017',
    server: 'HOSCBUD1',
    database: 'onepdt_receiving',
    port: 1433,
    options: {
        encrypt: false,
        trustServerCertificate: false
    }
});

module.exports = {
    sql,
    poolPromise
};
