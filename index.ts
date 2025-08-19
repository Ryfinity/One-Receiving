// const fileController = require('./src/controllers/FileController');
// fileController.quantityFiles();

const oneReceivingController = require('./src/controllers/OneRecieivingController');
oneReceivingController.asnOutrightBarcode();
// oneReceivingController.asnScBarcode();

// const kafkaConsumer = require('./src/services/kafka-consumer');
// kafkaConsumer.main();

// const kafkaProducer = require('./src/services/kafka-producer');
// kafkaProducer.main();

// const frappe = require("./src/services/frappe-api");
// frappe.postLogin();

const { Hono } = require('hono');
const or = require("./src/routes/oneReceivingRoutes");
const app = new Hono();
app.route('/api', or);

const server = Bun.serve({
    port: process.env.PORT,
    fetch: app.fetch
});

console.log(`Server running on port http://localhost:${server.port}`);