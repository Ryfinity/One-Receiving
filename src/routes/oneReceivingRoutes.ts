const { Hono } = require('hono');
const { getOutrightSummary } = require("../services/frappe-api");
const router = new Hono();

router.post('/get-outright-summary', async (c: any) => {
    const body = await c.req.json(); // 👈 parse JSON body
    const { store_code } = body;

    const data = await getOutrightSummary(body);
    return c.json(data);
});

module.exports = router