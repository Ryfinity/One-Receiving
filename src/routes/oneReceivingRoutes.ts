const { Hono } = require('hono');
const { getScOutrightSummary, getScOutrightDetails } = require("../services/frappe-api");
const router = new Hono();

router.post('/get-sc-outright-summary', async (c: any) => {
    const body = await c.req.json(); // 👈 parse JSON body

    const data = await getScOutrightSummary(body);
    return c.json(data);
});

router.post('/get-sc-outright-details', async (c: any) => {
    const body = await c.req.json(); // 👈 parse JSON body

    const data = await getScOutrightDetails(body);
    return c.json(data);
});

module.exports = router