const { Hono } = require('hono');
const { getScOutrightSummary, getScOutrightDetails, submitSummary, submitReject } = require("../services/frappe-api");
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

router.post('/submit-summary', async (c: any) => {
    const body = await c.req.json(); // 👈 parse JSON body

    const data = await submitSummary(body);
    return c.json(data);
});

router.post('/submit-reject', async (c: any) => {
    const body = await c.req.json(); // 👈 parse JSON body

    const data = await submitReject(body);
    return c.json(data);
});

// router.post('/posted-mms', async (c: any) => {
//     const body = await c.req.json(); // 👈 parse JSON body
    
//     const data = await postedBarcode(body);
//     return c.json(data);
// });


module.exports = router