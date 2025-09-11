const asndatabase = require('../config/asn');
const helpers = require('../utils/helpers');
const { poolPromise, sql } = require('../config/mms');
const kafkaProducer = require('../services/kafka-producer');
const { postAsnOutrightBarcodeData, postAsnScBarcodeData, postAsnOutrightBarcodeDetailsData, postAsnScBarcodeDetailsData } = require('../services/frappe-api');

async function asnOutrightBarcode() {
    try {
        const query = `SELECT DISTINCT aob.asn_id
                ,aob.identifier
                ,ar.vendor_code
                ,ar.vendor_name
                ,aob.store_code
                ,SUBSTR(REPLACE(ad.data,CONCAT(SUBSTRING_INDEX(ad.data, '|', 2),'|'),''),INSTR(REPLACE(ad.data,CONCAT(SUBSTRING_INDEX(ad.data, '|', 2),'|'),''),'|')+1) store_name
                ,aob.department_code
                ,adept.department_name
                ,aob.po_no
                ,aob.invoice_no
                ,aob.sku_no
                ,aob.qty
                ,aob.unit_cost
                ,aob.total_box
                ,aob.line_ender
                ,aw.name AS dc_rdu_name
                ,date_format(ar.delivery_date,'%Y-%m-%d') as delivery_date
                ,adept.env
            FROM asn_outright_barcode aob
            JOIN asn_request ar
                ON 1=1
            AND aob.asn_id = ar.asn_id
            LEFT OUTER
            JOIN asn_details ad
                ON ad.asn_id = ar.asn_id
            AND ad.type = 2
            AND aob.store_code = SUBSTR(REPLACE(ad.data,CONCAT(SUBSTRING_INDEX(ad.data, '|', 2),'|'),''),1,INSTR(REPLACE(ad.data,CONCAT(SUBSTRING_INDEX(ad.data, '|', 2),'|'),''),'|')-1)
            JOIN asn_warehouse aw
                ON ar.warehouse_type = aw.dc
            JOIN (SELECT DISTINCT dept_code department_code, dept_name department_name, env FROM asn_env_dept WHERE env != 'GSP') adept
                ON adept.department_code = aob.department_code
            WHERE 1=1
            AND ar.delivery_date = "2025-08-06"
            AND aob.qty IS NOT NULL
            AND ar.status = 1
            ORDER BY aob.asn_id, aob.store_code, aob.department_code, aob.po_no, aob.sku_no;`;

        const [rows] = await asndatabase.query(`${query}`);
        if (rows.length === 0) {
            console.log('❗️  No ASN Outright Barcode records found for today.');
            return;
        }

        const limit = 500;
        const chunks = await helpers.chunkData(rows, limit);
        
        for (let i = 0; i < chunks.length; i++) {
            await postAsnOutrightBarcodeData(chunks[i]).catch(console.error);
        }

        console.log(`#️⃣  Total chunk data for ASN Outright Barcode: ${chunks.length}`);
        console.log(`#️⃣  Fetched ${rows.length} ASN Outright Barcode records.`);
    } catch (error) {
        console.error('❌  Error fetching ASN Outright Barcode data:', error);
    }
}

async function asnScBarcode() {
    try {
        const query = `SELECT DISTINCT asb.asn_id
                ,asb.identifier
                ,asb.store_code
                ,avd.store_name
                ,ar.vendor_code
                ,ar.vendor_name
                ,asb.dr_number
                ,asb.dept_code
                ,adept.department_name
                ,asb.sub_dept_code
                ,asb.class_code
                ,avd.class_name
                ,asb.total_box
                ,asb.box_no
                ,asb.amount
                ,asb.line_ender
                ,avd.validation
                ,aw.name AS dc_rdu_name
                ,date_format(ar.delivery_date,'%Y-%m-%d') as delivery_date
                ,adept.env
            FROM asn_sc_barcode asb
            JOIN asn_request ar
                    ON 1=1
            AND asb.asn_id = ar.asn_id
            JOIN asn_vdr_data avd
                    ON asb.dr_number = avd.vdr_number
            AND asb.dept_code=avd.dept_code  
            AND asb.sub_dept_code = avd.sub_dept_code
            AND asb.class_code = avd.class_code
            AND asb.vendor_code=avd.vendor_code
            AND avd.validation = 1
            JOIN asn_warehouse aw
                ON 1=1
            AND ar.warehouse_type = aw.dc
            LEFT OUTER
            JOIN (SELECT DISTINCT dept_code department_code, dept_name department_name, env FROM asn_env_dept WHERE env != 'GSP') adept
                ON adept.department_code = asb.dept_code
            WHERE 1=1
            AND ar.delivery_date = "2025-08-06"
            AND ar.status = 1
            ORDER BY asb.asn_id, asb.store_code, asb.dr_number, asb.dept_code, asb.sub_dept_code, asb.class_code`;
    
        const [rows] = await asndatabase.query(`${query}`);
        if (rows.length === 0) {
            console.log('❗️  No ASN SC Barcode records found for today.');
            return;
        }

        const limit = 500;
        const chunks = await helpers.chunkData(rows, limit);
        
        for (let i = 0; i < chunks.length; i++) {
            await postAsnScBarcodeData(chunks[i]).catch(console.error);
        }

        console.log(`#️⃣  Total chunk data for ASN SC Barcode: ${chunks.length}`);
        console.log(`#️⃣  Fetched ${rows.length} ASN SC Barcode records.`);

    } catch (error) {
        console.error('❌  Error fetching ASN SC Barcode data:', error);
    }
}

async function asnBarcodeDetails(message: string, topic: string, partition: any) {
    try {
        const data = JSON.parse(message).data;
        const barcode = JSON.parse(data).barcode;
        const barcodes = barcode.split('\n');
        const identifier = barcodes[0].split(',')[0]; 
        console.log(data)
        if (identifier == "ORRA") {
            await asnOutrightBarcodeDetails(message, topic, partition)
        } else if(identifier == "ORDS") {
            await asnOutrightBarcodeDetails(message, topic, partition)
        } else if(identifier == "SCDS") {
            await asnScBarcodeDetails(message, topic, partition)
        } else if(identifier == "SCRA") {
            await asnScBarcodeDetails(message, topic, partition)
        } else {
            const response = {
                'clientid': JSON.parse(data).clientid,
                'uid': JSON.parse(data).uid,
                'pdtlocation': JSON.parse(data).pdtlocation,
                'data': {
                    "asnid": '',
                    "reference": '',
                    "reference_type": '',
                    "vendorcode": '',
                },
                "status": 'failed',
                "message": 'Unknown Indentifier.'
            }
            kafkaProducer.main(response);
            console.log("❓  Unknown Indentifier.");
        }
    } catch (error) {
        console.error(`❌  Error processing ASN Barcode details: ${error}`);
    }
}

async function asnOutrightBarcodeDetails(message: string, topic: string, partition: any) {
    try {
        const parsedMessage = JSON.parse(message);
        const data = JSON.parse(parsedMessage.data);
        const device = JSON.parse(parsedMessage.device);
        const barcodes = data.barcode.split('\n');

        let lastData: any = [];

        for (const item of barcodes) {
            const [
                identifier,
                store_code,
                department_code,
                po_no,
                invoice_no,
                sku_no,
                qty,
                unit_cost,
                total_box,
                sequence,
                line_ender
            ] = item.split(',');

            const outrightBarcode = {
                uid: data.uid,
                userid: data.userid,
                clientid: data.clientid,
                deviceno: data.deviceno,
                manufacturer: device.manufacturer,
                fingerprint: device.fingerprint,
                model: device.model,
                topic,
                partition,
                identifier,
                store_code: parseInt(store_code),
                department_code: parseInt(department_code),
                po_no: parseInt(po_no),
                invoice_no,
                sku_no: parseInt(sku_no),
                qty: parseInt(qty),
                unit_cost: parseInt(unit_cost),
                total_box: parseInt(total_box),
                sequence: parseInt(sequence),
                line_ender,
                pdt_location: data.pdtlocation,
                asn_ids: data.asnid,
                has_asnid: data.has_asnid
            };

            const result = await postAsnOutrightBarcodeDetailsData(outrightBarcode);
            lastData.push(result);
        }
        const response = lastData[lastData.length - 1].message;

        if (response.status == 'error') {
            console.error('❌  Need to logs this error');
        }
        if (response.status == 'failed') {
            kafkaProducer.main(lastData[lastData.length - 1].message)
            console.error('❌  Invalid Outright Detail');
        }

        if (response.status == 'success') {
            kafkaProducer.main(lastData[lastData.length - 1].message)
            console.error('✅  Valid Outright Detail');
        }

        console.log(`📦  Processing ASN Outright Barcode details for topic: ${topic}, partition: ${partition}`);
    } catch (error) {
        console.error(`❌  Error processing ASN Outright Barcode details: ${error}`);
    }
}

async function asnScBarcodeDetails(message: string, topic: string, partition: any) {
    try {
        const data = JSON.parse(message).data;
        const device = JSON.parse(message).device;
        const barcode = JSON.parse(data).barcode;
        const barcodes = barcode.split('\n');

        let lastData: any = [];

        for (const item of barcodes) {
            const [
                identifier, 
                store_code, 
                vendor_code, 
                dr_number, 
                dept_code, 
                sub_dept_code, 
                class_code, 
                total_box, 
                box_no, 
                amount, 
                line_ender
            ] = item.split(',');
            
            const scBarcode = {
                uid: JSON.parse(data).uid,
                userid: JSON.parse(data).userid,
                clientid: JSON.parse(data).clientid,
                deviceno: JSON.parse(data).deviceno,
                manufacturer: JSON.parse(device).manufacturer,
                fingerprint: JSON.parse(device).fingerprint,
                model: JSON.parse(device).model,
                topic: topic,
                partition: partition,
                identifier: identifier,
                store_code: parseInt(store_code),
                vendor_code: parseInt(vendor_code),
                dr_number: dr_number,
                dept_code: parseInt(dept_code),
                sub_dept_code: parseInt(sub_dept_code),
                class_code: parseInt(class_code),
                total_box: parseInt(total_box),
                box_no: box_no,
                amount: parseFloat(amount),
                line_ender: line_ender,
                pdt_location: JSON.parse(data).pdtlocation,
                asn_ids: JSON.parse(data).asnid,
                has_asnid: JSON.parse(data).has_asnid
            };
            const result = await postAsnScBarcodeDetailsData(scBarcode);
            lastData.push(result);
        }
        const response = lastData[lastData.length - 1].message;

        if (response.status == 'error') {
            console.error('❌  Need to logs this error');
        }
        if (response.status == 'failed') {
            kafkaProducer.main(lastData[lastData.length - 1].message)
            console.error('❌  Invalid SC Detail');
        }

        if (response.status == 'success') {
            kafkaProducer.main(lastData[lastData.length - 1].message)
            console.error('✅  Valid SC Detail');
        }

        console.log(`📦  Processing ASN SC Barcode details for topic: ${topic}, partition: ${partition}`);
    } catch (error) {
        console.error(`❌  Error processing ASN SC Barcode details: ${error}`);
    }
}

async function mmsConnection() {
    try {
        const pool = await poolPromise;
        // const result = await pool.request().query("SELECT * FROM OPENQUERY(mmsapc01, 'SELECT * FROM MMLSTLSL.SMDC40F2 WHERE VDRDATE = 240825 AND VDRTIME > 102030')");
        const result = await pool.request().query("SELECT * FROM OPENQUERY(mmsapc01, 'SELECT * FROM MMLSTLSL.SMDC01F1 ')");
        console.log(result.recordset);
    } catch (err) {
        console.error('❌  MMS Database Connection Error: ', err);
        throw err;
    }
}

async function insertMMSData(data: any[]) {
    try {
        const { postedBarcode } = require('../services/frappe-api');
        const pool = await poolPromise;
        const formattedDate = await helpers.formatDateToDDMMYY(new Date());
        const formattedTime = await helpers.formatTimeToHHMMSS(new Date());
        
        for (const item of data) {
            if(item.vendor_type === 'SC') {
                // console.log("🔄  Inserting SC data into MMS...", item);
                const query = `
                    INSERT INTO OPENQUERY(mmsapc01, 'SELECT VDRVEND,
                    VDRRFC, VDRDPT, VDRSDP, VDRCLS, VDRSTR, VDRBXS, VDRSTAT, 
                    VDRUSER, VDRDATE, VDRTIME, VDPDATE, VDPTIME, NDRAMT, NDRREM
                    FROM MMLSTLSL.SMDC40F2')
                    VALUES (@VDRVEND, @VDRRFC, @VDRDPT, @VDRSDP, @VDRCLS, @VDRSTR, @VDRBXS, @VDRSTAT, @VDRUSER,
                    @VDRDATE, @VDRTIME, @VDPDATE, @VDPTIME, @NDRAMT, @NDRREM)
                `;
                const result = await pool.request()
                    .input('VDRVEND', sql.Int, parseInt(item.vendor_code))
                    .input('VDRRFC', sql.Int, parseInt(item.po_no))
                    .input('VDRDPT', sql.Int, parseInt(item.dept_code))
                    .input('VDRSDP', sql.Int, parseInt(item.sub_dept_code))
                    .input('VDRCLS', sql.Int, parseInt(item.class_code)  )
                    .input('VDRSTR', sql.Int, parseInt(item.store_code))
                    .input('VDRBXS', sql.Int, parseInt(item.total_box))
                    .input('VDRSTAT', sql.VarChar, '')
                    .input('VDRUSER', sql.VarChar, item.userid)
                    .input('VDRDATE', sql.Int, formattedDate)
                    .input('VDRTIME', sql.Int, formattedTime)
                    .input('VDPDATE', sql.Int, '')
                    .input('VDPTIME', sql.Int, '')
                    .input('NDRAMT', sql.Int, parseFloat(item.amount))
                    .input('NDRREM', sql.VarChar, '')
                    .query(query);
                    
                console.log('✅  SC DB MMS data inserted:', result.rowsAffected);
                await postedBarcode(item);

            } else if(item.vendor_type === 'Outright') {
                // console.log("🔄  Inserting Outright data into MMS...", item);
                const queryFirst = `
                    INSERT INTO OPENQUERY(mmsapc01, 'SELECT PONUMB,
                    INUMBR, POLOC, PODQTY, PORQTY, POCQTY
                    FROM MMLSTLSL.SMDC01F2')
                    VALUES (@PONUMB, @INUMBR, @POLOC, @PODQTY, @PORQTY, @POCQTY)
                `;
                const resultFirst = await pool.request()
                    .input('PONUMB', sql.Int, parseInt(item.po_no))
                    .input('INUMBR', sql.Int, parseInt(item.sku_no))
                    .input('POLOC', sql.Int, parseInt(item.store_code))
                    .input('PODQTY', sql.Int, parseInt(item.total_box))
                    .input('PORQTY', sql.Int, parseInt(item.total_box) )
                    .input('POCQTY', sql.Int, parseInt(item.qty))
                    .query(queryFirst);
                    
                console.log('✅  Outright 1st DB MMS data inserted:', resultFirst.rowsAffected);

                const querySecond = `
                    INSERT INTO OPENQUERY(mmsapc01, 'SELECT DCPON,
                    DCRCV, DCINV, DCNUM, DCRQT, DCCNT, DCTAG
                    FROM MMLSTLSL.SMDC01F')
                    VALUES (@DCPON, @DCRCV, @DCINV, @DCNUM, @DCRQT, @DCCNT, @DCTAG)
                `;
                const resultSecond = await pool.request()
                    .input('DCPON', sql.Int, parseInt(item.po_no))
                    .input('DCRCV', sql.Int, '')
                    .input('DCINV', sql.VarChar, item.invoice_no)
                    .input('DCNUM', sql.Int, parseInt(item.sku_no))
                    .input('DCRQT', sql.Int, parseInt(item.qty) )
                    .input('DCCNT', sql.Int, parseInt(item.total_box))
                    .input('DCTAG', sql.Int, '')
                    .query(querySecond);
                    
                console.log('✅  Outright 2nd DB MMS data inserted:', resultSecond.rowsAffected);

                const queryThird = `
                    INSERT INTO OPENQUERY(mmsapc01, 'SELECT DCPON,
                    DCCNT, DCDATE, DCTAG, DUSER
                    FROM MMLSTLSL.SMDC01F1')
                    VALUES (@DCPON, @DCCNT, @DCDATE, @DCTAG, @DUSER)
                `;
                const resultThird = await pool.request()
                    .input('DCPON', sql.Int, parseInt(item.po_no))
                    .input('DCCNT', sql.Int, parseInt(item.sku_count))
                    .input('DCDATE', sql.Int, '')
                    .input('DCTAG', sql.Int, '')
                    .input('DUSER', sql.VarChar, item.userid)
                    .query(queryThird);
                    
                console.log('✅  Outright 3rd DB MMS data inserted:', resultThird.rowsAffected);
                await postedBarcode(item);
                
            } else {
                console.log(`❓  Unknown vendor type: ${item.vendor_type}`);
            }
        }

        return { message: 'MMS data insertion completed' };

    } catch (err) {
        console.error('❌ MMS Database Insert Error:', err);
        throw err;
    }
}

async function toMMS(data: any[], asn_ids: any) {
    try {
        const formattedDate = await helpers.formatDateToDDMMYY(new Date());
        const formattedTime = await helpers.formatTimeToHHMMSS(new Date());

        const scData:any = [];
        const outrightObj:any = [];

        const grouped = data.reduce((acc, item) => {
            const key = `${item.po_no}_${item.store_code}`; // group by PO + Store
            if (!acc[key]) {
                acc[key] = {
                asn_id: item.asn_id,
                po_no: item.po_no,
                store_code: item.store_code,
                store_name: item.store_name.trim(),
                vendor_code: item.vendor_code,
                vendor_name: item.vendor_name,
                status: item.status,
                current_box: item.current_box,
                total_box: item.total_box,
                dept_code: item.dept_code,
                sub_dept_code: item.sub_dept_code,
                vendor_type: item.vendor_type,
                userid: item.userid,
                class_code: item.class_code,
                amount: item.amount,
                sku_count: item.sku_count,
                items: []
                };
            }
            acc[key].items.push({
                po_no: item.po_no,
                invoice_no: item.invoice_no,
                sku_no: item.sku_no,
                qty: item.qty,
                total_box: item.total_box,
                store_code: item.store_code
            });

            return acc;
        }, {});

        const groupedArray = Object.values(grouped);

        groupedArray.forEach((item: any) => {
            if (item.vendor_type === 'SC') {
                scData.push({
                    ENV: "LSP",
                    TYPE: item.vendor_type,
                    VDRVEND: item.vendor_code,
                    VDRRFC: item.po_no,
                    VDRDPT: item.dept_code,
                    VDRSDP: item.sub_dept_code,
                    VDRCLS: item.class_code,
                    VDRSTR: item.store_code,
                    VDRBXS: item.total_box,
                    VDRUSER: item.userid,
                    VDRDATE: formattedDate,
                    VDRTIME: formattedTime,
                    NDRAMT: '',
                });
            } else if (item.vendor_type === 'Outright') {
                outrightObj.push({
                    ENV: 'LSP',
                    DCPO: item.po_no,
                    DCCNT: item.total_box,
                    DUSER: 'PDTUSER',
                    Details: item.items.map((itm: any) => ({
                        DCPON: itm.po_no,
                        DCINV: itm.invoice_no,
                        DCNUM: itm.sku_no,
                        DCRQT: itm.qty,
                        DCBOX: itm.total_box
                    })),
                    F2: item.items.map((itm: any) => ({
                        DCPON: itm.po_no,
                        DCNUM: itm.sku_no,
                        POLOC: itm.store_code,
                        PODQTY: itm.total_box,
                        PORQTY: itm.total_box,
                        POCQTY: itm.qty,
                    }))
                });
            } else {
                console.log(`❓  Unknown vendor type: ${item.vendor_type}`);
                return;
            }
        });

        console.log(JSON.stringify(outrightObj, null, 3));
        console.log(JSON.stringify(scData, null, 3));
        return;

    } catch (err) {
        console.error('❌ MMS Database Insert Error:', err);
        throw err;
    }
}

module.exports = {
    asnOutrightBarcode,
    asnScBarcode,
    asnOutrightBarcodeDetails,
    asnScBarcodeDetails,
    asnBarcodeDetails,
    insertMMSData,
    mmsConnection,
    toMMS
};