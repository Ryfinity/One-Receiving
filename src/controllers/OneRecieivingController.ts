const asndatabase = require('../config/asn');
const helpers = require('../utils/helpers');
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
                ,ar.delivery_date
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
            JOIN (SELECT DISTINCT department_code, department_name FROM asn_department_dc) adept
                ON adept.department_code = aob.department_code
            WHERE 1=1
            AND ar.delivery_date = CURDATE()
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
                ,asb.total_box
                ,asb.box_no
                ,asb.amount
                ,asb.line_ender 
                ,avd.validation
                ,aw.name AS dc_rdu_name
                ,ar.delivery_date
                ,adept.class_name
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
            JOIN (SELECT DISTINCT department_code, department_name FROM asn_department_dc) adept
                ON adept.department_code = asb.dept_code
            WHERE 1=1
            AND ar.delivery_date = CURDATE()
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

        if (identifier == "ORRA") {
            await asnOutrightBarcodeDetails(message, topic, partition)
        } else if(identifier == "SCDS") {
            await asnScBarcodeDetails(message, topic, partition)
        } else {
            console.log("❓  Unknow Indentifier.")
        }
    } catch (error) {
        console.error(`❌  Error processing ASN Barcode details: ${error}`);
    }
}

async function asnOutrightBarcodeDetails(message: string, topic: string, partition: any) {
    try {
        const data = JSON.parse(message).data;
        const device = JSON.parse(message).device;
        const barcode = JSON.parse(data).barcode;
        const barcodes = barcode.split('\n');

        barcodes.forEach(async (item: any) => {
            const [identifier, store_code, department_code, po_no, invoice_no, sku_no, qty, unit_cost, total_box, sequence, line_ender] = item.split(',');
            const outrightBarcode = {
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
                department_code: parseInt(department_code),
                po_no: parseInt(po_no),
                invoice_no: invoice_no,
                sku_no: parseInt(sku_no),
                qty: parseInt(qty),
                unit_cost: parseInt(unit_cost),
                total_box: parseInt(total_box),
                sequence: parseInt(sequence),
                line_ender: line_ender,
                pdt_location: JSON.parse(data).pdtlocation,
                asn_ids: JSON.parse(data).asnid

            };
            await postAsnOutrightBarcodeDetailsData(outrightBarcode).catch(console.error);
        });
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

        barcodes.forEach(async (item: any) => {
            const [identifier, store_code, vendor_code, dr_number, dept_code, sub_dept_code, class_code, total_box, box_no, amount, line_ender] = item.split(',');
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
                asn_ids: JSON.parse(data).asnid
            };
            await postAsnScBarcodeDetailsData(scBarcode).catch(console.error);
        });
        console.log(`📦  Processing ASN SC Barcode details for topic: ${topic}, partition: ${partition}`);
    } catch (error) {
        console.error(`❌  Error processing ASN SC Barcode details: ${error}`);
    }
}

module.exports = {
    asnOutrightBarcode,
    asnScBarcode,
    asnOutrightBarcodeDetails,
    asnScBarcodeDetails,
    asnBarcodeDetails
};