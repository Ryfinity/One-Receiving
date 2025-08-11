const axios = require('../config/axios');
const kafkaProducer = require('./kafka-producer');

async function postHeaderData(data: any): Promise<any> {
    try {
        const response = await axios.post('/api/method/smr_asn.api.po_receiving_header_stg_api.bulk_insert_headers', data)
        console.log('✅  Header data posted successfully:', response.data);
        if (response.data.message.status == 'error') {
            console.error('❌  Need to logs this error');
        }
        return response.data;
    } catch (error) {
        console.error('❌  Error posting header data:', error);
        throw error;
    }
}

async function postDetailData(data: any): Promise<any> {
    try {
        const response = await axios.post('/api/method/smr_asn.api.po_receiving_detail_stg_api.bulk_insert_details', data)
        console.log('✅  Detail data posted successfully:', response.data);
        if (response.data.message.status == 'error') {
            console.error('❌  Need to logs this error');
        }
        return response.data;
    } catch (error) {
        console.error('❌  Error posting detail data:', error);
        throw error;
    }
}

async function postQuantityData(data: any): Promise<any> {
    try {
        const response = await axios.post('/api/method/smr_asn.api.po_receiving_quantity_stg_api.bulk_insert_quantity', data)
        console.log('✅ Quantity data posted successfully:', response.data);
        if (response.data.message.status == 'error') {
            console.error('❌  Need to logs this error');
        }
        return response.data;
    }
    catch (error) {
        console.error('❌  Error posting quantity data:', error);
        throw error;
    }
}

async function postNonameData(data: any): Promise<any> {
    try {
        const response = await axios.post('/api/method/smr_asn.api.test_noname_api.bulk_insert_noname', data)
        console.log('✅ Noname data posted successfully:', response.data);
        if (response.data.message.status == 'error') {
            console.error('❌  Need to logs this error');
        }
        return response.data;
    } catch (error) {
        console.error('❌  Error posting noname data:', error);
        throw error;
    }
}

async function postLogin() {
    try {
        const response = await axios.post('/api/method/login', {
            usr: process.env.FrappeUser || 'Administrator',
            pwd: process.env.FrappePassword || 'admin'
        });
        console.log('✅  Login successful:', response.headers.get('set-cookie'));
        return response.data;
    } catch (error) {
        console.error('❌  Error during login:', error);
        throw error;
    }
}

async function postAsnOutrightBarcodeData(data: any): Promise<any> {
    try {
        const response = await axios.post('/api/method/smr_asn.api.or_asn_outright_barcode_api.bulk_insert_outright_barcode', data)
        console.log('✅  ASN Outright Barcode data posted successfully:', response.data);
        if (response.data.message.status == 'error') {
            console.error('❌  Need to logs this error');
        }
        return response.data; 
    } catch (error) {
        console.error('❌  Error posting ASN Outright Barcode data:', error);
    }
}

async function postAsnScBarcodeData(data: any): Promise<any> {
    try {
        const response = await axios.post('/api/method/smr_asn.api.or_asn_sc_barcode_api.bulk_insert_sc_barcode', data)
        console.log('✅  ASN SC Barcode data posted successfully:', response.data);
        if (response.data.message.status == 'error') {
            console.error('❌  Need to logs this error');
        }
        return response.data; 
    } catch (error) {
        console.error('❌  Error posting ASN SC Barcode data:', error);
    }
}

async function postAsnOutrightBarcodeDetailsData(data: any): Promise<any> {
    try {
        const response = await axios.post('/api/method/smr_asn.api.or_asn_outright_barcode_api.insert_outright_barcode_details', data)
        console.log('✅  ASN Outright Barcode details data posted successfully:', response.data);
        if (response.data.message.status == 'error') {
            console.error('❌  Need to logs this error');
        }
        if (response.data.message.status == 'invalid') {
            kafkaProducer.main(response.data.message)
            console.error('❌  Invalid Outright Detail');
        }
        return response.data;
    } catch (error) {
        console.error('❌  Error processing ASN Outright Barcode details data:', error);
    }
}

async function postAsnScBarcodeDetailsData(data: any): Promise<any> {
    try {
        const response = await axios.post('/api/method/smr_asn.api.or_asn_sc_barcode_api.insert_sc_barcode_details', data)
        console.log('✅  ASN SC Barcode details data posted successfully:', response.data);
        if (response.data.message.status == 'error') {
            console.error('❌  Need to logs this error');
        }
        if (response.data.message.status == 'invalid') {
            kafkaProducer.main(response.data.message)
            console.error('❌  Invalid SC Detail');
        }
        return response.data;
    } catch (error) {
        console.error('❌  Error processing ASN SC Barcode details data:', error);
    }
}

async function getScOutrightSummary(data: any):  Promise<any> {
    try {
        const response = await axios.post('/api/method/smr_asn.api.or_asn_outright_barcode_api.get_outright_summary', data)
        console.log('✅  ASN Outright Barcode summary:', response.data);
        if (response.data.message.status == 'error') {
            console.error('❌  Need to logs this error');
        }
        return response.data;
    } catch (error) {
        console.error('❌  Error processing ASN Outright Barcode summary data:', error);
    }
}

async function getScOutrightDetails(data: any):  Promise<any> {
    try {
        const response = await axios.post('/api/method/smr_asn.api.or_asn_outright_barcode_api.get_sc_outright_detail', data)
        console.log('✅  ASN Outright Barcode summary:', response.data);
        if (response.data.message.status == 'error') {
            console.error('❌  Need to logs this error');
        }
        return response.data;
    } catch (error) {
        console.error('❌  Error processing ASN Outright Barcode summary data:', error);
    }
}

module.exports = {
    postHeaderData, 
    postDetailData,
    postQuantityData,
    postNonameData,
    postLogin,
    postAsnOutrightBarcodeData,
    postAsnScBarcodeData,
    postAsnOutrightBarcodeDetailsData,
    postAsnScBarcodeDetailsData,
    getScOutrightSummary,
    getScOutrightDetails
};
