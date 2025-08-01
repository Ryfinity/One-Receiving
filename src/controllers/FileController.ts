const S3 = require('../services/s3-file');
const { removeFirstArray, getLastArray, snakeCaseKeys, chunkData, cleanDetailData, sleep } = require('../utils/helpers');
const fs = require('fs');
const https = require("https");
const parse = require('csv-parse/sync').parse;
const readFile = require('node:fs/promises').readFile;
const { postHeaderData, postDetailData, postQuantityData, postNonameData, postLogin } = require('../services/frappe-api');

const s3_one_receiving_folder = process.env.S3_ONE_RECEIVING_FOLDER || 'uat/one_receiving/';
const local_storage_folder = process.env.LOCAL_STORAGE_FOLDER || 'public/downloads/';

async function headerFiles() {
    const tempFiles = await S3.listAllFilesHeader(`${s3_one_receiving_folder+'headers/'}`).catch(console.error);
    const files = await removeFirstArray(tempFiles);

    files.forEach(async (file: any) => {
        const filename = await getLastArray(file);
        const signedUrl = await S3.getSignedUrlForS3File(`${s3_one_receiving_folder+'headers/'+filename}`, 3600).catch(console.error);

        https.get(signedUrl, (response: any) => {
            const filePath = `${local_storage_folder}${filename}`;
            const fileStream = fs.createWriteStream(`${filePath}`);
            response.pipe(fileStream);

            fileStream.on('error', (err: any) => {
                console.error(`❌ Error writing file ${filename}:`, err);
            });

            fileStream.on('finish', async () => {
                console.log(`✅ File ${filename} downloaded successfully.`);

                const file = await readFile(filePath, 'utf8');
                const records = parse(file, { skip_empty_lines: true, columns: true });
                const snakeCaseRecords = await snakeCaseKeys(records);
                const chunkedData = await chunkData(snakeCaseRecords);
                chunkedData.forEach(async (chunk: any) => {
                    console.log(`Chunk size: ${chunk.length}`);
                    await postHeaderData(chunk).catch(console.error);
                });

                fileStream.close(); 
            });

        }).on('error', (err: any) => {
            console.error(`❌ Error downloading file ${filename}:`, err);
        });
    });
}

async function detailFiles() {
    const tempFiles = await S3.listAllFilesHeader(`${s3_one_receiving_folder+'details/'}`).catch(console.error);
    const files = await removeFirstArray(tempFiles);
    
    files.forEach(async (file: any) => {
        const filename = await getLastArray(file);
        const signedUrl = await S3.getSignedUrlForS3File(`${s3_one_receiving_folder+'details/'+filename}`, 3600).catch(console.error);

        https.get(signedUrl, (response: any) => {
            const filePath = `${local_storage_folder}${filename}`;
            const fileStream = fs.createWriteStream(`${filePath}`);
            response.pipe(fileStream);

            fileStream.on('error', (err: any) => {
                console.error(`❌ Error writing file ${filename}:`, err);
            });

            fileStream.on('finish', async () => {
                console.log(`✅ File ${filename} downloaded successfully.`);

                const file = await readFile(filePath, 'utf8');
                const records = parse(file, { skip_empty_lines: true, columns: true });
                const snakeCaseRecords = await snakeCaseKeys(records);
                const chunkedData = await chunkData(snakeCaseRecords);
                chunkedData.forEach(async (chunk: any) => {
                    console.log(`Chunk size: ${chunk.length}`);
                    const cleanedData = await cleanDetailData(chunk);
                    await postDetailData(cleanedData).catch(console.error);
                });

                fileStream.close(); 
            });

        }).on('error', (err: any) => {
            console.error(`❌ Error downloading file ${filename}:`, err);
        });
    });
}

async function quantityFiles() {
    const tempFiles = await S3.listAllFilesHeader(`${s3_one_receiving_folder+'quantity/'}`).catch(console.error);
    const files = await removeFirstArray(tempFiles);
    
    files.forEach(async (file: any) => {
        const filename = await getLastArray(file);
        const signedUrl = await S3.getSignedUrlForS3File(`${s3_one_receiving_folder+'quantity/'+filename}`, 3600).catch(console.error);

        https.get(signedUrl, (response: any) => {
            const filePath = `${local_storage_folder}${filename}`;
            const fileStream = fs.createWriteStream(`${filePath}`);
            response.pipe(fileStream);

            fileStream.on('error', (err: any) => {
                console.error(`❌ Error writing file ${filename}:`, err);
            });

            fileStream.on('finish', async () => {
                console.log(`✅ File ${filename} downloaded successfully.`);

                const file = await readFile(filePath, 'utf8');
                const records = parse(file, { skip_empty_lines: true, columns: true });
                const snakeCaseRecords = await snakeCaseKeys(records);
                const chunkedData = await chunkData(snakeCaseRecords);
                chunkedData.forEach(async (chunk: any) => {
                    console.log(`Chunk size: ${chunk.length}`);
                    await postQuantityData(chunk).catch(console.error);
                });

                fileStream.close(); 
            });

        }).on('error', (err: any) => {
            console.error(`❌ Error downloading file ${filename}:`, err);
        });
    });
}

async function nonameFiles() {
    const tempFiles = await S3.listAllFilesHeader(`${s3_one_receiving_folder+'noname/'}`).catch(console.error);
    const files = await removeFirstArray(tempFiles);
    console.log(files);

    files.forEach(async (file: any) => {
        const filename = await getLastArray(file);
        const signedUrl = await S3.getSignedUrlForS3File(`${s3_one_receiving_folder+'noname/'+filename}`, 3600).catch(console.error);

        https.get(signedUrl, (response: any) => {
            const filePath = `${local_storage_folder}${filename}`;
            const fileStream = fs.createWriteStream(`${filePath}`);
            response.pipe(fileStream);

            fileStream.on('error', (err: any) => {
                console.error(`❌ Error writing file ${filename}:`, err);
            });

            fileStream.on('finish', async () => {
                console.log(`✅ File ${filename} downloaded successfully.`);

                const file = await readFile(filePath, 'utf8');
                const records = parse(file, { skip_empty_lines: true, columns: true });
                const snakeCaseRecords = await snakeCaseKeys(records);
                const chunkedData = await chunkData(snakeCaseRecords);
                console.log(records);
                chunkedData.forEach(async (chunk: any) => {
                    console.log(`Chunk size: ${chunk.length}`);
                    await postNonameData(chunk).catch(console.error);
                });

                fileStream.close(); 
            });

        }).on('error', (err: any) => {
            console.error(`❌ Error downloading file ${filename}:`, err);
        });
    });
}

async function login(): Promise<any> {
    const response: any = await postLogin();
    return response;
}

module.exports = {
    headerFiles,
    detailFiles,
    quantityFiles,
    nonameFiles,
    login
};