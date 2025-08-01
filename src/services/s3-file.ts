const { S3Client, GetObjectCommand, ListObjectsV2Command } = require ('@aws-sdk/client-s3');
const { getSignedUrl } = require('@aws-sdk/s3-request-presigner');
const { removeFirstArray } = require('../utils/helpers');

const s3Client = new S3Client({ region: process.env.S3_REGION,
    credentials: {
        accessKeyId: process.env.S3_ACCESS_KEY_ID,
        secretAccessKey: process.env.S3_SECRET_KEY
    }
});

async function listAllFilesHeader(prefix: string): Promise<any> {
    const files: string[] = [];

    const command = new ListObjectsV2Command({
        Bucket: process.env.S3_BUCKET_NAME,
        Prefix: prefix,
    });

    const response = await s3Client.send(command);
    const contents = response.Contents || [];

    contents.forEach((item: any) => {
        if (item.Key) files.push(item.Key);
    });

    return files;
}

async function getSignedUrlForS3File(fileName: string, expiresInSeconds: number): Promise<string> {
    const command = new GetObjectCommand({
        Bucket: process.env.S3_BUCKET_NAME,
        Key: fileName,
    });

    try {
        const signedUrl = await getSignedUrl(s3Client, command, { expiresIn: expiresInSeconds });
        return signedUrl;
    } catch (error) {
        console.error('❌ Error generating signed URL:', error);
        throw error;
    }
}

async function downloadFileFromS3(fileName: string): Promise<void> {
    const command = new GetObjectCommand({
        Bucket: process.env.S3_BUCKET_NAME,
        Key: fileName,
    });

    try {
        const response = await s3Client.send(command);
        console.log(`✅ File downloaded successfully: ${fileName}`);
    } catch (error) {
        console.error('❌ Error downloading file:', error);
        throw error;
    }
}

module.exports = {
    getSignedUrlForS3File,
    listAllFilesHeader
};