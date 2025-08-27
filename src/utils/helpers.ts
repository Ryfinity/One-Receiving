const { mapKeys, snakeCase } = require('lodash');

async function removeFirstArray(arr: string[]) {
    if (arr.length === 0) return arr;
    return arr.slice(1);
}

async function getLastArray(arr: string) {
    const split = arr.split("/")
    if (split.length === 0) return null;
    return split[split.length - 1];
}

async function snakeCaseKeys(obj: any) {
    const result = obj.map((item: any) => mapKeys(item, (_: any, key: string) => snakeCase(key)));
    return result;
}

async function chunkData(arr: any[], chunkSize: number = 100) {
    const result = [];
    for (let i = 0; i < arr.length; i += chunkSize) {
        result.push(arr.slice(i, i + chunkSize));
    }
    return result;
}

async function cleanDetailData(arr: any[]) {
    return arr.map((item: any) => {
        if (item['season_code'] == '' || item['dimension_code'] == '') {
            item['season_code'] = 0;
            item['dimension_code'] = 0;
        }
        return item;
    });
}

async function sleep(ms: number) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

async function formatDateToDDMMYY(dateInput: Date | string) {
    const date = new Date(dateInput);
    const dd = String(date.getDate()).padStart(2, '0');
    const mm = String(date.getMonth() + 1).padStart(2, '0');
    const yy = String(date.getFullYear()).slice(-2);
    return `${dd}${mm}${yy}`;
}

function formatTimeToHHMMSS(dateInput: Date | string): string {
    const date = new Date(dateInput);
    const hh = String(date.getHours()).padStart(2, '0');
    const mm = String(date.getMinutes()).padStart(2, '0');
    const ss = String(date.getSeconds()).padStart(2, '0');
    return `${hh}${mm}${ss}`;
}


module.exports = {
    removeFirstArray,
    getLastArray,
    snakeCaseKeys,
    chunkData,
    cleanDetailData,
    sleep,
    formatDateToDDMMYY,
    formatTimeToHHMMSS
};