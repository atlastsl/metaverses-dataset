const {Asset, Operation} = require('./models.js');
const ExcelJS = require('exceljs');
const mongoose = require('mongoose');
const fs = require('fs');
const minimist = require('minimist');
const Decimal = require('decimal.js');
require('dotenv').config();


let focalPoints = null;
const distances = {};


function parseDateParams(dateStr) {
    const parts = (dateStr || "").split("/");
    if (parts.length === 3 && parts.filter(x => isNaN(parseInt(x))).length === 0) {
        const day = parseInt(parts[0], 10), month = parseInt(parts[1], 10), year = parseInt(parts[2], 10);
        const date = new Date();
        date.setUTCFullYear(year); date.setUTCMonth(month-1); date.setUTCDate(day);
        date.setUTCHours(0, 0, 0, 0);
        return date;
    }
    return null;
}

async function fetchAllData(params) {
    const payload = {};
    if (params['collection']) payload['collection'] = params['collection'];
    if (params['asset']) payload['asset_contract'] = `0x${params['asset']}`;
    if (params['fdate']) {
        if (!payload['mvt_date']) payload['mvt_date'] = {};
        payload['mvt_date']['$gte'] = parseDateParams(params['fdate']);
    }
    if (params['tdate']) {
        if (!payload['mvt_date']) payload['mvt_date'] = {};
        payload['mvt_date']['$lt'] = parseDateParams(params['tdate']);
    }
    if (params['optype']) {
        payload['operation_type'] = params['optype'];
    }
    let request =  Operation.find(payload).populate('asset').sort('mvt_date');
    if (params['limit']) {
        request = request.limit(params['limit'])
    }
    return await request.allowDiskUse(true).exec();
}

function asset_getNumberOfPreviousOperations(operation, operations) {
    const nbPrevFreeOps = (operations || []).filter(op => {
        return op['asset'].toString() === operation['asset'].toString() && op['operation_type'] === "free" && op['mvt_date'].getTime() < operation['mvt_date'].getTime()
    }).length;
    const nbPrevSaleOps = (operations || []).filter(op => {
        return op['asset'].toString() === operation['asset'].toString() && op['operation_type'] === "sale" && op['mvt_date'].getTime() < operation['mvt_date'].getTime()
    }).length;
    let nbPrevLandsFreeOps = 0;
    let nbPrevLandsSaleOps = 0;
    return {nbPrevFreeOps, nbPrevSaleOps, nbPrevLandsFreeOps, nbPrevLandsSaleOps}
}

function operation_getAmount(operation) {
    let amount1Raw = operation['amount']?.[0]?.['value'], amount1CCy = operation['amount']?.[0]?.['currency'], amount1Usd = operation['amount']?.[0]?.['value_usd'];
    let amount2Raw = operation['amount']?.[1]?.['value'], amount2CCy = operation['amount']?.[1]?.['currency'], amount2Usd = operation['amount']?.[1]?.['value_usd'];
    let amountTotal = (operation['amount'] || []).map(x => x['value_usd']).reduce((a, b) => a+b, 0);
    let feesRaw = operation['fees']?.[0]?.['value'], feesCcy = operation['fees']?.[0]?.['currency'], feesUsd = operation['fees']?.[0]?.['value_usd'];
    return {
        amount1Raw, amount1CCy, amount1Usd, amount2Raw, amount2CCy, amount2Usd, amountTotal,
        feesRaw, feesCcy, feesUsd
    }
}

function get_blockchain_crypto_prices(operation) {
    let bcPrice1 = null, bcPrice2 = null;
    if (operation.collection === "decentraland") {
        bcPrice1 = operation['fees']?.[0]?.['currency_price'];
    }
    return {bcPrice1, bcPrice2};
}

function evaluateDistance (x, y, fpXY, disType) {
    const [x2, y2] = fpXY;
    let x1bn = new Decimal(x), y1bn = new Decimal(y), x2bn = new Decimal(x2), y2bn = new Decimal(y2); 
    if (disType === "manhattan") {
        const dx = x1bn.sub(x2bn).abs(), dy = y1bn.sub(y2bn).abs()
        return dx.add(dy).toNumber();
    }
    else {
        const dx = x1bn.sub(x2bn).pow(2), dy = y1bn.sub(y2bn).pow(2);
        return dx.add(dy).sqrt().toDecimalPlaces(7).toNumber();
    }
}

function calculateDistanceToFocalPoint (x, y, focalPoint, disType) {
    const distances = (focalPoint.parcelsXY || []).map(fpXY => evaluateDistance(x, y, fpXY, disType));
    return distances.length > 0 ? Math.min(...distances) : 0;
}

function calculateDistances (asset, fpType, disType) {
    if (!focalPoints) {
        focalPoints = JSON.parse(fs.readFileSync('./files/dcl_focal_points.json', {encoding: 'utf8'}));
    }
    if (!distances[asset['_id'].toString()]) {
        distances[asset['_id'].toString()] = {};
    }
    if (distances[asset['_id'].toString()][fpType]) {
        return distances[asset['_id'].toString()][fpType]
    }
    if (fpType === "plazas") {
        const plazas = Object.keys(focalPoints['plazas']);
        const prPlazas = plazas.filter(x => !x.includes("CENTRAL") && !x.includes("PIXEL"));
        const allDistances = {}, prDistances = [];
        for (const plaza of plazas) {
            const d = calculateDistanceToFocalPoint(asset.x, asset.y, focalPoints['plazas'][plaza], disType);
            allDistances[`DIST_${plaza}`] = d;
            if (prPlazas.includes(plaza)) prDistances.push(d);
        }
        allDistances['DIST_MIN_PR_PLAZA'] = prDistances.length > 0 ? Math.min(...prDistances) : 0;
        distances[asset['_id'].toString()]['plazas'] = allDistances;
    }
    else if (fpType === "roads") {
        const roads = Object.keys(focalPoints['roads']);
        const rDistances = roads.map(road => {
            return calculateDistanceToFocalPoint(asset.x, asset.y, focalPoints['roads'][road], disType);
        });
        distances[asset['_id'].toString()]['roads'] = {"DIST_ROAD": Math.min(...rDistances)};
    }
    else if (fpType === "districts") {
        const districts = Object.keys(focalPoints['districts']);
        const dstIndDistances = {}, dstCatDistances = {};
        for (const district of districts) {
            const dstFp = focalPoints['districts'][district];
            const d = calculateDistanceToFocalPoint(asset.x, asset.y, dstFp, disType);
            dstIndDistances[`DIST_${dstFp['id']}`] = d;
            const dstCat = focalPoints['districts'][district]['category'];
            if (!dstCatDistances[`DIST_DST_${dstCat}`]) dstCatDistances[`DIST_DST_${dstCat}`] = [];
            dstCatDistances[`DIST_DST_${dstCat}`].push(d);
        }
        for (const dstCat in dstCatDistances) {
            if (dstCatDistances.hasOwnProperty(dstCat)) dstCatDistances[dstCat] = Math.min(...dstCatDistances[dstCat])
        }
        distances[asset['_id'].toString()]['districts'] = {...dstIndDistances, ...dstCatDistances};
    }
    return distances[asset['_id'].toString()][fpType] || {};
}

function prettifyAddress(address) {
    return address.substring(0, 8) + "..." + address.substring(address.length-4);
}

function prettifyTxHash(txHash) {
    return txHash; //txHash.substring(0, 12) + "..." + txHash.substring(txHash.length-6);
}

function linifyOperation(operation, operations, distanceType) {
    const asset = operation['asset'];
    const {nbPrevFreeOps, nbPrevSaleOps, nbPrevLandsFreeOps, nbPrevLandsSaleOps} = asset_getNumberOfPreviousOperations(operation, operations);
    const {
        amount1Raw, amount1CCy, amount1Usd, amount2Raw, amount2CCy, amount2Usd, amountTotal,
        feesRaw, feesCcy, feesUsd
    } = operation_getAmount(operation);
    const {bcPrice1, bcPrice2} = get_blockchain_crypto_prices(operation);
    const plazasDistances = calculateDistances(asset, "plazas", distanceType);
    const roadsDistances = calculateDistances(asset, "roads", distanceType);
    const districtsDistances = calculateDistances(asset, "districts", distanceType);
    return {
        operation_id: operation['_id'].toString(),
        operation_hash: prettifyTxHash(operation['transaction_hash']),
        operation_type:  operation['operation_type'],
        collection: operation['collection'],
        blockchain: operation['blockchain'],
        mvt_date: operation['mvt_date'],
        sender: prettifyAddress(operation['sender']),
        recipient: prettifyAddress(operation['recipient']),
        asset_id: asset['_id'].toString(),
        asset_name: asset['name'],
        asset_description: asset['description'],
        asset_type: asset['type'],
        asset_coord_x: asset['x'],
        asset_coord_y: asset['y'],
        asset_size: 1,
        asset_lands: null,
        ...plazasDistances,
        ...roadsDistances,
        ...districtsDistances,
        asset_prev_free_ops: nbPrevFreeOps,
        asset_prev_sale_ops: nbPrevSaleOps,
        asset_lands_prev_free_ops: nbPrevLandsFreeOps,
        asset_lands_prev_sale_ops: nbPrevLandsSaleOps,
        amount_1_raw: amount1Raw,
        amount_1_ccy: amount1CCy,
        amount_1_usd: amount1Usd,
        amount_2_raw: amount2Raw,
        amount_2_ccy: amount2CCy,
        amount_2_usd: amount2Usd,
        amount_usd: amountTotal,
        fees_raw: feesRaw,
        fees_ccy: feesCcy,
        fees_usd: feesUsd,
        blockchain_crypto_price_1: bcPrice1,
        blockchain_crypto_price_2: bcPrice2,
        collection_crypto_name: operation['market_info']?.['currency'],
        collection_crypto_price: operation['market_info']?.['price'],
        collection_crypto_change_24h: operation['market_info']?.['change_24h'],
        collection_crypto_volume_24h: operation['market_info']?.['volume_24h'],
        collection_crypto_market_cap: operation['market_info']?.['market_cap'],
    }
}

function excel_newWorkbook() {
    const workbook = new ExcelJS.Workbook();
    workbook.creator = 'Aurelien';
    workbook.lastModifiedBy = 'Aurelien';
    workbook.created = new Date();
    workbook.modified = new Date();
    workbook.lastPrinted = new Date();
    return workbook;
}

function excel_writeWorksheet(worksheet, columns, lines) {
    worksheet.columns = columns.map(x => {return {key: x['key'], width: x['width']}});
    columns.forEach(x => {
        worksheet.getColumn(x['key']).alignment = {vertical: x['vertical'] || 'middle', horizontal: x['horizontal'] || 'left'};
    });
    let headersRow = worksheet.getRow(1);
    columns.forEach(x => {
        headersRow.getCell(x['key']).value = {richText: [{font: {italic: false, bold: true}, text: x['title'] || ''}]};
    });
    headersRow.alignment = {vertical: 'middle', horizontal: 'left'};

    let iRow = 2;
    for (const line of lines) {
        let row = worksheet.getRow(iRow);
        row.values = line;
        iRow++;
    }
}

async function excelFile(lines) {
    const workbook = excel_newWorkbook();

    let distPlazasCols = [];
    let distRoadsCols = [];
    let distDistrictsCols = [];

    if (focalPoints) {
        if (focalPoints['plazas']) {
            distPlazasCols = Object.keys(focalPoints['plazas']).map(key => {
                return {key: `DIST_${key}`, width: 15, horizontal: 'right', title: `DIST_${key}`, description: `Distance par rapport au Plaza ${focalPoints['plazas'][key]['name']}`}
            });
            distPlazasCols.push(
                {key: `DIST_MIN_PR_PLAZA`, width: 15, horizontal: 'right', title: `DIST_MIN_PR_PLAZA`, description: `Distance minimale par rapport à un plaza périphérique`}
            )
        }
        if (focalPoints['roads']) {
            distRoadsCols.push(
                {key: `DIST_ROAD`, width: 15, horizontal: 'right', title: `DIST_ROAD`, description: `Distance par rapport à la PISTE DE ROUTE la plus proche`}
            )
        }
        if (focalPoints['districts']) {
            const p1 = Object.keys(focalPoints['districts']).map(key => {
                const dstFp = focalPoints['districts'][key];
                return {key: `DIST_${dstFp['id']}`, width: 15, horizontal: 'right', title: `DIST_${dstFp['id']}`, description: `Distance par rapport au District ${dstFp['name']}`}
            });
            const categories = Object.keys(focalPoints['districts']).map(key => focalPoints['districts'][key]['category']).filter((x, i, a) => a.indexOf(x) === i);
            const p2 = categories.map(category => {
                return {key: `DIST_DST_${category}`, width: 15, horizontal: 'right', title: `DIST_DST_${category}`, description: `Distance par rapport au District de catégorie ${category} le plus proche`}
            });
            distDistrictsCols = [...p1, ...p2]
        }
    }
    
    let columns = [
        { key: 'operation_id', width: 15, horizontal: 'left', title: 'OPE_ID', description: 'Identifiant de l\'opération' },
        { key: 'operation_hash', width: 15, horizontal: 'left', title: 'OPE_HASH', description: 'Identifiant de l\'opération dans la blockchain' },
        { key: 'operation_type', width: 15, horizontal: 'left', title: 'OPE_TYPE', description: 'Type de l\'opération (Free = Cession, Sale = Vente)' },
        { key: 'collection', width: 15, horizontal: 'left', title: 'COLLECTION', description: 'Collection ou Plateforme métaverse' },
        { key: 'blockchain', width: 15, horizontal: 'left', title: 'BLOCKCHAIN', description: 'Environnement blockchain' },
        { key: 'mvt_date', width: 15, horizontal: 'left', title: 'OPE_DATE', description: 'Date de l\'opération' },
        { key: 'sender', width: 15, horizontal: 'left', title: 'SENDER', description: 'Expéditeur' },
        { key: 'recipient', width: 15, horizontal: 'left', title: 'RECIPIENT', description: 'Récipiendaire' },
        { key: 'asset_id', width: 15, horizontal: 'left', title: 'ASSET_ID', description: 'Identifiant de l\'actif transigé' },
        { key: 'asset_name', width: 15, horizontal: 'left', title: 'ASSET_NAME', description: 'Nom de l\'actif transigé' },
        { key: 'asset_description', width: 15, horizontal: 'left', title: 'ASSET_DES', description: 'Description de l\'actif transigé' },
        { key: 'asset_type', width: 15, horizontal: 'left', title: 'ASSET_TYPE', description: 'Type de l\'actif transigé (Land = Parcelle, Estate = Domaine ou ensemble de parcelles)' },
        { key: 'asset_coord_x', width: 15, horizontal: 'right', title: 'COORD_X', description: 'Coordonnée X de l\'actif transigé dans la map du métavers' },
        { key: 'asset_coord_y', width: 15, horizontal: 'right', title: 'COORD_Y', description: 'Coordonnée Y de l\'actif transigé dans la map du métavers' },
        { key: 'asset_size', width: 15, horizontal: 'right', title: 'SIZE', description: 'Taille de l\'actif transigé (en unité de taille dans le métavers)' },
        { key: 'asset_lands', width: 15, horizontal: 'left', title: 'LANDS', description: 'Liste des parcelles incluses dans l\'actif transigé (Dans le cas ou type = estate)' },
        ...distPlazasCols,
        ...distRoadsCols,
        ...distDistrictsCols,
        //{ key: 'asset_owner', width: 15, horizontal: 'left', title: 'OWNER', description: 'Propriétaire actuel de l\'actif transigé' },
        { key: 'asset_prev_free_ops', width: 15, horizontal: 'right', title: 'PV_FREE_OPS', description: 'Nombre de cessions de l\'actif transigé avant opération' },
        { key: 'asset_prev_sale_ops', width: 15, horizontal: 'right', title: 'PV_SALE_OPS', description: 'Nombre de ventes de l\'actif transigé avant opération' },
        { key: 'asset_lands_prev_free_ops', width: 15, horizontal: 'right', title: 'LANDS_PV_FREE_OPS', description: 'Nombre de cessions des parcelles includes dans l\'actif transigé avant opération' },
        { key: 'asset_lands_prev_sale_ops', width: 15, horizontal: 'right', title: 'LANDS_PV_SALE_OPS', description: 'Nombre de ventes des parcelles includes dans l\'actif transigé avant opération' },
        { key: 'amount_1_raw', width: 15, horizontal: 'right', title: 'AMT_1_RAW', description: 'Montant en Crypto 1 utilisée dans l\'opération' },
        { key: 'amount_1_ccy', width: 15, horizontal: 'right', title: 'AMT_1_CCY', description: 'Nom de la Crypto 1 utilisée dans l\'opération' },
        { key: 'amount_1_usd', width: 15, horizontal: 'right', title: 'AMT_1_USD', description: 'Equivalent en USD de la qté de Crypto 1 utilisée l\'opération' },
        { key: 'amount_2_raw', width: 15, horizontal: 'right', title: 'AMT_2_RAW', description: 'Montant en Crypto 2 utilisée dans l\'opération' },
        { key: 'amount_2_ccy', width: 15, horizontal: 'right', title: 'AMT_2_CCY', description: 'Nom de la Crypto 2 utilisée dans l\'opération' },
        { key: 'amount_2_usd', width: 15, horizontal: 'right', title: 'AMT_2_RAW', description: 'Equivalent en USD de la qté de Crypto 2 utilisée l\'opération' },
        { key: 'amount_usd', width: 15, horizontal: 'right', title: 'AMT_TOT_USD', description: 'Montant total en USD échangé contre l\'actif transigé' },
        { key: 'fees_raw', width: 15, horizontal: 'right', title: 'FEES_RAW', description: 'Frais en crypto de la blockchain encourus par l\'opération' },
        { key: 'fees_ccy', width: 15, horizontal: 'right', title: 'FEES_CCY', description: 'Crypto de la blockchain en laquelle les frais ont été prélevés' },
        { key: 'fees_usd', width: 15, horizontal: 'right', title: 'FEES_USD', description: 'Frais en USD encourus par l\'opération' },
        { key: 'blockchain_crypto_price_1', width: 15, horizontal: 'right', title: 'BLC_CRYP_1_PRICE', description: 'Cours de la crypto de l\'env Blockchain 1 support du métavers (ETH/Ethereum pour Decentraland, Somnium Space et CryptoVoxels, | MATIC/Polygon pour TheSandbox)' },
        { key: 'blockchain_crypto_price_2', width: 15, horizontal: 'right', title: 'BLC_CRYP_2_PRICE', description: 'Cours de la crypto de l\'env Blockchain 1 support du métavers (ETH/Ethereum pour TheSandbox)' },
        { key: 'collection_crypto_name', width: 15, horizontal: 'left', title: 'COL_CRYP_NAME', description: 'Nom de la crypto associée au projet du métavers' },
        { key: 'collection_crypto_price', width: 15, horizontal: 'right', title: 'COL_CRYP_PRICE', description: 'Cours de la crypto du métavers au moment de la transaction' },
        { key: 'collection_crypto_change_24h', width: 15, horizontal: 'right', title: 'COL_CRYP_CHANGE_24H', description: 'Variation de prix sur 24H de la crypto du métavers à la clôture la veille du jour de la transaction' },
        { key: 'collection_crypto_volume_24h', width: 15, horizontal: 'right', title: 'COL_CRYP_VOLUME_24H', description: 'Volume échangé en USD, de la crypto du métavers à la clôture de la veille du jour de la transaction' },
        { key: 'collection_crypto_market_cap', width: 15, horizontal: 'right', title: 'COL_CRYP_MARKET_CAP', description: 'Capitalisation boursière de la crypto de la crypto du métavers la veille du jour de la transaction' },
    ]

    let ddWorksheet = workbook.addWorksheet("DATA DESCRIPTION", {views: [{showGridLines: true}]});
    const ddColumns = [
        { key: 'name', title: 'Nom Colonne', horizontal: 'left' },
        { key: 'description', title: 'Description Colonne', horizontal: 'left' },
    ];
    const ddData = columns.map(c => {
        return {
            name: c.title,
            description: c.description
        }
    });

    let dataWorksheet = workbook.addWorksheet("DATA", {views: [{showGridLines: true}]});

    excel_writeWorksheet(ddWorksheet, ddColumns, ddData);
    excel_writeWorksheet(dataWorksheet, columns, lines);

    await workbook.xlsx.writeFile(`./MetaversesDataset.xlsx`);
}

function helper() {
    console.log(`Usage: node main.js -c <Collection> -s <from_date (DD/MM/YYYY)> -e <To Date (DD/MM/YYYY)> -t <Operation Type> -l <Limit> -d <Distance Type (manhattan, euclidean)>`)
}

function handleArguments() {
    var argv = minimist(process.argv.slice(2));
    if (!argv || !argv['c'] || argv['h']) {
        helper();
        return false;
    }
    return {
        collection: argv['c'],
        fdate: argv['s'],
        tdate: argv['e'],
        optype: argv['t'],
        distance_type: argv['d']?.toString()?.toLowerCase() || 'manhattan',
        limit: argv['l'] && !isNaN(parseInt(argv['l'], 10)) && parseInt(argv['l'], 10) > 0 ? parseInt(argv['l']) : undefined
    }
}

async function main() {
    const params = handleArguments();
    if (!params) {
        return false;
    }

    const distanceType = params['distance_type'];

    console.log(process.env.DATABASE_URL);
    await mongoose.connect(process.env.DATABASE_URL, {dbName: process.env.DATABASE_NAME});
    console.info(`${new Date()} - Connected to database !!`);

    console.info(`${new Date()} - Fetching Operations...`);
    const operations = await fetchAllData(params);
    console.info(`${new Date()} - Operations fetched successfully. ${operations.length} operations !!!`);
    
    await mongoose.disconnect();
    console.info(`${new Date()} - Disconnected from datasbase successfully !!`);

    const lines = [];
    console.info(`${new Date()} - Parsing Data...`);
    const count = operations.length;
    let i = 0;
    for (const operation of operations) {
        console.info(`${new Date()} - Parsing Data - Operation ${operation['_id'].toString()} (${(i+1)}/${count}) Begin...`);
        const line = linifyOperation(operation, operations, distanceType);
        console.info(`${new Date()} - Parsing Data - Operation ${operation['_id'].toString()} (${(i+1)}/${count}) DONE !!!`);
        i++;
        lines.push(line);
    }
    console.info(`${new Date()} - Data parsed successfully !!!`);

    console.info(`${new Date()} - Write Excel File`);
    await excelFile(lines);
    console.info(`${new Date()} - Excel Written successfully !!`);

    return true;
}

main()
    .then((done) => { 
        if (done) {
            setTimeout(() => {process.exit(0)}, 24*60*60*1000)
        }
        else {
            process.exit(0)
        }
     })
    .catch(e => console.error(e));
