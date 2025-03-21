const {Asset, AssetMetadata, Operation, CurrencyPrice} = require('./models.js');
const ExcelJS = require('exceljs');
const mongoose = require('mongoose');
const minimist = require('minimist');
require('dotenv').config();

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
    let request =  Operation.find(payload).sort('mvt_date');
    if (params['limit']) {
        request = request.limit(params['limit'])
    }
    return await request.allowDiskUse(true).exec();
}

async function operation_async_getActualMetadata(operation) {
    const payload = [
        {
            $match: {
                asset: operation['asset'],
                $or: [{ date: null }, { date: { $lt: operation.mvt_date } }],
            },
        },
        {
            $group: {
                _id: {
                    asset: '$asset',
                    category: '$category',
                    macro_type: '$macro_type',
                    macro_subtype: '$macro_subtype'
                },
                lastDate: { $max: '$date' },
            },
        }
    ]
    const lastMetadata = await AssetMetadata.aggregate(payload).exec();
    const assetMetadataOperationList = [];
    for (const metadatumInf of lastMetadata) {
        const metadatum = await AssetMetadata
            .findOne({asset: metadatumInf._id.asset, category: metadatumInf._id.category, macro_type: metadatumInf._id.macro_type, macro_subtype: metadatumInf._id.macro_subtype, date: metadatumInf.lastDate})
            .exec();
        assetMetadataOperationList.push(metadatum);
    }
    return assetMetadataOperationList;
}

async function operation_async_getAsset(operation) {
    return await Asset.findOne({_id: operation['asset']}).exec();
}

async function asset_async_getAssetsByCoordinates(collection, coordinates) {
    const orPayload = (coordinates || [])
        .map(c => ({x: parseInt(c.split(",")[0], 10), y: parseInt(c.split(",")[1], 10)}))
        .filter(c => !isNaN(c.x) && !isNaN(c.y))
    const payload = {collection, $or: orPayload}
    return await Asset.distinct('_id', payload).exec();
}

async function asset_async_getNumberOfPreviousOperations(asset, operation, assetMetadataOperationList) {
    const nbPrevFreeOps = await Operation
        .countDocuments({asset: operation['asset'], mvt_date: {$lt: operation['mvt_date']}, operation_type: "free"})
        .allowDiskUse(true).exec();
    const nbPrevSaleOps = await Operation
        .countDocuments({asset: operation['asset'], mvt_date: {$lt: operation['mvt_date']}, operation_type: "sale"})
        .allowDiskUse(true).exec();
    let nbPrevLandsFreeOps = 0;
    let nbPrevLandsSaleOps = 0;
    if (asset.type === "estate") {
        let lands = asset_getMetadataValue(assetMetadataOperationList, asset, "lands");
        if (lands) {
            lands = lands.split("|");
            const landsAssetsIds = await asset_async_getAssetsByCoordinates(operation.collection, lands);
            nbPrevLandsFreeOps = await Operation
                .countDocuments({asset: {$in: landsAssetsIds}, mvt_date: {$lt: operation['mvt_date']}, operation_type: "free"})
                .allowDiskUse(true).exec();
            nbPrevLandsSaleOps = await Operation
                .countDocuments({asset: {$in: landsAssetsIds}, mvt_date: {$lt: operation['mvt_date']}, operation_type: "sale"})
                .allowDiskUse(true).exec();
        }
    }
    return {nbPrevFreeOps, nbPrevSaleOps, nbPrevLandsFreeOps, nbPrevLandsSaleOps}
}

function asset_getMetadataValue_in_list(assetMetadataOperationList, category, macro_type, macro_subtype) {
    const metadatum = (assetMetadataOperationList || []).find(m => {
        let flag = m.category === category;
        if (macro_type) {
            flag = flag && (m.macro_type === macro_type && (macro_subtype ? m.macro_subtype === macro_subtype : true));
        }
        return flag;
    });
    if (metadatum) {
        if (metadatum.data_type === "boolean") return metadatum.value?.toString()?.toLowerCase() === "true";
        else if (metadatum.data_type === "integer") return metadatum.value ? parseInt(metadatum.value.toString(), 10) : null;
        else if (metadatum.data_type === "float") return metadatum.value ? parseFloat(metadatum.value.toString()) : null;
        else if (metadatum.data_type === "address") return metadatum.value ? prettifyAddress(metadatum.value) : null;
        else return metadatum.value;
    }
    return null;
}

function asset_getMetadataValue(assetMetadataOperationList, asset, metadataName) {
    if (metadataName === "coordinate_x") {
        return asset.x;
    }
    if (metadataName === "coordinate_y") {
        return asset.y;
    }
    if (metadataName === "size") {
        if (asset.type === "land") return 1;
        return asset_getMetadataValue_in_list(assetMetadataOperationList, "size", null);
    }
    if (metadataName === "lands") {
        if (asset.type === "land") return null;
        return asset_getMetadataValue_in_list(assetMetadataOperationList, "lands", null);
    }
    if (metadataName.startsWith("distance")) {
        const params = metadataName.split("_")
        const macroType = params[1], macroSubtype = params[2];
        return asset_getMetadataValue_in_list(assetMetadataOperationList, "distance", macroType, macroSubtype);
    }
    if (metadataName === "owner") {
        return asset_getMetadataValue_in_list(assetMetadataOperationList, "owner", null);
    }
    return null;
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

async function find_currency_price(currency, date) {
    const cp = await CurrencyPrice.findOne({currency: currency, start: {$lte: date}, end: {$gt: date}}).exec();
    return cp?.close;
}

async function get_collection_crypto_price(operation) {
    if (operation.collection === "decentraland") {
        return await find_currency_price("MANA", operation.mvt_date);
    }
}

async function get_blockchain_crypto_prices(operation) {
    let bcPrice1 = null, bcPrice2 = null;
    if (operation.collection === "decentraland") {
        bcPrice1 = operation['fees']?.[0]?.['currency_price'];
    }
    return {bcPrice1, bcPrice2};
}

function get_operation_hyperlink(operation) {
    if (operation['blockchain'] === "ethereum") {
        return `https://www.etherscan.io/tx/${operation['transaction_hash']}`;
    }
    return null;
}

function prettifyAddress(address) {
    return address.substring(0, 8) + "..." + address.substring(address.length-4);
}

async function linifyOperation(operation) {
    const asset = await operation_async_getAsset(operation);
    const assetMetadataOperationList = await operation_async_getActualMetadata(operation);
    const {nbPrevFreeOps, nbPrevSaleOps, nbPrevLandsFreeOps, nbPrevLandsSaleOps} =
        await asset_async_getNumberOfPreviousOperations(asset, operation, assetMetadataOperationList);
    const {
        amount1Raw, amount1CCy, amount1Usd, amount2Raw, amount2CCy, amount2Usd, amountTotal,
        feesRaw, feesCcy, feesUsd
    } = operation_getAmount(operation);
    const {bcPrice1, bcPrice2} = await get_blockchain_crypto_prices(operation);
    const colCrypPrice = await get_collection_crypto_price(operation);
    return {
        operation_id: operation['_id'].toString(),
        operation_type:  operation['operation_type'],
        collection: operation['collection'],
        blockchain: operation['blockchain'],
        mvt_date: operation['mvt_date'],
        sender: prettifyAddress(operation['sender']),
        recipient: prettifyAddress(operation['recipient']),
        asset_id: operation['asset'].toString(),
        asset_name: asset['name'],
        asset_description: asset['description'],
        asset_type: asset['type'],
        asset_coord_x: asset_getMetadataValue(assetMetadataOperationList, asset, "coordinate_x"),
        asset_coord_y: asset_getMetadataValue(assetMetadataOperationList, asset, "coordinate_y"),
        asset_size: asset_getMetadataValue(assetMetadataOperationList, asset, "size"),
        asset_lands: asset_getMetadataValue(assetMetadataOperationList, asset, "lands"),
        asset_distance_plaza_c: asset_getMetadataValue(assetMetadataOperationList, asset, "distance_plaza_Central Genesis Plaza"),
        asset_distance_plaza_n: asset_getMetadataValue(assetMetadataOperationList, asset, "distance_plaza_North Genesis Plaza"),
        asset_distance_plaza_s: asset_getMetadataValue(assetMetadataOperationList, asset, "distance_plaza_South Genesis Plaza"),
        asset_distance_plaza_e: asset_getMetadataValue(assetMetadataOperationList, asset, "distance_plaza_East Genesis Plaza"),
        asset_distance_plaza_w: asset_getMetadataValue(assetMetadataOperationList, asset, "distance_plaza_West Genesis Plaza"),
        asset_distance_plaza_nw: asset_getMetadataValue(assetMetadataOperationList, asset, "distance_plaza_North-West Genesis Plaza"),
        asset_distance_plaza_ne: asset_getMetadataValue(assetMetadataOperationList, asset, "distance_plaza_North-East Genesis Plaza"),
        asset_distance_plaza_sw: asset_getMetadataValue(assetMetadataOperationList, asset, "distance_plaza_South-West Genesis Plaza"),
        asset_distance_plaza_se: asset_getMetadataValue(assetMetadataOperationList, asset, "distance_plaza_South-East Genesis Plaza"),
        asset_distance_district_ce: asset_getMetadataValue(assetMetadataOperationList, asset, "distance_district_District [Culture and Education]"),
        asset_distance_district_pl: asset_getMetadataValue(assetMetadataOperationList, asset, "distance_district_District [Politics]"),
        asset_distance_district_gm: asset_getMetadataValue(assetMetadataOperationList, asset, "distance_district_District [Business]"),
        asset_distance_district_bs: asset_getMetadataValue(assetMetadataOperationList, asset, "distance_district_District [Gaming]"),
        asset_distance_district_sm: asset_getMetadataValue(assetMetadataOperationList, asset, "distance_district_District [Small District]"),
        asset_distance_road: asset_getMetadataValue(assetMetadataOperationList, asset, "distance_road"),
        asset_owner: asset_getMetadataValue(assetMetadataOperationList, asset, "owner"),
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
        //collection_crypto_price: colCrypPrice,
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

    let columns = [
        { key: 'operation_id', width: 15, horizontal: 'left', title: 'OPE_ID', description: 'Identifiant de l\'opération' },
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
        { key: 'asset_distance_plaza_c', width: 15, horizontal: 'right', title: 'DIST_CENTRAL_PLAZA', description: 'Distance entre l\'actif transigé et le PLAZA Central' },
        { key: 'asset_distance_plaza_n', width: 15, horizontal: 'right', title: 'DIST_NORTH_PLAZA', description: 'Distance entre l\'actif transigé et le PLAZA Périphérique Nord' },
        { key: 'asset_distance_plaza_s', width: 15, horizontal: 'right', title: 'DIST_SOUTH_PLAZA', description: 'Distance entre l\'actif transigé et le PLAZA Périphérique Sud' },
        { key: 'asset_distance_plaza_e', width: 15, horizontal: 'right', title: 'DIST_EAST_PLAZA', description: 'Distance entre l\'actif transigé et le PLAZA Périphérique Est' },
        { key: 'asset_distance_plaza_w', width: 15, horizontal: 'right', title: 'DIST_WEST_PLAZA', description: 'Distance entre l\'actif transigé et le PLAZA Périphérique Ouest' },
        { key: 'asset_distance_plaza_ne', width: 15, horizontal: 'right', title: 'DIST_NORTH_EAST_PLAZA', description: 'Distance entre l\'actif transigé et le PLAZA Périphérique Nord-Est' },
        { key: 'asset_distance_plaza_nw', width: 15, horizontal: 'right', title: 'DIST_NORTH_WEST_PLAZA', description: 'Distance entre l\'actif transigé et le PLAZA Périphérique Nord-Ouest' },
        { key: 'asset_distance_plaza_se', width: 15, horizontal: 'right', title: 'DIST_SOUTH_EAST_PLAZA', description: 'Distance entre l\'actif transigé et le PLAZA Périphérique Sud-Est' },
        { key: 'asset_distance_plaza_sw', width: 15, horizontal: 'right', title: 'DIST_SOUTH_WEST_PLAZA', description: 'Distance entre l\'actif transigé et le PLAZA Périphérique Sud-Ouest' },
        { key: 'asset_distance_district_ce', width: 15, horizontal: 'right', title: 'DIST_DST_CULT_EDU', description: 'Distance entre l\'actif transigé et le DISTRICT Culture et Education le plus proche' },
        { key: 'asset_distance_district_pl', width: 15, horizontal: 'right', title: 'DIST_DST_POLITIC', description: 'Distance entre l\'actif transigé et le DISTRICT Politique le plus proche' },
        { key: 'asset_distance_district_gm', width: 15, horizontal: 'right', title: 'DIST_DST_GAMING', description: 'Distance entre l\'actif transigé et le DISTRICT Gaming le plus proche' },
        { key: 'asset_distance_district_bs', width: 15, horizontal: 'right', title: 'DIST_DST_BUSINESS', description: 'Distance entre l\'actif transigé et le DISTRICT Business le plus proche' },
        { key: 'asset_distance_district_sm', width: 15, horizontal: 'right', title: 'DIST_DST_SMALL', description: 'Distance entre l\'actif transigé et le DISTRICT de petite taille le plus proche' },
        { key: 'asset_distance_road', width: 15, horizontal: 'right', title: 'DIST_ROAD', description: 'Distance entre l\'actif transigé et la PISTE DE ROUTE la plus proche' },
        { key: 'asset_owner', width: 15, horizontal: 'left', title: 'OWNER', description: 'Propriétaire actuel de l\'actif transigé' },
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
    console.log(`Usage: node main.js -c <Collection> -a <Asset Contract> -s <from_date (DD/MM/YYYY)> -e <To Date (DD/MM/YYYY)> -t <Operation Type> -l <Limit>`)
}

function handleArguments() {
    var argv = minimist(process.argv.slice(2));
    if (!argv || !argv['c'] || argv['h']) {
        helper();
        return false;
    }
    return {
        collection: argv['c'],
        asset: argv['a'],
        fdate: argv['s'],
        tdate: argv['e'],
        optype: argv['t'],
        limit: argv['l'] && !isNaN(parseInt(argv['l'], 10)) && parseInt(argv['l'], 10) > 0 ? parseInt(argv['l']) : undefined
    }
}

async function main() {
    const params = handleArguments();
    if (!params) {
        return false;
    }

    console.log(process.env.DATABASE_URL);
    await mongoose.connect(process.env.DATABASE_URL, {dbName: process.env.DATABASE_NAME});
    console.info(`${new Date()} - Connected to database !!`);

    console.info(`${new Date()} - Fetching Operations...`);
    const operations = await fetchAllData(params);
    console.info(`${new Date()} - Operations fetched successfully. ${operations.length} operations !!!`);

    const lines = [];
    console.info(`${new Date()} - Parsing Data...`);
    const count = operations.length;
    let i = 0;
    for (const operation of operations) {
        console.info(`${new Date()} - Parsing Data - Operation ${operation['_id'].toString()} (${(i+1)}/${count}) Begin...`);
        const line = await linifyOperation(operation);
        console.info(`${new Date()} - Parsing Data - Operation ${operation['_id'].toString()} (${(i+1)}/${count}) DONE !!!`);
        i++;
        lines.push(line);
    }
    console.info(`${new Date()} - Data parsed successfully !!!`);

    console.info(`${new Date()} - Write Excel File`);
    await excelFile(lines);
    console.info(`${new Date()} - Excel Written successfully !!`);

    await mongoose.disconnect();
    console.info(`${new Date()} - Disconnected from datasbase successfully !!`);

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
