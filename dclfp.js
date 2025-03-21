const fs = require('fs');

const tilesFile = './files/latest.json';
const districtsFile = './files/districts.json';
const plazasFile = './files/plaza.json';

function logLine (text) {
    console.info(`${new Date().toLocaleString()} --> ${text}`);
}

function districtNameToId (disName) {
    const parts = disName.split(" ");
    let p1 = parts[0], p2 = parts[1], p3 = parts[2];
    let id = p1.substring(0, Math.min(4, p1.length));
    if (p2) {
        if (p2.length < 2) {
            if (p3 && p3.length > 2) p2 = p3;
            else p2 = null;
        }
    }
    if (p2) {
        id = id + "_" + p2.substring(0, Math.min(4, p2.length))
    }
    return id.toString().toUpperCase();
}

function organizeFocalPoints () {
    logLine('Read files...');
    let tiles = JSON.parse(fs.readFileSync(tilesFile, {encoding: 'utf-8'}));
    let districts = JSON.parse(fs.readFileSync(districtsFile, {encoding: 'utf-8'}));
    let plazas = JSON.parse(fs.readFileSync(plazasFile, {encoding: 'utf-8'}));

    logLine('Filter tiles...');
    tiles = Object.keys(tiles.data || {}).map(key => tiles.data[key]).filter(x => ['road', 'plaza', 'district'].includes(x['type'])).map(tile => ({xy: [tile['x'], tile['y']], nftId: tile['nftId'], type: tile['type'], estateId: tile['estateId']}));


    logLine('Organize Plazas...');
    const plazaFocalPoints = {};
    for(const plazaId in plazas) {
        if (plazas.hasOwnProperty(plazaId)) {
            const plazaTiles = tiles.filter(x => x['estateId'] === plazaId);
            plazaFocalPoints[plazas[plazaId]['id']] = {
                estateId: plazaId,
                id: plazas[plazaId]['id'],
                name: plazas[plazaId]['name'],
                parcelsNb: plazaTiles.length,
                parcelsXY: plazaTiles.map(x => x['xy']),
                parcels: plazaTiles,
            };
        }
    }

    logLine('Organize Roads...');
    const roads = tiles.filter(x => x['type'] === "road").map(x => x['estateId']).filter((x, i, a) => a.indexOf(x) === i);
    const roadFocalPoints = {};
    for(const roadId of roads) {
        const roadTiles = tiles.filter(x => x['estateId'] === roadId);
        roadFocalPoints[roadId] = {
            estateId: roadId,
            id: roadId,
            name: `Road ${roadId}`,
            parcelsNb: roadTiles.length,
            parcelsXY: roadTiles.map(x => x['xy']),
            parcels: roadTiles
        };
    }
    /*const allRoads = {
        estateId: null,
        id: "roads",
        name: "All roads",
        parcelsXY: Object.keys(roadFocalPoints).map(key => roadFocalPoints[key]['parcelsXY']).reduce((a, b) => [...a, ...b], []);
    }
    roadFocalPoints["roads"] = allRoads;*/

    logLine('Organize Districts...');
    districts = districts.data;
    const districtFocalPoints = {};
    for (const district of districts) {
        if (!['Genesis Plaza', 'Road'].includes(district['category'])) {
            let category = null;
            if (district['category'] === 'Culture and Education') category = "CULT_EDU";
            else if (district['category'] === 'Politics') category = "POLITIC";
            else if (district['category'] === 'Gaming') category = "GAMING";
            else if (district['category'] === 'Business') category = "BUSINESS";
            else if (district['category'] === 'Small District') category = "SMALL";
            districtFocalPoints[district['id']] = {
                estateId: null,
                id: districtNameToId(district['name']),
                name: district['name'],
                description: district['description'],
                parcelsNb: (district['parcels'] || []).length,
                parcelsXY: (district['parcels'] || []).map(p => p.split(",").map(a => parseInt(a, 10))),
                parcels: [],
                category
            };
        }
    }

    logLine(`Write in file...`);
    const rendered = {
        plazas: plazaFocalPoints,
        roads: roadFocalPoints,
        districts: districtFocalPoints
    }
    fs.writeFileSync('./files/dcl_focal_points.json', JSON.stringify(rendered, null, 4));
}

organizeFocalPoints();