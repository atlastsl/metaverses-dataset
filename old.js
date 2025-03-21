function asset_numberOfPreviousOperations(operation, asset, operations, assets, assetMetadataOperationList) {
    const previousOps = (operations || []).filter(
        x => x['asset'].toString() === asset['_id'].toString() && x['mvt_date'].getTime() < operation['mvt_date'].getTime()
    )
    const nbPrevFreeOps = previousOps.filter(x => x['operation_type'] === "free").length;
    const nbPrevSaleOps = previousOps.filter(x => x['operation_type'] === "sale").length;
    let nbPrevLandsFreeOps = 0;
    let nbPrevLandsSaleOps = 0;
    if (asset.type === "estate") {
        let lands = asset_getMetadataValue(assetMetadataOperationList, asset, "lands");
        if (lands) {
            lands = lands.split("|");
            const landsAssetsIds = asset_getAssetsByCoordinates(assets, lands);
            const landsPrevOps = (operations || []).filter(
                x => landsAssetsIds.includes(x['asset'].toString()) && x['mvt_date'].getTime() < operation['mvt_date'].getTime()
            );
            nbPrevLandsFreeOps = landsPrevOps.filter(x => x['operation_type'] === "free").length;
            nbPrevLandsSaleOps = landsPrevOps.filter(x => x['operation_type'] === "sale").length;
        }
    }
    return {nbPrevFreeOps, nbPrevSaleOps, nbPrevLandsFreeOps, nbPrevLandsSaleOps}
}

function operation_getActualMetadata(assetsMetadata, operation) {
    const assetMetadataRawList = (assetsMetadata || []).filter(metadatum => {
        return metadatum['asset'].toString() === operation['asset'].toString() && 
            (!metadatum['date'] || metadatum['date'].getTime() < operation['mvt_date'].getTime())
    });
    const assetMetadataBcList = [];
    const assetMetadataBcListGroupsIds = {};
    for (const metadatum of assetMetadataRawList) {
        const groupId = metadatum['category'] + "_" + (metadatum['macro_type'] || "");
        if (!assetMetadataBcListGroupsIds[groupId]) {
            assetMetadataBcList.push([]);
            assetMetadataBcListGroupsIds[groupId] = assetMetadataBcList.length - 1;
        }
        assetMetadataBcList[assetMetadataBcListGroupsIds[groupId]].push(metadatum);
    }
    const assetMetadataOperationList = [];
    for (const assetMetadataGroupList of assetMetadataBcList) {
        assetMetadataGroupList.sort((a, b) => {return (a['date'].getTime() || 0) > (b['date'].getTime() || 0) ? -1 : 1});
        assetMetadataOperationList.push(assetMetadataGroupList[0]);
    }
    return assetMetadataOperationList;
}

function operation_getAsset(assets, operation) {
    return (assets || []).find(x => x._id.toString() === operation['asset'].toString());
}

function asset_getAssetsByCoordinates(assets, coordinates) {
    return (assets || []).filter(a => coordinates.includes(`${a.x},${a.y}`)).map(a => a['_id'].toString())
}