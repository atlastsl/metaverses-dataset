const mongoose = require('mongoose');

const OperationSchema = new mongoose.Schema({
    collection: {type: String},
    asset: {type: mongoose.Schema.Types.ObjectId, ref: 'Asset'},
    asset_contract: {type: String},
    asset_id: {type: String},
    transaction_hash: {type: String},
    operation_type: {type: String, enum: ['free', 'sale']},
    transaction_type: {type: String, enum: ['mint', 'transfer']},
    blockchain: {type: String},
    block_number: {type: Number},
    block_hash: {type: String},
    mvt_date: {type: Date},
    sender: {type: String},
    recipient: {type: String},
    amount: [{
        value: {type: Number},
        currency: {type: String},
        currency_price: {type: Number},
        value_usd: {type: Number}
    }],
    fees: [{
        value: {type: Number},
        currency: {type: String},
        currency_price: {type: Number},
        value_usd: {type: Number}
    }],
    market_info: {
        currency: {type: String},
        price: {type: Number},
        change_24h: {type: Number},
        volume_24h: {type: Number},
        market_cap: {type: Number},
    }
}, {
    collection: 'operations'
});
const Operation = mongoose.model('Operation', OperationSchema);

const AssetSchema = new mongoose.Schema({
    asset_id: {type: String},
    collection: {type: String},
    contract: {type: String},
    token_standard: {type: String, enum: ['erc721']},
    name: {type: String},
    description: {type: String},
    blockchain: {type: String},
    type: {type: String, enum: ['land', 'estate', 'district']},
    x: {type: Number},
    y: {type: Number},
    urls: [{
        name: {type: String},
        url: {type: String}
    }]
}, {
    collection: 'assets'
});
const Asset = mongoose.model('Asset', AssetSchema);

const AssetMetadataSchema = new mongoose.Schema({
    collection: {type: String},
    asset: {type: mongoose.Schema.Types.ObjectId, ref: 'Asset'},
    asset_contract: {type: String},
    asset_name: {type: String},
    category: {type: String, enum: ['coordinates', 'size', 'distance', 'owner', 'lands']},
    name: {type: String},
    display_name: {type: String},
    data_type: {type: String, enum: ['integer', 'float', 'bool', 'string', 'string-array', 'address']},
    value: {type: String},
    macro_type: {type: String, enum: ['plaza', 'road', 'district']},
    macro_subtype: {type: String},
    date: {type: Date},
    operations: [{type: mongoose.Schema.Types.ObjectId, ref: 'Operation'}]
}, {
    collection: 'asset_metadata'
});
const AssetMetadata = mongoose.model('AssetMetadata', AssetMetadataSchema);

const CurrencyPriceSchema = new mongoose.Schema({
    currency: {type: String},
    start: {type: Date},
    end: {type: Date},
    open: {type: Number},
    close: {type: Number},
    high: {type: Number},
    low: {type: Number},
    avg: {type: Number},
    volume: {type: Number},
    market_cap: {type: Number},
}, {
    collection: 'currency_prices'
});
const CurrencyPrice = mongoose.model('CurrencyPrice', CurrencyPriceSchema);

module.exports = {
    Asset, AssetMetadata, Operation, CurrencyPrice
}
