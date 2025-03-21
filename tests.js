const Decimal = require('decimal.js');

function main () {
    const x = 1, y = 1, x2 = 3, y2 = 3;
    let x1bn = new Decimal(x), y1bn = new Decimal(y), x2bn = new Decimal(x2), y2bn = new Decimal(y2); 
    const dx = x1bn.sub(x2bn).pow(2), dy = y1bn.sub(y2bn).pow(2);
    const r = dx.add(dy).sqrt().toDP(7, Decimal.ROUND_DOWN).toNumber()
    console.log(r);
}

main();