const puppeteer = require('puppeteer');

(async () => {
    const dir = `public/screenshots`
    const screens = {
        'ctbk-rides': { query: '', height: 540, },
        'ctbk-nj': { query: '?r=jh', },
        'ctbk-stations': { query: 'stations?ll=40.732_-74.025&z=13.5&ss=HB102', width: 800, height: 800, selector: '.leaflet-lines-pane svg path' },
        'ctbk-ride-minutes-by-gender': { query: '?y=m&s=g&pct=&g=mf&d=1406-2101', },
        'ctbk-rides-by-user': { query: '?s=u&pct=', },
        'ctbk-ebike-minutes': { query: '?y=m&s=b&rt=ce', },
    }

    const browser = await puppeteer.launch();
    const page = await browser.newPage();

    const items = Array.from(Object.entries(screens))
    for (let [ name, { query, width, height, selector } ] of items) {
        width = width || 800
        height = height || 580
        selector = selector || '.plotly svg rect'
        const url = `https://ctbk.dev/${query}`
        console.log(`Loading ${url}`)
        await page.goto(url);
        console.log(`Loaded ${url}`)

        await page.setViewport({ width, height });
        console.log("setViewport")
        await page.waitForSelector(selector);
        console.log("selector")
        const path = `${dir}/${name}.png`
        await page.screenshot({ path });
    }

    await browser.close();
})();
