#!/usr/bin/env node

const puppeteer = require('puppeteer');
const program = require('commander');

const cwd = __dirname

const options =
    program
        .option('-h, --host <host or port>', 'Hostname to load screenshots from; numeric <port> is mapped to 127.0.0.1:<port>')
        .option('-i, --include <regex>', 'Only generate screenshots whose name matches this regex')
        .parse(process.argv)
        .opts()

let scheme
let {host, include} = options
if (host) {
    scheme = 'http'
    if (host.match(/^\d+$/)) {
        host = `127.0.0.1:${host}`
    }
} else {
    host = 'ctbk.dev'
    scheme = 'https'
}
console.log("host:", host, "includes:", include);

(async () => {
    const dir = `${cwd}/public/screenshots`
    const screens = {
        'ctbk-rides': { query: '', height: 540, },
        'ctbk-nj': { query: '?r=jh', },
        'ctbk-stations': { query: 'stations?ll=40.732_-74.025&z=13.5&ss=HB102', width: 800, height: 800, selector: '.leaflet-lines-pane svg path' },
        'ctbk-ride-minutes-by-gender': { query: '?y=m&s=g&pct=&g=mf&d=1406-2102', },
        'ctbk-rides-by-user': { query: '?s=u&pct=', },
        'ctbk-ebike-minutes': { query: '?y=m&s=b&rt=ce', },
        'ctbk-ebike-minutes-by-user': { query: '?y=m&s=u&rt=e', },
        'plot-fallback': { query: '?dl=1', download: true },
    }

    const browser = await puppeteer.launch();
    const page = await browser.newPage();

    const items = Array.from(Object.entries(screens))
    for (let [ name, { query, width, height, selector, download } ] of items) {
        if (include && !name.match(include)) {
            console.log(`Skipping ${name}`)
            continue
        }
        width = width || 800
        height = height || 580
        selector = selector || '.plotly svg rect'
        const url = `${scheme}://${host}/${query}`
        const path = `${dir}/${name}.png`
        if (download) {
            console.log(`Setting download behavior to ${dir}`)
            await page._client().send('Page.setDownloadBehavior', {
                behavior: 'allow',
                downloadPath: dir
            });
        }
        console.log(`Loading ${url}`)
        await page.goto(url);
        console.log(`Loaded ${url}`)

        await page.setViewport({ width, height });
        console.log("setViewport")
        await page.waitForSelector(selector);
        console.log("selector")
        if (!download) {
            await page.screenshot({path});
        } else {
            console.log("sleep 1s")
            await new Promise(r => setTimeout(r, 1000))
            console.log("sleep done")
        }
    }

    await browser.close();
})();
