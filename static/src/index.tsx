import {HashRouter, Route, Routes} from "react-router-dom";
import React, {useState,} from 'react';
import ReactDOM from 'react-dom'

import $ from 'jquery';
import {App} from "./plot";
import {Worker} from "./worker";

function Router() {
    const [ url, setUrl ] = useState("/assets/ymrgtb_cd_201306:202111.sqlite");
    const [ worker, setWorker ] = useState<Worker>(new Worker({ url, }))

    return (
        <HashRouter>
            <Routes>
                <Route path="" element={<App url={url} worker={worker} />} />
                {/*<Route path="disk-tree" element={<DiskTree url={url} worker={worker} />} />*/}
            </Routes>
        </HashRouter>
    )
}

$(document).ready(function () {
    ReactDOM.render(<Router/>, document.getElementById('root'));
});
