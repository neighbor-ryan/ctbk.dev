import {HashRouter, Route, Routes} from "react-router-dom";
import React, {useState,} from 'react';
import ReactDOM from 'react-dom'

import $ from 'jquery';
import {App} from "./plot";
import {S3Tree} from "./s3tree";
import {QueryParamProvider} from "use-query-params";
import {RouteAdapter} from "./route-adapter";

const DEFAULT_AGGREGATED_URL = 'https://ctbk.s3.amazonaws.com/aggregated/ymrgtb_cd.sqlite'

function Router() {
    const [ url, setUrl ] = useState(DEFAULT_AGGREGATED_URL);

    return (
        <HashRouter>
            <QueryParamProvider ReactRouterRoute={RouteAdapter}>
                <Routes>
                    <Route path="" element={<App url={url} />} />
                    <Route path="s3/:bucket" element={<S3Tree />} />
                </Routes>
            </QueryParamProvider>
        </HashRouter>
    )
}

$(document).ready(function () {
    ReactDOM.render(<Router/>, document.getElementById('root'));
});
