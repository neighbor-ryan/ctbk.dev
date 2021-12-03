import {HashRouter, Location, Route, Routes, useLocation, useNavigate} from "react-router-dom";
import React, {ReactElement, ReactNode, useState,} from 'react';
import ReactDOM from 'react-dom'

import $ from 'jquery';
import {App} from "./plot";
import {Worker} from "./worker";
import { QueryParamProvider } from "use-query-params";

/**
 * This is the main thing you need to use to adapt the react-router v6
 * API to what use-query-params expects.
 *
 * Pass this as the `ReactRouterRoute` prop to QueryParamProvider.
 */
const RouteAdapter: React.FC = ({ children }) => {
    const navigate = useNavigate();
    const location = useLocation();

    const adaptedHistory = React.useMemo(
        () => ({
            replace(location: Location) {
                navigate(location, { replace: true, state: location.state });
            },
            push(location: Location) {
                navigate(location, { replace: false, state: location.state });
            },
        }),
        [navigate]
    );
    // https://github.com/pbeshai/use-query-params/issues/196
    // eslint-disable-next-line @typescript-eslint/ban-ts-comment
    // @ts-ignore
    // eslint-disable-next-line @typescript-eslint/no-unsafe-return
    return children({ history: adaptedHistory, location });
};

function Router() {
    const [ url, setUrl ] = useState("/assets/ymrgtb_cd_201306:202111.sqlite");
    const [ worker, setWorker ] = useState<Worker>(new Worker({ url, }))

    return (
        <HashRouter>
            <QueryParamProvider ReactRouterRoute={RouteAdapter}>
            {/*<QueryParamProvider ReactRouterRoute={Route}>*/}
                <Routes>
                    <Route path="" element={<App url={url} worker={worker} />} />
                    {/*<Route path="disk-tree" element={<DiskTree url={url} worker={worker} />} />*/}
                </Routes>
            </QueryParamProvider>
        </HashRouter>
    )
}

$(document).ready(function () {
    ReactDOM.render(<Router/>, document.getElementById('root'));
});
