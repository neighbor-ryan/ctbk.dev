import { Html, Head, Main, NextScript } from 'next/document'
import {basePath} from "../src/utils/config";

export default function Document() {
    return (
        <Html>
            <Head>
                <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/4.7.0/css/font-awesome.min.css" />
                <link rel="icon" href={`${basePath}/favicon.ico`} />
            </Head>
            <body>
            <Main />
            <NextScript />
            </body>
        </Html>
    )
}
