const {
    createVanillaExtractPlugin
} = require('@vanilla-extract/next-plugin');
const withVanillaExtract = createVanillaExtractPlugin();

const createTranspileModulesPlugin = require("next-transpile-modules");
const withTranspileModules = createTranspileModulesPlugin(["next-utils"]);

/** @type {import('next').NextConfig} */
const nextConfig = {
    reactStrictMode: true,
    webpack: function (config, options) {
        config.experiments = { asyncWebAssembly: true };
        config.module.rules.push({
            test: /\.wasm$/,
            type: 'webassembly/sync',
        });
        return config;
    },
    swcMinify: true,
    images: {
        unoptimized: true,
    },
    // webpack(webpackConfig) {
    //     return { ...webpackConfig, optimization: { minimize: false } };
    // },
}

const withMDX = require('@next/mdx')({
    extension: /\.mdx?$/,
    options: {
        // If you use remark-gfm, you'll need to use next.config.mjs
        // as the package is ESM only
        // https://github.com/remarkjs/remark-gfm#install
        remarkPlugins: [],
        rehypePlugins: [],
        // If you use `MDXProvider`, uncomment the following line.
        providerImportSource: "@mdx-js/react",
    },
})
module.exports = withTranspileModules(withVanillaExtract(withMDX({
    ...nextConfig,
    // Append the default value with md extensions
    pageExtensions: ['ts', 'tsx', 'js', 'jsx', 'md', 'mdx'],
})))
