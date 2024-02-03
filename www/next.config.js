const {
    createVanillaExtractPlugin
} = require('@vanilla-extract/next-plugin');
const withVanillaExtract = createVanillaExtractPlugin();

const { NEXT_MINIFY, NEXT_BASE_PATH } = process.env
const minimize = !['false', 'n', 'no', '0'].includes(NEXT_MINIFY)

/** @type {import('next').NextConfig} */
const nextConfig = {
    swcMinify: true,
    images: {
        unoptimized: true,
    },
    basePath: NEXT_BASE_PATH,
    webpack({ optimization, ...webpackConfig }) {
        return { ...webpackConfig, optimization: { ...optimization, minimize } };
    },
    output: "export",
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

module.exports =
    withVanillaExtract(withMDX({
        ...nextConfig,
        // Append the default value with md extensions
        pageExtensions: ['ts', 'tsx', 'js', 'jsx', 'md', 'mdx'],
    }))
