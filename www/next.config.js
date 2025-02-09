import WithMdx from "@next/mdx"

const { NEXT_MINIFY } = process.env
const minimize = !['false', 'n', 'no', '0'].includes(NEXT_MINIFY)

/** @type {import('next').NextConfig} */
const nextConfig = {
  swcMinify: true,
  transpilePackages: [
    '@rdub/base',
    '@rdub/icons',
  ],
  images: {
    unoptimized: true,
  },
  webpack({ optimization, ...config }) {
    return { ...config, optimization: { ...optimization, minimize } }
  },
  output: "export",
}

const withMDX = WithMdx({
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

export default withMDX({
  ...nextConfig,
  // Append the default value with md extensions
  pageExtensions: ['ts', 'tsx', 'js', 'jsx', 'md', 'mdx'],
})
