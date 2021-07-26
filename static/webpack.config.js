const webpack = require('webpack');

module.exports = {
  entry: "./src/index.tsx",
  module: {
    rules: [
      {
        test: /\.tsx?$/,
        use: "ts-loader",
        exclude: /node_modules/,
      },
    ],
  },
  resolve: {
    extensions: [".tsx", ".ts", ".js"],
  },
  output: {
    filename: "bundle.js",
  },
  devServer: {
    publicPath: "/dist",
  },
  plugins: [
    new webpack.DefinePlugin({
      'process.env.NODE_DEBUG': JSON.stringify(process.env.NODE_DEBUG),
    })
  ]
};
