var path = require('path');

module.exports = {
  entry: {
    app: [
      './src/index.js'
    ]
  },

  output: {
    path: path.resolve(__dirname + '/dist'),
    filename: '[name].js',
  },

  module: {
    rules: [
      {
        test: /\.(css|scss)$/,
        use: {
          loader:  'file-loader?name=[name].[ext]',
          options: {}
        }
      },
      {
        test:    /\.html$/,
        exclude: /node_modules/,
        use: {
          loader:  'file-loader?name=[name].[ext]',
          options: {}
        }
      },
      {
        test:    /\.elm$/,
        exclude: [/elm-stuff/, /node_modules/],
        use: {
          loader:  'elm-webpack-loader?verbose=true&warn=true',
          options: {}
        }
      },
    ],

    noParse: /\.elm$/,
  },

  devServer: {
    inline: true,
    stats: { colors: true },
  },
};

// vim: et sw=2 sts=2
