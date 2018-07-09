
# web client

Statser's web UI that is usually reachable at `http://localhost:8080/.statser/`
is written in [Elm][elm].


## Building

The project is a usual npm package built via [webpack][webpack] and can be
produced with [yarn][yarn] (my personal favorite) or vanilla [npm][npm].


### make

Just invoke `make` in the current (`elm-client`) directory to install all
required JavaScript dependencies and build the project. The `Makefile` uses
either `yarn` or `npm` depending on their existence in `$PATH`:

```
$ make
```


### using yarn

```
$ yarn
$ yarn build
```


### using npm

```
$ npm install
$ npm run build
```


[elm]: http://elm-lang.org/
[webpack]: https://webpack.js.org
[yarn]: https://yarnpkg.com/
[npm]: https://www.npmjs.com/
