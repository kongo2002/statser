import UIkit from 'uikit';
import Icons from 'uikit/dist/js/uikit-icons';

// loads the Icon plugin
UIkit.use(Icons);

var m = require('mithril')

var Api = {
  _stats: [],
  _health: [],
  _timestamp: 0,

  metrics: function() {
    return m.request({
      method: 'GET',
      url: '/.statser/metrics'
    }).then(function(res) {
      Api._stats = res.stats;
      Api._health = res.health;
      Api._timestamp = res.timestamp;

      return res;
    });
  },
};

var mkNav = function(name, link) {
  var route = m.route.get();
  var cls = route == link ? '.uk-active' : '';
  return m('li' + cls, m('a', {href: link, oncreate: m.route.link}, name));
}

var navigation = function() {
  return m('nav.uk-navbar-container', {'uk-navbar': ''}, [
    m('.uk-navbar-left', [
      m('ul.uk-navbar-nav', [
        mkNav('statser', '/'),
        mkNav('dashboard', '/'),
        mkNav('control', '/control/')
      ])
    ]),
    m('.uk-navbar-right', [
      m('ul.uk-navbar-nav', [
        mkNav('login', '/login/')
      ])
    ])
  ])
};

var Live = {
  LIVE_ID: '#live',
  MAX_TICKS: 20,
  GRAPH_NAME: 'committed-points',

  _update: function(vnode) {
    var state = vnode.state;
    var x = state._x;
    var y = state._y;
    var valueline = state._valueline;

    // just pick the latest metric matching the `GRAPH_NAME`
    var ts = Api._timestamp;
    var liveMetric = Api._stats.find(function(s) { return s.name == Live.GRAPH_NAME; });

    if (!liveMetric || ts <= state.__ts) {
      Live.scheduleUpdate(vnode);
      return;
    }

    state.__ts = ts;
    state.__data.push({x: new Date(ts * 1000), y: liveMetric.value});
    state.__data = state.__data.slice(1);

    // scale the range of the data
    x.domain(d3.extent(state.__data, function(d) { return d.x; }));
    y.domain(d3.extent(state.__data, function(d) { return d.y; }));

    var svg = d3.select(Live.LIVE_ID).transition();
    var duration = 750;

    svg.select(".line")
      .duration(duration)
      .attr("d", valueline(state.__data));

    svg.select(".xaxis")
      .duration(duration)
      .call(d3.axisBottom(x));

    svg.select(".yaxis")
      .duration(duration)
      .call(d3.axisLeft(y).ticks(6));

      Live.scheduleUpdate(vnode);
  },

  oncreate: function(vnode) {
    this.__data = [];
    this.__ts = 0;

    // initial data set
    var now = new Date();
    for (var i=Live.MAX_TICKS; i>0; i--) {
      this.__data.push({x: new Date(+now - i * 10000), y: 0});
    }

    var margin = {top: 50, right: 50, bottom: 50, left: 50},
      width = 560 - margin.left - margin.right,
      height = 240 - margin.top - margin.bottom;

    // set the ranges
    var x = d3.scaleTime().range([0, width]);
    var y = d3.scaleLinear().range([height, 0]);

    // define the line
    var valueline = d3.line()
      .curve(d3.curveCatmullRom)
      .x(function(d) { return x(d.x); })
      .y(function(d) { return y(d.y); });

    // keep track of ranges
    this._x = x;
    this._y = y;
    this._valueline = valueline;

    var svg = d3.select(Live.LIVE_ID).append("svg")
      .attr("width", width + margin.left + margin.right)
      .attr("height", height + margin.top + margin.bottom)
      .append("g")
      .attr("transform",
        "translate(" + margin.left + "," + margin.top + ")");

    // legend
    svg.append("text")
      .attr("x", 10)  // space legend
      .attr("y", 20)
      .attr("class", "legend")
      .text(Live.GRAPH_NAME);

    // Scale the range of the data
    x.domain(d3.extent(this.__data, function(d) { return d.x; }));
    y.domain(d3.extent(this.__data, function(d) { return d.y; }));

    // Add the paths with different curves.
    svg.append("path")
      .datum(this.__data)
      .attr("class", "line")
      .attr("d", valueline(this.__data));

    // x axis
    svg.append("g")
      .attr("class", "xaxis")
      .attr("transform", "translate(0," + height + ")")
      .call(d3.axisBottom(x));

    // y axis
    svg.append("g")
      .attr("class", "yaxis")
      .call(d3.axisLeft(y).ticks(6));

    Live.scheduleUpdate(vnode);
  },

  scheduleUpdate: function(vnode) {
    vnode.state.__timer = window.setTimeout(function() {
      Live._update(vnode);
    }, 1000);
  },

  onremove: function(vnode) {
    if (this.__timer) {
      window.clearTimeout(this.__timer);
      this.__timer = null;
    }
  },

  view: function() {
    // D3 is responsible to fill the SVG element
    return m(`div${Live.LIVE_ID}.graph`);
  }
}

var Dashboard = {
  UPDATE_INTERVAL: 10 * 1000,

  _lead: function() {
    return m('p.uk-text-lead',
      'Find the most important operation metrics, statistics and health checkpoints on statser below.');
  },

  _meta: function() {
    return m('p.uk-text-meta', [
      'This page\'s content is updated every 10 seconds via ',
      m('abbr', {title: 'server sent events'}, 'SSE'),
      '. If this doesn\'t work for you, please try the ',
      m('a', {href: '/.statser/health'}, 'JSON endpoint'), ' instead.'
    ]);
  },

  _health: function(vnode) {
    return [
      m('h2', 'Health'),
      m('p', 'Health checks on statser\'s main operational components.'),
      m('div#health',
        m('dl.uk-description-list', vnode.state.__healths.map(function(health) {
          var success = health.good;
          var label = success ? '.uk-label-success' : '.uk-label-danger';
          var cls = '.uk-label' + label;
          var icon = success ? 'check' : 'close';
          var ts = (new Date(health.timestamp * 1000)).toUTCString();

          return [
            m('dt', health.name),
            m('dd', [
              m('span' + cls, {'uk-icon': 'icon: ' + icon}),
              m('small', ' last seen at ' + ts)
            ])
          ];
        }))
      )
    ];
  },

  _live: function() {
    return [
      m('h2', 'Live'),
      m(Live)
    ];
  },

  _metrics: function(vnode) {
    return [
      m('h2', 'Metrics'),
      m('p', 'Statistics on some of the most important performance characteristics of this statser instance.'),
      m('table.uk-table.uk-table-divider.uk-table-responsive', [
        m('thead', m('tr', [
          m('th', 'Statistic'),
          m('th', 'Metric')
        ])),
        m('tbody', vnode.state.__metrics.map(function(metric) {
          var type = metric.type == 'counter' ? '/sec' : 'average';
          return m('tr', [
            m('td', metric.name),
            m('td', metric.value.toFixed(2), m('small', ` ${type}`))
          ]);
        }))
      ])
    ];
  },

  _fetch: function(vnode) {
    Api.metrics()
      .then(function(res) {
        vnode.state.__metrics = res.stats || [];
        vnode.state.__healths = res.health || [];

      });

    // trigger next fetch - basically polling
    vnode.state.__timer = window.setTimeout(function() {
      Dashboard._fetch(vnode);
    }, Dashboard.UPDATE_INTERVAL);
  },

  oninit: function(vnode) {
    this.__metrics = [];
    this.__healths = [];
  },

  oncreate: function(vnode) {
    Dashboard._fetch(vnode);
  },

  onremove: function(vnode) {
    if (this.__timer) {
      window.clearTimeout(this.__timer);
      this.__timer = null;
    }
  },

  view: function(vnode) {
    return [
      navigation(),
      m('.uk-container', [
        this._lead(),
        this._meta(),
        this._health(vnode),
        this._live(),
        this._metrics(vnode)
      ])
    ];
  }
};

var Control = {
  view: function() {
    return navigation();
  }
};

var Login = {
  view: function() {
    return navigation();
  }
};

m.route(document.body, '/', {
  '/': Dashboard,
  '/control': Control,
  '/login': Login
});

// vim: set et sw=2 sts=2:
