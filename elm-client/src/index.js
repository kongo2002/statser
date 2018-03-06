'use strict';

import UIkit from 'uikit';
import Icons from 'uikit/dist/js/uikit-icons';

// load uikit icons
UIkit.use(Icons);

require('./index.html');

// load elm application
var Elm = require('./Main.elm');
var app = Elm.Main.fullscreen();

// SVG's dimensions
var width = 560 - 2 * 50;
var height = 240 - 2 * 50;
var duration = 750;

// D3 scales
var x = d3.scaleTime().range([0, width]);
var y = d3.scaleLinear().range([height, 0]);

// D3 line definition
var valueline = d3.line()
  .curve(d3.curveCatmullRom)
  .x(function(d) { return x(new Date(d[0] * 1000)); })
  .y(function(d) { return y(d[1]); });

// live update subscription
app.ports.liveUpdate.subscribe(function(data) {
  var metric = data[0];
  var stats = data[1];

  var svg = d3.select('#live').transition();

  x.domain(d3.extent(stats, function(d) { return new Date(d[0] * 1000); }));
  y.domain(d3.extent(stats, function(d) { return d[1]; }));

  svg.select('.line')
    .duration(duration)
    .attr('d', valueline(stats));

  svg.select('.legend')
    .text(metric);

  svg.select('.xaxis')
    .duration(duration)
    .call(d3.axisBottom(x));

  svg.select('.yaxis')
    .duration(duration)
    .call(d3.axisLeft(y).ticks(6));
});

// subscription for uikit's notification
app.ports.notification.subscribe(function(data) {
  var message = data[0];
  var status = data[1];

  UIkit.notification({
    message: message,
    status: status,
    pos: 'top-right',
    timeout: 5000
  });
});

// vim: et sw=2 sts=2
