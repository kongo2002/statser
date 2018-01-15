$(document).ready(function() {
  var healthRow = function(elem) {
    var good = elem.good === true;
    var ts = (elem.timestamp) ? (new Date(elem.timestamp * 1000)).toUTCString() : '-';
    var icon = good ? 'oi-circle-check text-success' : 'oi-circle-x text-danger';
    var title = good ? 'good' : 'bad';
    var name = '<dt class="col-sm-4">'+elem.name+'</dt>';
    var descr = '<dd class="col-sm-8"><span class="oi '+icon+'" title="'+title+'" aria-hidden="true"></span><span class="small"> last reported in: '+ts+'</span></dd>';

    return name + descr;
  };

  // initial data set
  var now = new Date();
  var maxTicks = 20;
  var graphName = 'committed-points';
  var metricsData = [];
  for (var i=maxTicks; i>0; i--) {
    metricsData.push({x: new Date(+now - i * 10000), y: 0});
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

  var svg = d3.select("#live").append("svg")
    .attr("width", width + margin.left + margin.right)
    .attr("height", height + margin.top + margin.bottom)
    .append("g")
    .attr("transform",
      "translate(" + margin.left + "," + margin.top + ")");

  // legend
  svg.append("text")
    .attr("x", width+5)  // space legend
    .attr("y", margin.top + 20)
    .attr("class", "legend");

  // Scale the range of the data
  x.domain(d3.extent(metricsData, function(d) { return d.x; }));
  y.domain(d3.extent(metricsData, function(d) { return d.y; }));

  // Add the paths with different curves.
  svg.append("path")
    .datum(metricsData)
    .attr("class", "line")
    .attr("d", valueline(metricsData));

  // x axis
  svg.append("g")
    .attr("class", "xaxis")
    .attr("transform", "translate(0," + height + ")")
    .call(d3.axisBottom(x));

  // y axis
  svg.append("g")
    .attr("class", "yaxis")
    .call(d3.axisLeft(y));

  var update = function(data) {
    // Scale the range of the data
    x.domain(d3.extent(data, function(d) { return d.x; }));
    y.domain(d3.extent(data, function(d) { return d.y; }));

    var svg = d3.select("#live").transition();
    var duration = 750;

    svg.select(".line")
      .duration(duration)
      .attr("d", valueline(data));

    svg.select(".xaxis")
      .duration(duration)
      .call(d3.axisBottom(x));

    svg.select(".yaxis")
      .duration(duration)
      .call(d3.axisLeft(y));
  };

  var e = new EventSource('/.statser/stream');
  e.addEventListener('open', function(event) {});

  e.onmessage = function(event) {
    var data = $.parseJSON(event.data);
    var interval = data.interval;

    $('#health').html('');

    for (var idx in data.health) {
      var elem = data.health[idx];
      $('#health').append(healthRow(elem));
    }

    $('#stats tbody').html('');

    for (var idx in data.stats) {
      var elem = data.stats[idx];
      var name = elem.name;
      var row = (elem.type == 'counter')
        ? '<tr><th scope="row">'+name+'</td><td>'+elem.value.toFixed(2)+'</td><td>-</td></tr>'
        : '<tr><th scope="row">'+name+'</td><td>-</td><td>'+elem.value.toFixed(2)+'</td></tr>';

      // update graph metrics if matching
      if (name === graphName) {
        metricsData.push({x: new Date(), y: elem.value});
        metricsData = metricsData.slice(1);
        update(metricsData);
      }

      $('#stats tbody:last').append(row);
    }
  };
});

/* vim: set et sw=2 sts=2: */
