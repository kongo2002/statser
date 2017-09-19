$(document).ready(function() {
  var healthRow = function(elem) {
    var good = elem.good === true;
    var icon = good ? 'oi-circle-check text-success' : 'oi-circle-x text-danger';
    var title = good ? 'good' : 'bad';
    var name = '<dt class="col-sm-4">'+elem.name+'</dt>';
    var descr = '<dd class="col-sm-8"><span class="oi '+icon+'" title="'+title+'" aria-hidden="true"></span></dd>';

    return name + descr;
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
      var row = (elem.type == 'counter')
        ? '<tr><th scope="row">'+elem.name+'</td><td>'+elem.value.toFixed(2)+'</td><td>-</td></tr>'
        : '<tr><th scope="row">'+elem.name+'</td><td>-</td><td>'+elem.value.toFixed(2)+'</td></tr>';

      $('#stats tbody:last').append(row);
    }
  };
});
