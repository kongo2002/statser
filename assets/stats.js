$(document).ready(function() {
  var e = new EventSource('/.statser/stream');
  e.addEventListener('open', function(event) {});

  e.onmessage = function(event) {
    var data = $.parseJSON(event.data);

    $('#stats tbody').html('');
    for (var idx in data.stats) {
      var row = data.stats[idx];
      $('#stats tbody:last').append('<tr><td>' + row.type + '</td><td>' + row.value + '</td></tr>');
    }
  };
});
