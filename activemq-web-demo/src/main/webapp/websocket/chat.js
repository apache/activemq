$(document).ready(function(){

  var client, destination;

  $('#connect_form').submit(function() {
    var url = $("#connect_url").val();
    var login = $("#connect_login").val();
    var passcode = $("#connect_passcode").val();
    destination = $("#destination").val();

    client = Stomp.client(url);

    // this allows to display debug logs directly on the web page
    client.debug = function(str) {
      $("#debug").append(str + "\n");
    };
    // the client is notified when it is connected to the server.
    var onconnect = function(frame) {
      client.debug("connected to Stomp");
      $('#connect').fadeOut({ duration: 'fast' });
      $('#disconnect').fadeIn();
      $('#send_form_input').removeAttr('disabled');

      client.subscribe(destination, function(message) {
        $("#messages").append("<p>" + message.body + "</p>\n");
      });
    };
    client.connect(login, passcode, onconnect);

    return false;
  });

  $('#disconnect_form').submit(function() {
    client.disconnect(function() {
      $('#disconnect').fadeOut({ duration: 'fast' });
      $('#connect').fadeIn();
      $('#send_form_input').addAttr('disabled');
    });
    return false;
  });

  $('#send_form').submit(function() {
    var text = $('#send_form_input').val();
    if (text) {
      client.send(destination, {foo: 1}, text);
      $('#send_form_input').val("");
    }
    return false;
  });

});