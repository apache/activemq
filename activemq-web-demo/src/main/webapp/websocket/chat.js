/**
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the 'License'); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an 'AS IS' BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
      $("#debug").append(document.createTextNode(str + "\n"));
    };
    // the client is notified when it is connected to the server.
    var onconnect = function(frame) {
      client.debug("connected to Stomp");
      $('#connect').fadeOut({ duration: 'fast' });
      $('#disconnect').fadeIn();
      $('#send_form_input').removeAttr('disabled');

      client.subscribe(destination, function(message) {
        var p = document.createElement("p");
        var t = document.createTextNode(message.body);
        p.appendChild(t);
        $("#messages").append(p);
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