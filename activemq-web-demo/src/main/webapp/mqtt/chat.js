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
    
    var host = $("#connect_host").val();    
    var port = $("#connect_port").val();
    var clientId = $("#connect_clientId").val();
    destination = $("#destination").val();

    
    client = new Messaging.Client(host, Number(port), clientId);

    client.onConnect = onConnect;
  
    client.onMessageArrived = onMessageArrived;
    client.onConnectionLost = onConnectionLost;            

    client.connect({onSuccess:onConnect, onFailure:onFailure}); 
    return false;
  });  

    // the client is notified when it is connected to the server.
    var onConnect = function(frame) {
      debug("connected to MQTT");
      $('#connect').fadeOut({ duration: 'fast' });
      $('#disconnect').fadeIn();
      $('#send_form_input').removeAttr('disabled');
      client.subscribe(destination);
    };  

    // this allows to display debug logs directly on the web page
    var debug = function(str) {
      $("#debug").append(document.createTextNode(str + "\n"));
    };  

  $('#disconnect_form').submit(function() {
    client.disconnect();
    $('#disconnect').fadeOut({ duration: 'fast' });
    $('#connect').fadeIn();
    $('#send_form_input').addAttr('disabled');
    return false;
  });

  $('#send_form').submit(function() {
    var text = $('#send_form_input').val();
    if (text) {
      message = new Messaging.Message(text);
      message.destinationName = destination;
      client.send(message);
      $('#send_form_input').val("");
    }
    return false;
  });

  function onFailure(failure) {
    debug("failure");
    debug(failure.errorMessage);
  }  

  function onMessageArrived(message) {
    var p = document.createElement("p");
    var t = document.createTextNode(message.payloadString);
    p.appendChild(t);
    $("#messages").append(p);
  }

  function onConnectionLost(responseObject) {
    if (responseObject.errorCode !== 0) {
      debug(client.clientId + ": " + responseObject.errorCode + "\n");
    }
  }

});