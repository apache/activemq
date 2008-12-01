/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// AMQ Ajax handler
// This class provides the main API for using the Ajax features of AMQ. It
// allows JMS messages to be sent and received from javascript when used
// with the org.apache.activemq.web.MessageListenerServlet
//
var amq =
{
  // The URI of the MessageListenerServlet
  uri: '/amq',

  // Polling. Set to true (default) if waiting poll for messages is needed
  poll: true,
  
  // Poll delay. if set to positive integer, this is the time to wait in ms before
  // sending the next poll after the last completes.
  _pollDelay: 0,

  _first: true,
  _pollEvent: function(first) {},
  _handlers: new Array(),

  _messages:0,
  _messageQueue: '',
  _queueMessages: 0,

  _messageHandler: function(request)
  {
    try
    {
      if (request.status == 200)
      {
        var response = request.responseXML.getElementsByTagName("ajax-response");
        if (response != null && response.length == 1)
        {
          for ( var i = 0 ; i < response[0].childNodes.length ; i++ )
          {
            var responseElement = response[0].childNodes[i];

            // only process nodes of type element.....
            if ( responseElement.nodeType != 1 )
              continue;

            var id   = responseElement.getAttribute('id');


            var handler = amq._handlers[id];
            if (handler!=null)
            {
              for (var j = 0; j < responseElement.childNodes.length; j++)
              {
                handler(responseElement.childNodes[j]);
	      }
            }
          }
        }
      }
    }
    catch(e)
    {
      alert(e);
    }
  },

  startBatch: function()
  {
    amq._queueMessages++;
  },

  endBatch: function()
  {
    amq._queueMessages--;
    if (amq._queueMessages==0 && amq._messages>0)
    {
      var body = amq._messageQueue;
      amq._messageQueue='';
      amq._messages=0;
      amq._queueMessages++;
      new Ajax.Request(amq.uri, { method: 'post', postBody: body, onSuccess: amq.endBatch});
    }
  },

  _pollHandler: function(request)
  {
    amq.startBatch();
    try
    {
      amq._messageHandler(request);
      amq._pollEvent(amq._first);
      amq._first=false;
    }
    catch(e)
    {
        alert(e);
    }
    amq.endBatch();

    if (amq._pollDelay>0)
      setTimeout('amq._sendPoll()',amq._pollDelay);
    else
      amq._sendPoll();
  },
  
  _sendPoll: function(request)
  {
    new Ajax.Request(amq.uri, { method: 'get', onSuccess: amq._pollHandler });
  },

  // Add a function that gets called on every poll response, after all received
  // messages have been handled.  The poll handler is past a boolean that indicates
  // if this is the first poll for the page.
  addPollHandler : function(func)
  {
    var old = amq._pollEvent;
    amq._pollEvent = function(first)
    {
      old(first);
      func(first);
    }
  },

  // Send a JMS message to a destination (eg topic://MY.TOPIC).  Message should be xml or encoded
  // xml content.
  sendMessage : function(destination,message)
  {
    amq._sendMessage(destination,message,'send');
  },

  // Listen on a channel or topic.   handler must be a function taking a message arguement
  addListener : function(id,destination,handler)
  {
    amq._handlers[id]=handler;
    amq._sendMessage(destination,id,'listen');
  },

  // remove Listener from channel or topic.
  removeListener : function(id,destination)
  {
    amq._handlers[id]=null;
    amq._sendMessage(destination,id,'unlisten');
  },

  _sendMessage : function(destination,message,type)
  {
    if (amq._queueMessages>0)
    {
      if (amq._messages==0)
      {
        amq._messageQueue='destination='+destination+'&message='+message+'&type='+type;
      }
      else
      {
        amq._messageQueue+='&d'+amq._messages+'='+destination+'&m'+amq._messages+'='+message+'&t'+amq._messages+'='+type;
      }
      amq._messages++;
    }
    else
    {
      amq.startBatch();
      new Ajax.Request(amq.uri, { method: 'post', postBody: 'destination='+destination+'&message='+message+'&type='+type, onSuccess: amq.endBatch});
    }
  },
  
  _startPolling : function()
  {
   if (amq.poll)
      new Ajax.Request(amq.uri, { method: 'get', parameters: 'timeout=10', onSuccess: amq._pollHandler });
  }
};

Behaviour.addLoadEvent(amq._startPolling);

function getKeyCode(ev)
{
    var keyc;
    if (window.event)
        keyc=window.event.keyCode;
    else
        keyc=ev.keyCode;
    return keyc;
}
