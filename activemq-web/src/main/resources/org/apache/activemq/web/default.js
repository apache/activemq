
// Technique borrowed from scriptaculous to do includes.

var DefaultJS = {
  Version: 'AMQ default JS',
  script: function(libraryName) {
    document.write('<script type="text/javascript" src="'+libraryName+'"></script>');
  },
  load: function() {
    var scriptTags = document.getElementsByTagName("script");
    for(var i=0;i<scriptTags.length;i++) {
      if(scriptTags[i].src && scriptTags[i].src.match(/default\.js$/)) {
        var path = scriptTags[i].src.replace(/default\.js$/,'');
        this.script(path + 'prototype.js');
        this.script(path + 'behaviour.js');
        this.script(path + '_amq.js');
        // this.script(path + 'scriptaculous.js');
        break;
      }
    }
  }
}

DefaultJS.load();


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

  _first: true,
  _pollEvent: function(first) {},
  _handlers: new Array(),
  
  _messages:0,
  _messageQueue: '',
  _queueMessages: false,
  
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
                var child = responseElement.childNodes[j]
                if (child.nodeType == 1) 
                {
                  handler(child);
                }
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
  
  _pollHandler: function(request) 
  {
    amq._queueMessages=true;
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
    
    amq._queueMessages=false;
    
    if (amq._messages==0)
    {
      if (amq.poll)
        new Ajax.Request(amq.uri, { method: 'get', onSuccess: amq._pollHandler }); 
    }
    else
    {
      var body = amq._messageQueue+'&poll='+amq.poll;
      amq._messageQueue='';
      amq._messages=0;
      new Ajax.Request(amq.uri, { method: 'post', onSuccess: amq._pollHandler, postBody: body }); 
    }
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
    if (amq._queueMessages)
    {
      amq._messageQueue+=(amq._messages==0?'destination=':'&destination=')+destination+'&message='+message+'&type='+type;
      amq._messages++;
    }
    else
    {
      new Ajax.Request(amq.uri, { method: 'post', postBody: 'destination='+destination+'&message='+message+'&type='+type});
    }
  },
  
  _startPolling : function()
  {
    if (amq.poll)
      new Ajax.Request(amq.uri, { method: 'get', parameters: 'timeout=0', onSuccess: amq._pollHandler });
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
