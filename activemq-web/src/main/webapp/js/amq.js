

// AMQ  handler
var amq = 
{
  first: true,
  pollEvent: function(first) {},
  
  addPollHandler : function(func)
  {
    var old = this.pollEvent;
    this.pollEvent = function(first) 
    {
      old(first);
      func(first);
    }
  },
  
  sendMessage : function(destination,message)
  {
      ajaxEngine.sendRequestWithData('amqSend', message, 'destination='+destination); 
  },
  
  // Listen on a topic.   handler must have a amqMessage method taking a message arguement
  addTopicListener : function(id,topic,handler)
  {   
      var ajax2amq = { ajaxUpdate: function(msg) { handler.amqMessage(amqFirstElement(msg)) } };
      ajaxEngine.registerAjaxObject(id, ajax2amq);
      ajaxEngine.sendRequest('amqListen', 'destination='+topic+'&topic=true&id='+id+'&listen=true'); 
  },
  
  // Listen on a channel.   handler must have a amqMessage method taking a message arguement
  addChannelListener: function(id,channel,handler)
  {   
      var ajax2amq = { ajaxUpdate: function(msg) { handler.amqMessage(amqFirstElement(msg)) } };
      ajaxEngine.registerAjaxObject(id, ajax2amq);
      ajaxEngine.sendRequest('amqListen', 'destination='+channel+'&topic=false&id='+id+'&listen=true'); 
  }
  
};

function initAMQ()
{
  // Register URLs.  All the same at the moment and params are used to distinguish types
  ajaxEngine.registerRequest('amqListen','/amq');
  ajaxEngine.registerRequest('amqPoll', '/amq');
  ajaxEngine.registerRequest('amqSend', '/amq');
  
  var pollHandler = {
       ajaxUpdate: function(ajaxResponse) 
       {
           // Poll again for events
           amq.pollEvent(amq.first);
           amq.first=false;
           ajaxEngine.sendRequest('amqPoll');
       }
  };
  ajaxEngine.registerAjaxObject('amqPoll', pollHandler);
  
  var sendHandler = {
       ajaxUpdate: function(ajaxResponse) 
       {
       }
  };
  ajaxEngine.registerAjaxObject('amqSend', sendHandler);
  
  ajaxEngine.sendRequest('amqPoll',"timeout=0"); // first poll to establish polling session ID
}
Behaviour.addLoadEvent(initAMQ);  


function amqFirstElement(t) {
    for (i = 0; i < t.childNodes.length; i++) {
        var child = t.childNodes[i]
        if (child.nodeType == 1) {
            return child
        }
    }
    return null
}
