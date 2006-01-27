// -----------------
// Original code by Joe Walnes
// -----------------


var connection = null;
var chatTopic = "CHAT.DEMO";
var chatMembership = "CHAT.DEMO";



// returns the text of an XML element
function elementText(element) {
    var answer = ""
    var node = element.firstChild
    while (node != null) {
        var tmp = node.nodeValue
        if (tmp != null) {
            answer += tmp
        }
        node = node.nextSibling
    }
    return answer
}


var room = 
{
  last: "",
  username: "unknown",
  
  join: function()
  {
    var name = $('username').value;
    if (name == null || name.length==0 )
    {
      alert("Please enter a username!");
    }
    else
    {
       username=name;
       
       amq.addTopicListener('chat',chatTopic,room);
       // amq.sendMessage(chatSubscription, "<subscribe>" + username + "</subscribe>");
      
       // switch the input form
       $('join').className='hidden';
       $('joined').className='';
       $('phrase').focus();
       Behaviour.apply();
       amq.sendMessage(chatMembership, "<message type='join' from='" + username + "'/>");
    }
  },
  
  leave: function()
  {
       amq.sendMessage(chatMembership, "<message type='leave' from='" + username + "'/>");
       // switch the input form
       $('join').className='';
       $('joined').className='hidden';
       $('username').focus();
       username=null;
      Behaviour.apply();
  },
  
  chat: function()
  {
    var text = $('phrase').value;
    if (text != null && text.length>0 )
    {
        // TODO more encoding?
        text=text.replace('<','&lt;');
        text=text.replace('>','&gt;');
        amq.sendMessage(chatTopic, "<message type='chat' from='" + username + "'>" + text + "</message>");
	$('phrase').value="";
    }
  },
  
  amqMessage: function(message) 
  {
     var chat=$('chat');
     var type=message.attributes['type'].value;
     var from=message.attributes['from'].value;
         
     switch(type)
     {
       case 'chat' :
       {
          var text=message.childNodes[0].data;
          var alert='false';
     
          if ( from == this.last )
            from="...";
          else
          {
            this.last=from;
            from+=":";
          }
     
          chat.innerHTML += "<span class=\"from\">"+from+"&nbsp;</span><span class=\"text\">"+text+"</span><br/>";
          break;
       }
       
       case 'ping' :
       {
          var li = document.createElement('li');
          li.innerHtml=from;
          $('members').innerHTML+="<span class=\"member\">"+from+"</span><br/>";
	  break;
       }
       
       case 'join' :
       {
          $('members').innerHTML="";
          if (username!=null)
	    amq.sendMessage(chatMembership, "<message type='ping' from='" + username + "'/>");
          chat.innerHTML += "<span class=\"alert\"><span class=\"from\">"+from+"&nbsp;</span><span class=\"text\">has joined the room!</span></span><br/>";
	  break;
       }
       
       case 'leave':
       {
          $('members').innerHTML="";
          if (username!=null)
            amq.sendMessage(chatMembership, "<message type='ping' from='" + username + "'/>");
          chat.innerHTML += "<span class=\"alert\"><span class=\"from\">"+from+"&nbsp;</span><span class=\"text\">has left the room!</span></span><br/>";
	  break;
       }
     }
     
     chat.scrollTop = chat.scrollHeight - chat.clientHeight;
     
  }
};


function chatPoll(first)
{
   if (first ||  $('join').className=='hidden' && $('joined').className=='hidden')
   {
       $('join').className='';
       $('joined').className='hidden';
       $('username').focus();
      Behaviour.apply();
   }
}

function chatInit()
{
  amq.addPollHandler(chatPoll);
}


var behaviours = 
{ 
  '#username' : function(element)
  {
    element.setAttribute("autocomplete","OFF"); 
    element.onkeypress = function(event)
    {
        if (event && (event.keyCode==13 || event.keyCode==10))
        {
      	  room.join();
	}
    } 
  },
  
  '#joinB' : function(element)
  {
    element.onclick = function()
    {
      room.join();
    }
  },
  
  '#phrase' : function(element)
  {
    element.setAttribute("autocomplete","OFF");
    element.onkeypress = function(event)
    {
        if (event && (event.keyCode==13 || event.keyCode==10))
        {
          room.chat();
	  return false;
	}
	return true;
    }
  },
  
  '#sendB' : function(element)
  {
    element.onclick = function()
    {
    	room.chat();
    }
  },
  
  
  '#leaveB' : function(element)
  {
    element.onclick = function()
    {
      room.leave();
    }
  }
};

Behaviour.register(behaviours);
Behaviour.addLoadEvent(chatInit);  














