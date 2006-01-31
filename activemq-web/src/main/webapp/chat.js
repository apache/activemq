// -----------------
// Original code by Joe Walnes
// -----------------


var connection = null;
var chatTopic = "topic://CHAT.DEMO";
var chatMembership = "topic://CHAT.DEMO";



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
  _last: "",
  _username: null,
  
  join: function()
  {
    var name = $('username').value;
    if (name == null || name.length==0 )
    {
      alert('Please enter a username!');
    }
    else
    {
       this._username=name;
       
       $('join').className='hidden';
       Behaviour.apply();
       amq.addListener('chat',chatTopic,room._chat);
       $('join').className='hidden';
       $('joined').className='';
       $('phrase').focus();
       Behaviour.apply();
       amq.sendMessage(chatMembership, "<message type='join' from='" + room._username + "'/>");
    }
  },
  
  leave: function()
  {
       amq.removeListener('chat',chatTopic);
       // switch the input form
       $('join').className='';
       $('joined').className='hidden';
       $('username').focus();
       room._username=null;
       Behaviour.apply();
       amq.sendMessage(chatMembership, "<message type='leave' from='" + room._username + "'/>");
  },
  
  chat: function()
  {
    var text = $F('phrase');
    $('phrase').value='';
    if (text != null && text.length>0 )
    {
        // TODO more encoding?
        text=text.replace('<','&lt;');
        text=text.replace('>','&gt;');
        amq.sendMessage(chatTopic, "<message type='chat' from='" + room._username + "'>" + text + "</message>");
    }
  },
  
  _chat: function(message) 
  {
     var chat=$('chat');
     var type=message.getAttribute('type');
     var from=message.getAttribute('from');
         
     switch(type)
     {
       case 'chat' :
       {
          var text=message.childNodes[0].data;
     
          if ( from == room._last )
            from="...";
          else
          {
            room._last=from;
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
          if (room._username!=null) 
	    amq.sendMessage(chatMembership, "<message type='ping' from='" + room._username + "'/>");
          chat.innerHTML += "<span class=\"alert\"><span class=\"from\">"+from+"&nbsp;</span><span class=\"text\">has joined the room!</span></span><br/>";
	  break;
       }
       
       case 'leave':
       {
          $('members').innerHTML="";
          if (room._username!=null)
            amq.sendMessage(chatMembership, "<message type='ping' from='" + room._username + "'/>");
          chat.innerHTML += "<span class=\"alert\"><span class=\"from\">"+from+"&nbsp;</span><span class=\"text\">has left the room!</span></span><br/>";
	  break;
       }
     }
     
     chat.scrollTop = chat.scrollHeight - chat.clientHeight;
     
  },
  
  _poll: function(first)
  {
     if (first ||  $('join').className=='hidden' && $('joined').className=='hidden')
     {
       $('join').className='';
       $('joined').className='hidden';
       $('username').focus();
      Behaviour.apply();
     }
  }
};

amq.addPollHandler(room._poll);

var chatBehaviours = 
{ 
  '#username' : function(element)
  {
    element.setAttribute("autocomplete","OFF"); 
    element.onkeyup = function(ev)
    {  
        var keyc;
        if (window.event)
           keyc=window.event.keyCode;
        else
           keyc=ev.keyCode;
        if (keyc==13 || keyc==10)
        {
      	  room.join();
	  return false;
	}
	return true;
    } 
  },
  
  '#joinB' : function(element)
  {
    element.onclick = function(event)
    {
      room.join();
      return true;
    }
  },
  
  '#phrase' : function(element)
  {
    element.setAttribute("autocomplete","OFF");
    element.onkeyup = function(ev)
    {   
        var keyc;
        if (window.event)
           keyc=window.event.keyCode;
        else
           keyc=ev.keyCode;
           
        if (keyc==13 || keyc==10)
        {
          room.chat();
	  return false;
	}
	return true;
    }
  },
  
  '#sendB' : function(element)
  {
    element.onclick = function(event)
    {
      room.chat();
    }
  },
  
  
  '#leaveB' : function(element)
  {
    element.onclick = function()
    {
      room.leave();
      return false;
    }
  }
};

Behaviour.register(chatBehaviours); 














