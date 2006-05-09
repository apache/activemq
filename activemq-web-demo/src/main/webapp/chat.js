


var room = 
{
  _last: "",
  _username: null,
  _chatTopic: "topic://CHAT.DEMO",
  
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
       
       amq.addListener('chat',this._chatTopic,room._chat);
       $('join').className='hidden';
       $('joined').className='';
       $('phrase').focus();
       Behaviour.apply();
       amq.sendMessage(this._chatTopic, "<message type='join' from='" + room._username + "'/>");
    }
  },
  
  leave: function()
  {
       amq.removeListener('chat',this._chatTopic);
       // switch the input form
       $('join').className='';
       $('joined').className='hidden';
       $('username').focus();
       Behaviour.apply();
       amq.sendMessage(this._chatTopic, "<message type='leave' from='" + room._username + "'/>");
       room._username=null;
  },
  
  chat: function(text)
  {
    if (text != null && text.length>0 )
    {
        // TODO more encoding?
        text=text.replace('<','&lt;');
        text=text.replace('>','&gt;');
        amq.sendMessage(this._chatTopic, "<message type='chat' from='" + room._username + "'>" + text + "</message>");
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
          $('members').innerHTML+="<span class=\"member\">"+from+"</span><br/>";
	  break;
       }
       
       case 'join' :
       {
          $('members').innerHTML="";
          if (room._username!=null) 
	    amq.sendMessage(this._chatTopic, "<message type='ping' from='" + room._username + "'/>");
          chat.innerHTML += "<span class=\"alert\"><span class=\"from\">"+from+"&nbsp;</span><span class=\"text\">has joined the room!</span></span><br/>";
	  break;
       }
       
       case 'leave':
       {
          $('members').innerHTML="";
          if (room._username!=null)
            amq.sendMessage(this._chatTopic, "<message type='ping' from='" + room._username + "'/>");
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
        var keyc=getKeyCode(ev);
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
        var keyc=getKeyCode(ev);
           
        if (keyc==13 || keyc==10)
        {
          var text = $F('phrase');
          $('phrase').value='';
          room.chat(text);
	  return false;
	}
	return true;
    }
  },
  
  '#sendB' : function(element)
  {
    element.onclick = function(event)
    {
      var text = $F('phrase');
      $('phrase').value='';
      room.chat(text);
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














