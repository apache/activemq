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

var amq = org.activemq.Amq;

org.activemq.Chat = function() {
	var last = '';

	var user = null;

	var chatTopic = 'topic://CHAT.DEMO';

	var chat, join, joined, phrase, members, username = null;

	var chatHandler = function(message) {
		var type = message.getAttribute('type');
		var from = message.getAttribute('from');

		switch (type) {
			// Incoming chat message
			case 'chat' : {
				var text = message.childNodes[0].data;

				if (from == last) from = '...';
				else {
					last = from;
					from += ':';
				}

				chat.innerHTML += '<span class=\'from\'>' + from + '&nbsp;</span><span class=\'text\'>' + text + '</span><br/>';
				break;
			}

			// Incoming ping request, add the person's name to your list.
			case 'ping' : {
				members.innerHTML += '<span class="member">' + from + '</span><br/>';
				break;
			}

			// someone new joined the chatroom, clear your list and
			// broadcast your name to all users.
			case 'join' : {
				members.innerHTML = '';
				if (user != null)
					amq.sendMessage(chatTopic, '<message type="ping" from="' + user + '"/>');
				chat.innerHTML += '<span class="alert"><span class="from">' + from + '&nbsp;</span><span class="text">has joined the room!</span></span><br/>';
				break;
			}

			// Screw you guys, I'm going home...
			// When I (and everyone else) receive a leave message, we broadcast
			// our own names in a ping in order to update everyone's list.
			// todo: Make this more efficient by simply removing the person's name from the list.
			case 'leave': {
				members.innerHTML = '';
				chat.innerHTML += '<span class="alert"><span class="from">' + from + '&nbsp;</span><span class="text">has left the room!</span></span><br/>';

				// If we are the one that is leaving...
				if (from == user) {
				// switch the input form
					join.className = '';
					joined.className = 'hidden';
					username.focus();

					user = null;
					amq.removeListener('chat', chatTopic);
				}
				if (user != null)
					amq.sendMessage(chatTopic, '<message type="ping" from="' + user + '"/>');
				break;
			}
		}

		chat.scrollTop = chat.scrollHeight - chat.clientHeight;
	};

	var getKeyCode = function (ev) {
		var keyc;
		if (window.event) keyc = window.event.keyCode;
		else keyc = ev.keyCode;
		return keyc;
	};

	// Again, you would generally use your particular js library to attach
	// event handlers. However, I wanted to remove the dependency on the
	// behaviors.js file in the original code, and I am demonstrating a library
	// that can work with a variety of js libraries, so we are going old-school.
	var addEvent = function(obj, type, fn) {
		if (obj.addEventListener)
			obj.addEventListener(type, fn, false);
		else if (obj.attachEvent) {
			obj["e"+type+fn] = fn;
			obj[type+fn] = function() { obj["e"+type+fn]( window.event ); }
			obj.attachEvent( "on"+type, obj[type+fn] );
		}
	};

	var initEventHandlers = function() {
		addEvent(username, 'keyup', function(ev) {
			var keyc = getKeyCode(ev);
			if (keyc == 13 || keyc == 10) {
				org.activemq.Chat.join();
				return false;
			}
			return true;
		});

		addEvent(document.getElementById('joinB'), 'click', function() {
			org.activemq.Chat.join();
			return true;
		});

		addEvent(phrase, 'keyup', function(ev) {
			var keyc = getKeyCode(ev);

			if (keyc == 13 || keyc == 10) {
				var text = phrase.value;
				phrase.value = '';
				org.activemq.Chat.chat(text);
				return false;
			}
			return true;
		});

		addEvent(document.getElementById('sendB'), 'click', function() {
			var text = phrase.value;
			phrase.value = '';
			org.activemq.Chat.chat(text);
		});

		addEvent(document.getElementById('leaveB'), 'click', function() {
			org.activemq.Chat.leave();
			return false;
		});
	};

	return {
		join: function() {
			var name = username.value;
			if (name == null || name.length == 0) {
				alert('Please enter a username!');
			} else {
				user = name;

				amq.addListener('chat', chatTopic, chatHandler);
				join.className = 'hidden';
				joined.className = '';
				phrase.focus();

				amq.sendMessage(chatTopic, '<message type="join" from="' + user + '"/>');
			}
		},

		leave: function() {
			amq.sendMessage(chatTopic, '<message type="leave" from="' + user + '"/>');
		},

		chat: function(text) {
			if (text != null && text.length > 0) {
				// TODO more encoding?
				text = text.replace('<', '&lt;');
				text = text.replace('>', '&gt;');

				amq.sendMessage(chatTopic, '<message type="chat" from="' + user + '">' + text + '</message>');
			}
		},

		init: function() {
			join = document.getElementById('join');
			joined = document.getElementById('joined');
			chat = document.getElementById('chat');
			members = document.getElementById('members');
			username = document.getElementById('username');
			phrase = document.getElementById('phrase');

			if (join.className == 'hidden' && joined.className == 'hidden') {
				join.className = '';
				joined.className = 'hidden';
				username.focus();
			}

			initEventHandlers();
		}
	}
}();













