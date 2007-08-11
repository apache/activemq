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

// -----------------
// Original code by Joe Walnes
// -----------------

function Connection(webmqUrl, id) {
    this.messageListeners = new Array()
    this.webmqUrl = webmqUrl
    if (id == null) {
        // TODO use secure random id generation
        id = Math.round(Math.random() * 100000000)
    }
    this.id = id
    // TODO don't start anything until document has finished loading
    var http = this.createHttpControl()
    this.getNextMessageAndLoop(webmqUrl, id, http)
}

Connection.prototype.getNextMessageAndLoop = function(webmqUrl, id, http) {
    http.open("GET", webmqUrl + "?id=" + id + "&xml=true", true)
    var connection = this
    http.onreadystatechange = function() {     
        if (http.readyState == 4) {
            var ok
            try {
                ok = http.status && http.status == 200
            } 
            catch (e) {
                ok = false // sometimes accessing the http.status fields causes errors in firefox. dunno why. -joe
            }
            if (ok) {
                connection.processIncomingMessage(http)
            }
            // why do we have to create a new instance?
            // this is not required on firefox but is on mozilla
            //http.abort()
            http = connection.createHttpControl()
            connection.getNextMessageAndLoop(webmqUrl, id, http)
        }
    }
    http.send(null)
}

Connection.prototype.sendMessage = function(destination, message) {
    // TODO should post via body rather than URL
    // TODO specify destination in message
    var http = this.createHttpControl()
    http.open("POST", this.webmqUrl + "?id=" + this.id + "&body=" + message, true)
    http.send(null)
}

Connection.prototype.processIncomingMessage = function(http) {
    var destination = http.getResponseHeader("destination")
    var message = http.responseXML
    if (message == null) {
        message = http.responseText
    }
    //alert(message.responseText)
    for (var j = 0; j < this.messageListeners.length; j++) {
        var registration = this.messageListeners[j]
        if (registration.matcher(destination)) {
            registration.messageListener(message, destination)
        }
    }
}

Connection.prototype.addMessageListener = function(matcher, messageListener) {
    var wrappedMatcher
    if (matcher.constructor == RegExp) {
        wrappedMatcher = function(destination) { 
            return matcher.test(destination)
        }
    } 
    else if (matcher.constructor == String) {
        wrappedMatcher = function(destination) { 
            return matcher == destination
        }
    } 
    else {
        wrappedMatcher = matcher
    }
    this.messageListeners[this.messageListeners.length] = { matcher: wrappedMatcher, messageListener: messageListener }
}

Connection.prototype.createHttpControl = function() {
    // for details on using XMLHttpRequest see
    // http://webfx.eae.net/dhtml/xmlextras/xmlextras.html
   try {
      if (window.XMLHttpRequest) {
         var req = new XMLHttpRequest()

         // some older versions of Moz did not support the readyState property
         // and the onreadystate event so we patch it!
         if (req.readyState == null) {
            req.readyState = 1
            req.addEventListener("load", function () {
               req.readyState = 4
               if (typeof req.onreadystatechange == "function") {
                  req.onreadystatechange()
               }
            }, false)
         }

         return req
      }
      if (window.ActiveXObject) {
         return new ActiveXObject(this.getControlPrefix() + ".XmlHttp")
      }
   }
   catch (ex) {}
   // fell through
   throw new Error("Your browser does not support XmlHttp objects")
}

Connection.prototype.getControlPrefix = function() {
   if (this.prefix) {
      return this.prefix
   }

   var prefixes = ["MSXML2", "Microsoft", "MSXML", "MSXML3"]
   var o, o2
   for (var i = 0; i < prefixes.length; i++) {
      try {
         // try to create the objects
         o = new ActiveXObject(prefixes[i] + ".XmlHttp")
         o2 = new ActiveXObject(prefixes[i] + ".XmlDom")
         return this.prefix = prefixes[i]
      }
      catch (ex) {}
   }
   throw new Error("Could not find an installed XML parser")
}

