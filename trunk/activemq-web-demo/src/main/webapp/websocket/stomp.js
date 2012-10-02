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
(function() {
  var Client, Stomp, WebSocketStompMock;
  var __hasProp = Object.prototype.hasOwnProperty, __bind = function(fn, me){ return function(){ return fn.apply(me, arguments); }; };
  Stomp = {
    frame: function(command, headers, body) {
      if (headers == null) {
        headers = [];
      }
      if (body == null) {
        body = '';
      }
      return {
        command: command,
        headers: headers,
        body: body,
        id: headers.id,
        receipt: headers.receipt,
        transaction: headers.transaction,
        destination: headers.destination,
        subscription: headers.subscription,
        error: null,
        toString: function() {
          var lines, name, value;
          lines = [command];
          for (name in headers) {
            if (!__hasProp.call(headers, name)) continue;
            value = headers[name];
            lines.push("" + name + ":" + value);
          }
          lines.push('\n' + body);
          return lines.join('\n');
        }
      };
    },
    unmarshal: function(data) {
      var body, chr, command, divider, headerLines, headers, i, idx, line, trim, _ref, _ref2, _ref3;
      divider = data.search(/\n\n/);
      headerLines = data.substring(0, divider).split('\n');
      command = headerLines.shift();
      headers = {};
      body = '';
      trim = function(str) {
        return str.replace(/^\s+/g, '').replace(/\s+$/g, '');
      };
      line = idx = null;
      for (i = 0, _ref = headerLines.length; 0 <= _ref ? i < _ref : i > _ref; 0 <= _ref ? i++ : i--) {
        line = headerLines[i];
        idx = line.indexOf(':');
        headers[trim(line.substring(0, idx))] = trim(line.substring(idx + 1));
      }
      chr = null;
      for (i = _ref2 = divider + 2, _ref3 = data.length; _ref2 <= _ref3 ? i < _ref3 : i > _ref3; _ref2 <= _ref3 ? i++ : i--) {
        chr = data.charAt(i);
        if (chr === '\0') {
          break;
        }
        body += chr;
      }
      return Stomp.frame(command, headers, body);
    },
    marshal: function(command, headers, body) {
      return Stomp.frame(command, headers, body).toString() + '\0';
    },
    client: function(url) {
      return new Client(url);
    }
  };
  Client = (function() {
    function Client(url) {
      this.url = url;
      this.counter = 0;
      this.connected = false;
      this.subscriptions = {};
    }
    Client.prototype._transmit = function(command, headers, body) {
      var out;
      out = Stomp.marshal(command, headers, body);
      if (typeof this.debug === "function") {
        this.debug(">>> " + out);
      }
      return this.ws.send(out);
    };
    Client.prototype.connect = function(login_, passcode_, connectCallback, errorCallback) {
      var klass;
      if (typeof this.debug === "function") {
        this.debug("Opening Web Socket...");
      }
      klass = WebSocketStompMock || WebSocket;
      this.ws = new klass(this.url);
      this.ws.onmessage = __bind(function(evt) {
        var frame, onreceive;
        if (typeof this.debug === "function") {
          this.debug('<<< ' + evt.data);
        }
        frame = Stomp.unmarshal(evt.data);
        if (frame.command === "CONNECTED" && connectCallback) {
          this.connected = true;
          return connectCallback(frame);
        } else if (frame.command === "MESSAGE") {
          onreceive = this.subscriptions[frame.headers.subscription];
          return typeof onreceive === "function" ? onreceive(frame) : void 0;
        }
      }, this);
      this.ws.onclose = __bind(function() {
        var msg;
        msg = "Whoops! Lost connection to " + this.url;
        if (typeof this.debug === "function") {
          this.debug(msg);
        }
        return typeof errorCallback === "function" ? errorCallback(msg) : void 0;
      }, this);
      this.ws.onopen = __bind(function() {
        if (typeof this.debug === "function") {
          this.debug('Web Socket Opened...');
        }
        return this._transmit("CONNECT", {
          login: login_,
          passcode: passcode_
        });
      }, this);
      return this.connectCallback = connectCallback;
    };
    Client.prototype.disconnect = function(disconnectCallback) {
      this._transmit("DISCONNECT");
      this.ws.close();
      this.connected = false;
      return typeof disconnectCallback === "function" ? disconnectCallback() : void 0;
    };
    Client.prototype.send = function(destination, headers, body) {
      if (headers == null) {
        headers = {};
      }
      if (body == null) {
        body = '';
      }
      headers.destination = destination;
      return this._transmit("SEND", headers, body);
    };
    Client.prototype.subscribe = function(destination, callback, headers) {
      var id;
      if (headers == null) {
        headers = {};
      }
      id = "sub-" + this.counter++;
      headers.destination = destination;
      headers.id = id;
      this.subscriptions[id] = callback;
      this._transmit("SUBSCRIBE", headers);
      return id;
    };
    Client.prototype.unsubscribe = function(id, headers) {
      if (headers == null) {
        headers = {};
      }
      headers.id = id;
      delete this.subscriptions[id];
      return this._transmit("UNSUBSCRIBE", headers);
    };
    Client.prototype.begin = function(transaction, headers) {
      if (headers == null) {
        headers = {};
      }
      headers.transaction = transaction;
      return this._transmit("BEGIN", headers);
    };
    Client.prototype.commit = function(transaction, headers) {
      if (headers == null) {
        headers = {};
      }
      headers.transaction = transaction;
      return this._transmit("COMMIT", headers);
    };
    Client.prototype.abort = function(transaction, headers) {
      if (headers == null) {
        headers = {};
      }
      headers.transaction = transaction;
      return this._transmit("ABORT", headers);
    };
    Client.prototype.ack = function(message_id, headers) {
      if (headers == null) {
        headers = {};
      }
      headers["message-id"] = message_id;
      return this._transmit("ACK", headers);
    };
    return Client;
  })();
  if (typeof window !== "undefined" && window !== null) {
    window.Stomp = Stomp;
  } else {
    exports.Stomp = Stomp;
    WebSocketStompMock = require('./test/server.mock.js').StompServerMock;
  }
}).call(this);