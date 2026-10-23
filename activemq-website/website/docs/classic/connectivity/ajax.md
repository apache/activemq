---
title: "Ajax"
slug: /ajax
---


Introduction
============

ActiveMQ supports [Ajax](http://en.wikipedia.org/wiki/Ajax_%28programming%29) which is an Asychronous Javascript And Xml mechanism for real time web applications. This means you can create highly real time web applications taking full advantage of the publish/subscribe nature of ActiveMQ

Ajax allows a regular DHTML client (with JavaScript and a modern version 5 or later web browser) to send and receive messages over the web. Ajax support in ActiveMQ builds on the same basis as the [REST](rest) connector for ActiveMQ which allows any web capable device to send or receive messages over JMS.

To see Ajax in action, try [running the examples](web-samples)

The Servlet
===========

The AMQ AjaxServlet needs to be installed in your webapplications to support JMS over Ajax:

```
<servlet>
  <servlet-name>AjaxServlet</servlet-name>
  <servlet-class>org.apache.activemq.web.AjaxServlet</servlet-class>
</servlet>
...
<servlet-mapping>
  <servlet-name>AjaxServlet</servlet-name>
  <url-pattern>/amq/*</url-pattern>
</servlet-mapping>
```
The servlet both serves the required js files and handles the JMS requests and responses.

Javascript API
==============

The ajax featues of amq are provided on the client side by the [amq.js](https://github.com/apache/activemq/blob/main/activemq-web-demo/src/main/webapp/js/amq.js) script. Beginning with ActiveMQ 5.4, this script utilizes one of three different adapters to support ajax communication with the server. Current [jQuery](http://jquery.org), [Prototype](http://prototypejs.org), and [Dojo](http://www.dojotoolkit.org) are supported, and recent versions of all three libraries are shipped with ActiveMQ.
```
<script type="text/javascript" src="js/jquery-1.4.2.min.js"></script>
<script type="text/javascript" src="js/amq\_jquery\_adapter.js"></script>
<script type="text/javascript" src="js/amq.js"></script>
<script type="text/javascript">
  var amq = org.activemq.Amq;
  amq.init({ 
    uri: 'amq', 
    logging: true,
    timeout: 20
  });
</script>
```
Including these scripts results in the creation of a javascript object called `amq`, which provides the API to send messages and to subscribe to channels and topics.

Sending a message
-----------------

All that is required to send a JMS message from the javascript client, is to call the method:
```
amq.sendMessage(myDestination,myMessage);
```
where `myDestination` is the URL string address of the destination (e.g. `topic://MY.NAME` or `channel://MY.NAME`) and `myMessage` is any well formed XML or plain text encoded as XML content.

Receiving messages
------------------

To receive messages, the client must define a message handling function and register it with the [amq](https://github.com/apache/activemq/tree/main/activemq-web-demo/src/main/webapp/js/amq.js) object. For example:
```
var myHandler =
{
  rcvMessage: function(message)
  {
     alert("received "+message);
  }
};

amq.addListener(myId,myDestination,myHandler.rcvMessage);
```
where `myId` is a string identifier that can be used for a later call to `amq.removeHandler(myId)` and `myDestination` is a URL string address of the destination (e.g. `topic://MY.NAME` or `channel://MY.NAME`). When a message is received, a call back to the `myHandler.rcvMessage` function passes the message to your handling code.  
The "message" is actually a text of the Text message or a String representation (`toString()`) in case of Object messages.

Be aware that, by default, messages published via [Stomp](stomp) which include a `content-length` header will be converted by ActiveMQ to binary messages, and will not be visible to your web clients. Beginning with ActiveMQ 5.4.0, you can resolve this problem by always setting the [`amq-msg-type` header](https://issues.apache.org/jira/browse/AMQ-2833) to `text` in messages which will may be consumed by web clients.

### Selector support

By default, an ajax client will receive all messages on a topic or queue it is subscribed to. In [ActiveMQ 5.4.1](http://activemq.apache.orgOverview/Download/activemq-541-release) amq.js supports [JMS selectors](http://activemq.apache.orgFeatures/Consumer Features/selectors) since it is frequently useful to receive only a subset of these messages. Selectors are supplied to an `amq.addListener` call by way of an optional 4th parameter.
```
amq.addListener( myId, myDestination, myHandler.rcvMessage, { selector:"identifier='TEST'" } );
```
When used in this way, the Javascript client will receive only messages containing an `identifier` header set to the value `TEST`.

Using AMQ Ajax in Multiple Browser Windows
------------------------------------------

All windows or tabs in a single browser share the same `JSESSIONID` on the ActiveMQ server. Unless the server can distinguish listeners from multiple windows, messages which were intended for 1 window will be delivered to another one instead. Effectively, this means that amq.js could be active in only a single browser window at any given time. Beginning in [ActiveMQ 5.4.2](http://activemq.apache.orgOverview/DownloadOverview/Download/Overview/Download/activemq-542-release), this is resolved by allowing each call to `amq.init` to specify a unique `clientId`. When this is done, multiple windows in the same browser can happily co-exist. Each can have a separate set of message subscriptions on the broker with no interactions between them.

In this example, we use the current time (at the time the web page is loaded) as a unique identifier. This is effective as long as two browser windows are not opened within the same millisecond, and is the approach used by the example [chat.md](https://github.com/apache/activemq/tree/main/activemq-web-demo/src/main/webappchat) included with ActiveMQ. Other schemes to ensure the uniqueness of `clientId` can easily be devised. Note that this `clientId` need only be unique within a single session. (Browser windows opened in the same millisecond in separate browsers will not interact, since they are in different sessions.)
```
org.activemq.Amq.init({
  uri: 'amq', 
  logging: true, 
  timeout: 45, 
  clientId:(new Date()).getTime().toString() 
});
```
Note that this `clientId` is common to all message subscriptions in a single tab or window, and is entirely different from the `clientId` which is supplied as a first argument in `amq.addListener` calls.

*   In `amq.init`, `clientId` serves to distinguish different web clients sharing the same `JSESSIONID`. All windows in a single browser need a unique `clientId` when they call `amq.init`.
*   In `amq.addListener`, `clientId` is used to associate a message subscription with the callback function which should be invoked when a message is received for that subscription. These `clientId` values are internal to each web page, and do not need to be unique across multiple windows or tabs.

How it works
============

AjaxServlet and MessageListenerServlet
--------------------------------------

The ajax featues of amq are handled on the server side by the [AjaxServlet](https://github.com/apache/activemq/tree/main/activemq-web/src/main/java/org/apache/activemq/web/AjaxServlet.java) which extends the [MessageListenerServlet](https://github.com/apache/activemq/tree/main/activemq-web/src/main/java/org/apache/activemq/web/MessageListenerServlet.java). This servlet is responsible for tracking the existing clients (using a HttpSesssion) and lazily creating the AMQ and javax.jms objects required by the client to send and receive messages (eg. Destination, MessageConsumer, MessageAVailableListener). This servlet should be mapped to `/amq/*` in the web application context serving the Ajax client (this can be changed, but the client javascript `amq.uri` field needs to be updated to match.)

Client Sending messages
-----------------------

When a message is sent from the client it is encoded as the content of a POST request, using the API of one of the supported connection adapters (jQuery, Prototype, or Dojo) for [XmlHttpRequest](http://jibbering.com/2002/4/httprequest.html). The [amq](https://github.com/apache/activemq/tree/main/activemq-web-demo/src/main/webapp/js/amq.js) object may combine several sendMessage calls into a single POST if it can do so without adding additional delays (see polling below).

When the MessageListenerServlet receives a POST, the messages are decoded as `application/x-www-form-urlencoded` parameters with their type (in this case `send` as opposed to `listen` or `unlisten` see below) and destination. If a destination channel or topic do not exist, it is created. The message is sent to the destination as a TextMessage.

Listening for messages
----------------------

When a client registers a listener, a message subscription request is sent from the client to the server in a POST in the same way as a message, but with a type of `listen`. When the MessageListenerServlet receives a `listen` message, it lazily creates a MessageAvailableConsumer and registers a Listener on it.

Waiting Poll for messages
-------------------------

When a Listener created by the MessageListenerServlet is called to indicate that a message is available, due to the limitations of the HTTP client-server model, it is not possible to send that message directly to the ajax client. Instead the client must perform a special type of **Poll** for messages. Polling normally means periodically making a request to see if there are messages available and there is a trade off: either the poll frequency is high and excessive load is generated when the system is idle; or the frequency is low and the latency for detecting new messages is high.

To avoid the load vs latency tradeoff, AMQ uses a waiting poll mechanism. As soon as the amq.js script is loaded, the client begins polling the server for available messages. A poll request can be sent as a GET request or as a POST if there are other messages ready to be delivered from the client to the server. When the MessageListenerServlet receives a poll it:

1.  if the poll request is a POST, all `send`, `listen` and `unlisten` messages are processed
2.  if there are no messages available for the client on any of the subscribed channels or topic, the servlet suspends the request handling until:
    *   A MessageAvailableConsumer Listener is called to indicate that a message is now available; or
    *   A timeout expires (normally around 30 seconds, which is less than all common TCP/IP, proxy and browser timeouts).
3.  A HTTP response is returned to the client containing all available messages encapsulated as `text/xml`.

When the amq.js javascipt receives the response to the poll, it processes all the messages by passing them to the registered handler functions. Once it has processed all the messages, it immediately sends another poll to the server.

Thus the idle state of the amq ajax feature is a poll request "parked" in the server, waiting for messages to be sent to the client. Periodically this "parked" request is refreshed by a timeout that prevents any TCP/IP, proxy or browser timeout closing the connection. The server is thus able to asynchronously send a message to the client by waking up the "parked" request and allowing the response to be sent.

The client is able to asynchronously send a message to the server by creating (or using an existing) second connection to the server. However, during the processing of the poll response, normal client message sending is suspended, so that all messages to be sent are queued and sent as a single POST with the poll that will be sent (with no delay) at the end of the processing. This ensures that only two connections are required between client and server (the normal for most browsers).

Threadless Waiting
------------------

The waiting poll described above is implemented using the [Jetty 6 Continuations](http://docs.codehaus.org/display/JETTY/Continuations) mechanism. This allows the thread associated with the request to be released during the wait, so that the container does not need to have a thread per client (which may be a large number). If another servlet container is used, the Continuation mechanism falls back to use a wait and the thread is not released.

Comparison to Pushlets
======================

Firstly we could easily add support for pushlets to ActiveMQ. However we prefer the Ajax approach for various reasons

*   using Ajax means that we use a distinct HTTP request for each send/receive which is much more friendly to web infrastructure (firewalls, proxies, caches and so forth) rather than having an infinitely-long GET.
*   we can still take advantage of HTTP 1.1 keep-alive sockets and pipeline processing to gain the efficiency of a single socket used for communication between the client and server side; though in a way that works with any HTTP-capable infrastructure
*   the server is pure REST and so will work with any client side (rather than being tied to custom JavaScript function calls used on the page which the Pushlet approach requires). So Pushlets tie the server to the web page; with Ajax we can have a generic service which works with any page.
*   the client can be in control over frequency of polling & timeouts. e.g. it can avoid the memory issues of Pushlets in some browsers by using a 20-second timeout HTTP GET. Or using a zero timeout GET to poll queues.
*   its easier to take full advantage of HTTP encoding of messages, rather than using JavaScript function calls as the transfer protocol.
*   pushlets assume the server knows what functions are used on the client side as the server basically writes JavaScript function calls down the scoket - it's better for us to send generic XML packets (or strings or whatever the message format is) and let the JavaScript client side be totally decoupled from the server side
*   Ajax supports clean XML support allowing full XML documents to be streamed to the client for rich messages which are easy to process via standard JavaScript DOM support
