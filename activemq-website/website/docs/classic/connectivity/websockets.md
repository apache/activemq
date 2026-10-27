---
title: "WebSockets"
slug: /websockets
---


Introduction
============

Besides [Ajax](ajax) API, starting with version **5.4.0** onwards, you can use HTML5 [WebSockets](http://dev.w3.org/html5/websockets/) to exchange messages with the broker from your browser.

HTML 5 introduced web sockets, as a standardized way to communicate asynchronously with the server from a web page. This is practically an ideal channel for implementing asynchronous messaging for web pages. Since JavaScript easily handles text and JSON formatted data, [Stomp](http://activemq.apache.org/stomp/) protocol is a good choice for the wire protocol to be used over web sockets. Since version **5.9.0**, we also support efficient binary MQTT protocol over Web sockets.

This solution should bring better messaging capabilities to JavaScript clients then simple Ajax API, as implementing Stomp or MQTT in JavaScript brings much more messaging-oriented API and features such as transactions, for example.

Configuration
=============

ActiveMQ comes with _WebSocket_ transport which implements Stomp over WebSockets functionality.

To configure it, you need to place something like this to your ActiveMQ configuration file

```xml
<transportConnectors>
  <transportConnector name="websocket" uri="ws://0.0.0.0:61614"/>
</transportConnectors>
```

One thing worth noting is that web sockets (just as Ajax) implements the _same origin policy_, so you can access only brokers running on the same host as the web application running the client.

Secure Web Sockets
==================

Version 5.7.0 introduced _Secure Web Socket_ transport. To configure it you need two things. First, you need to configure a new transport connector like this

```xml
<transportConnectors>
  <transportConnector name="secure_websocket" uri="wss://0.0.0.0:61614"/>
</transportConnectors>
```

Note that we use _wss_ url prefix to denote a secured version of the protocol. Next you need to provide SSL context for this transport. You can do that by providing _sslContext_ in your broker configuration in a similar fashion as you'd do for _ssl_ or _https_ transports.

```xml
<sslContext>
    <sslContext keyStore="file:${activemq.conf}/broker.ks"
                keyStorePassword="password" trustStore="file:${activemq.conf}/broker.ts"
                trustStorePassword="password"
    />
</sslContext>
```

That's it, your secure websocket transport is ready. Take a look at the next section to see how to use a demo to test it out.

Demos
=====

As of version 5.6.0, an adapted demo of [stomp-websocket](http://github.com/jmesnil/stomp-websocket) library is integrated with ActiveMQ web demo application.  
As of version 5.9.0, we have a similar demo using MQTT and [Eclipse Paho client](http://git.eclipse.org/c/paho/org.eclipse.paho.mqtt.javascript.git)

To see demos:

*   Start the broker with `ws` transport (you can use activemq-demo.xml configuration for that)
*   Go to [http://localhost:8161/demo/websocket](http://localhost:8161/demo/websocket) to check Stomp example
*   Go to [http://localhost:8161/demo/mqtt](http://localhost:8161/demo/mqtt) to check MQTT example

Clients
=======

*   [Stomple](http://github.com/krukow/stomple) by [Karl Krukow](http://blog.higher-order.net/)
*   [stomp-websocket](http://github.com/jmesnil/stomp-websocket) by [Jeff Mesnil](http://jmesnil.net/weblog/)
*   [Eclipse Paho MQTT JavaScript client](http://git.eclipse.org/c/paho/org.eclipse.paho.mqtt.javascript.git)

More Resources
==============

*   [ActiveMQ 5.4: Stomp over Web Sockets](http://www.nighttale.net/activemq/activemq-54-stomp-over-web-sockets.html)
*   [Stomple RC1: Combining WebSockets and Reliable Messaging](http://blog.higher-order.net/2010/06/01/stomple-rc1-combining-websockets-and-reliable-messaging/)
*   [Stomp On Web Sockets](http://jmesnil.net/stomp-websocket/doc/)

