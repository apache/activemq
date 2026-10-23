---
title: "Title"
slug: /chat
---

 Chat Example - ActiveMQ Web Demos

``` 
// Note, normally you wouldn't just add an onload function in this 
// manner. In fact, you typically want to fire this method on the 
// document.onready event, however this type of functionality is verbose // and best left to the domain of your favorite js library. 
// 
// For example, in jQuery the following onload would be replaced with: 
// jQuery(function() { 
// org.activemq.Amq.init({ uri: 'amq' }); 
// org.activemq.Chat.init(); 
// } window.onload = function() { org.activemq.Amq.init({ uri: 'amq', logging: true, timeout: 45, clientId:(new Date()).getTime().toString() }); org.activemq.Chat.init(); }; 
```

Chat Example
============

Welcome to the Ajax chat example

Username:   Join

Chat:   Send Leave

This Chat example creates an ActiveMQ broker using the configuration information found in the `web.xml` file. There isn't much there. Just a name-value parameter named `org.apache.activemq.brokerURL` is assigned a value of `vm://localhost?broker.persistent=false`. This is enough however to lazy-initialize the broker when it is needed.

The client leverages a javascript library `amq.js` to perform all of the JMS-related client side code. This involves establishing a communication pipeline to the JMS server. This pipeline uses a long-poll connection to the server. All JMS communication will be received down this pipe, and when the JMS server has no traffic to send, this pipeline will patiently wait until there is new traffic or until it times out. If a timeout does occur, the connection will reconnect to the server for another round. (Of course you will want/need to use a server that supports continuations in order for this to scale beyond a few hundred clients.)

The `chat.js` file contains the script to respond to the UI interactions. It also talks to the `amq.js` file to send messages and provides a message handler that will respond to incoming JMS messages.

There is no server-side state in this application. The client sets up a JMS Topic on the server and attaches itself as a listener to this topic. From that point, all messages that are sent to the topic are received by each listener. Even the list of members in the chat room are the result of clients replying to a ping request.

Please note that `amq.js` has been refactored to allow AJAX calls to be made using any javascript library. Example adapter classes for [jQuery](http://jquery.com/) and [Prototype](http://www.prototypejs.org/) have been provided.

### Useful Links

*   [Documentation](http://activemq.apache.org/ "The most popular and powerful open source Message Broker")
*   [FAQ](http://activemq.apache.orgfaq)
*   [Downloads](http://activemq.apache.org/download)
*   [Forums](http://activemq.apache.org/discussion-forums)