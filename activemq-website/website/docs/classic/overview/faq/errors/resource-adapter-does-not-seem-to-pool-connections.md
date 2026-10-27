---
title: "Resource Adapter does not seem to pool connections"
slug: /resource-adapter-does-not-seem-to-pool-connections
---

It may seem like the resource adapter when used in an app server like geronimo or jboss is not pooling connections. Looking the the ActiveMQ broker logs, it will show multiple message for each use of a pooled conntion simlilar to:
```
16:43:07 INFO  Adding new client: ID:localhost-58375-1120682586690-1:0 on transport: TcpTransportChannel: Socket\[addr=/127.0.0.1,port=58376,localport=61616\]
16:43:08 INFO  Removing client: ID:localhost-58375-1120682586690-1:0 on transport: TcpTransportChannel: Socket\[addr=/127.0.0.1,port=58376,localport=61616\]
```
At first glance it may seem like a new connection is being established.. but if you look at the logs is more detail, you will noticed that the client connection id's are being reused since the connections are being pooled. This behavior is a side-effect of the connection being reset to a 'cleaned up' state. It allows the next client to use the connection as if it has just been created.

So these messages are normal, and YES, your jms connections are being pooled!

