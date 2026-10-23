---
title: "REST protocols"
slug: /rest-protocols
---


There are many ways to map JMS to REST...

Crappy non-REST
---------------

Send via
```
POST /queue/Destination
```
Consume
```
GET /queue/Destination
```
This is bad as the GET is not idempotent. We can add a user ID or use a cookie
```
GET /queue/Destination?jsessionId=....
```
though a caching proxy may keep returning the same message

Simple REST using 1 at a time delivery
--------------------------------------

Subscribe via
```
POST /subscribe/consumerId/Destination  
POST /unsubscribe/consumerId/Destination
```
Then get messges via
```
POST /nextMessageForMe/consumerId
```
If you want to acknowledge messages you then
```
POST /ack/consumerId/messageId
```
Batch message REST
------------------
```
POST lockNextMessage/consumerId?max=10 
```
-\> give back up to 10 message URLs
```
GET message/consumerId/messageId
```
acknowledge them via
```
DELETE /consumerId/messages/messageId
```
### Further options...

You can include ACK messages inside the POST; so the 2nd POST could include the ACKs - also you could return the batch of messages as well.

