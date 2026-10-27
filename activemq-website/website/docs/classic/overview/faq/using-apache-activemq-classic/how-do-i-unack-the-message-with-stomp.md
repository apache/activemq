---
title: "How do I unack the message with Stomp"
slug: /how-do-i-unack-the-message-with-stomp
---

There is no explicit "unack" command in Stomp. Once the client receives the message it cannot be marked as "unconsumed" and sent to another subscriber (or redelivered to the same subscriber again). It's up to your application (or Stomp client) to handle failed processing of received messages and implement "message redelivery".

Stomp transactions are often mistakenly considered to be a solution for this use case. But that's not the case, since transactions are only related to sending messages and acknowledgments. If you start a transaction, send a message ack in a transaction and finally abort it, the message will not be redelivered again. It just means that broker will not send any more messages to the client if the prefetch limit is reached.

Take a look at the following example:
```
StompConnection connection = new StompConnection();
connection.open("localhost", 61613);
		
connection.connect("system", "manager");
		
connection.send("/queue/test", "message 1");
connection.send("/queue/test", "message 2");
connection.send("/queue/test", "message 3");
		
HashMap<String, String> headers = new HashMap<String, String>();
headers.put("activemq.prefetchSize", "1");
connection.subscribe("/queue/test", "client", headers);
		
connection.begin("tx1");
StompFrame frame = connection.receive();
System.out.println(frame.getBody());
connection.ack(frame, "tx1");
connection.abort("tx1");
		
connection.begin("tx2");
connection.ack(frame, "tx2");        //sending the ack again
frame = connection.receive();
System.out.println(frame.getBody());
connection.ack(frame, "tx2");
connection.commit("tx2");
		
connection.begin("tx3");
frame = connection.receive();
System.out.println(frame.getBody());
connection.ack(frame, "tx3");
connection.commit("tx3");
```
This simple application will print
```
message 1
message 2
message 3
```
Since the transaction `tx1` has been aborted, we needed to acknowledge that message again in `tx2` in order to be able to receive the next message (since the prefetch size used is 1).

Also take a look at these pages for more info:

*   [http://activemq.apache.org/stomp/stomp10/additional.html#transaction_handling](http://activemq.apache.org/stomp/stomp10/additional.html#transaction_handling)
*   [what-is-the-prefetch-limit-for](what-is-the-prefetch-limit-for)

