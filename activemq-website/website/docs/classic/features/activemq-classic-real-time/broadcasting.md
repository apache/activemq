---
title: "Broadcasting"
slug: /broadcasting
---


ActiveBlaze provides infrastructure for fast, reliable peer to peer messaging to meet the demands of high performance and collaborative applications.  
Using the basic building block of a Channel, there exists a functional hierarchy used for different problem domains, each one a super-set of the previous one.

The BlazeChannel supports broadcasting on Topics - either using reliable multicast or point-cast.

To use a BlazeChannel create one from the a factory:
```
import org.apache.activeblaze.*;
...
BlazeChannelFactory factory = new BlazeChannelFactory();
BlazeChannel sender = factory.createChannel();
//start the channel and send a message
sender.start();

String destination = "foo.bar";
BlazeMessage msg = new BlazeMessage("test payload");
sender.broadcast(destination,msg);
//shutdown the sender
sender.shutDown();

You can similarly subscribe to Topic messages by using a listener

BlazeChannel receiver = factory.createChannel();
receiver.start();

//add a listener

receiver.addBlazeTopicMessageListener(destination, new BlazeMessageListener() {
            public void onMessage(BlazeMessage msg) {
                System.out.println("Got a msg: " + msg);
            }
        });

receiver.shutDown();
```
