---
title: "Life Cycle"
slug: /life-cycle
---


Life cycle of Channels
----------------------

Blaze Channels are in one of five states:

### Constructed

the Channel isn't initialized or has been shutDown

### initialized

you can explicitly initialize a Channel by invoking its init() method. At this point its Configuration is set
```
BlazeChannelFactory factory = new BlazeChannelFactory();
BlazeChannel channel = factory.createChannel();
channel.init();
```

### started

this will implicitly initialize the channel and start the Channel's underlying communication with its peers:
```
BlazeChannelFactory factory = new BlazeChannelFactory();
BlazeChannel channel = factory.createChannel();
channel.start();
```

### stopped

this will stop communication - however you are able to re-start the channel at a latter point
```
BlazeChannelFactory factory = new BlazeChannelFactory();
BlazeChannel channel = factory.createChannel();
channel.stop();
// do something else
...
//re-start
channel.start();
```

### shut down

this will implicitly call stop() - and de-construct the channel. It is possible to re-initialize the channel again - and it is recommend that to apply configuration changes to the channel - it be shut down and re-started - e.g.

```
BlazeChannelFactory factory = new BlazeChannelFactory();
BlazeChannel channel = factory.createChannel();
channel.shutDown();
// change the congiguration
channel.getConfiguration().setBroadcastURI("tcp://localhost:60661");
//re-start
channel.start();
```
