---
title: "The ActiveBlaze Message type"
slug: /the-activeblaze-message-type
---


The foundation of message and event parsing in ActiveBlaze is the BlazeMessage.  
A BlazeMessage is a a Map of key value pairs, where the keys are strings and the values are primitive objects.

The values supported are:

*   byte
*   bytes[]
*   char
*   short
*   int
*   long
*   float
*   double
*   Map (supports recursion)

You can create a BlazeMessage by its default constructor - e.g.
```
import org.apache.activeblaze.*;
...

BlazeMessage msg = new BlazeMessage();
msg.setFloatValue("rate",0.94f);
```
There's a few utility methods to support passing around standard objects - e.g.
```
import org.apache.activeblaze.*;
...

BlazeMessage msg = new BlazeMessage("test string");
String text = msg.getText();

BlazeMessage msg = new BlazeMessage();
msg.setText("test string");
String text = msg.getText();

...

byte[] data = getSomeData();
BlazeMessage msg = new BlazeMessage(data);
byte[] result  = msg.getBytes();

BlazeMessage msg = new BlazeMessage();
msg.setBytes("data);
byte[] result = msg.getData();

...

Date date = new Date();
BlazeMessage msg = new BlazeMessage(date);
Date result = msg.getObject();

BlazeMessage msg = new BlazeMessage();
msg.setObject(date);
Object result = msg.getObject();
```

