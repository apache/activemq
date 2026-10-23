---
title: "Structured Message Properties and MapMessages"
slug: /structured-message-properties-and-mapmessages
---


Structured Message Properties and MapMessages
---------------------------------------------

This JMS extension feature allows you to attach Map and List properties to any JMS Message or to use nested Maps and Lists inside a MapMessage. This allows you to efficiently send typesafe structured information in your messages without requiring the overhead of XML/XSD parsing which are parseable in other languages like C, C++, C# etc.

This enhancement allows any Map or List implementation to be used in the value parameter of these JMS methods

*   [Message.setObjectProperty(key, value)](http://java.sun.com/j2ee/1.4/docs/api/javax/jms/Message.html#setObjectProperty(java.lang.String,%20java.lang.Object))
*   [MapMessage.setObject(key, value)](http://java.sun.com/j2ee/1.4/docs/api/javax/jms/MapMessage.html#setObject(java.lang.String,%20java.lang.Object))

The value can be an arbitrarily nested structure of Maps, Lists and primitive objects (numbers and strings). 

**Note:** The depth of nested structures (ie. a list of list of list etc) is limited to 127.

To give you an idea of the kinds of things you can do you could try looking at these unit test cases

*   [NestedMapMessageTest](https://github.com/apache/activemq/blob/main/activemq-unit-tests/src/test/java/org/apache/activemq/test/message/NestedMapMessageTest.java)
*   [NestedMapAndListPropertyTest](https://github.com/apache/activemq/blob/main/activemq-unit-tests/src/test/java/org/apache/activemq/test/message/NestedMapAndListPropertyTest.java)

