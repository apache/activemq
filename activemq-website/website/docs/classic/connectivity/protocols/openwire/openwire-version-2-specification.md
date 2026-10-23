---
title: "OpenWire Version 2 Specification"
slug: /openwire-version-2-specification
---


> **OpenWire Version 2 is not the latest version.**
> 
> This article references OpenWire V2 which is no longer the latest version. The formatting and encoding rules in this article are still valid for later OpenWire versions, but later versions define additional fields in the OpenWire commands. The default settings shown in this article may not reflect the current defaults, see [this page](configuring-wire-formats) for the latest default OpenWire settings.

Overview
--------

OpenWire is used to marshal objects to byte arrays and back. We will refer to the marshaled objects as commands.

A TCP network connection would see multiple commands back to back on the stream. Commands are not delimited in anyway and are variable sized.
```
+---------+  +---------+      +---------+
| command |  | command | .... | command |
+---------+  +---------+      +---------+
```
All data primitive types used in the encoded commands are encoded in big-endian/network byte order.

primitive types and encoding:
```
             |               |               |               |               |               |               
+----------+ | +-----------+ | +-----------+ | +-----------+ | +-----------+ | +-----------+ | +-----------+ 
| byte     | | | char      | | | short     | | | int       | | | long      | | | float     | | | double    | 
+----------+ | +-----------+ | +-----------+ | +-----------+ | +-----------+ | +-----------+ | +-----------+ 
| 1 octect | | | 2 octects | | | 2 octects | | | 4 octects | | | 8 octects | | | 4 octects | | | 8 octects | 
+----------+ | +-----------+ | +-----------+ | +-----------+ | +-----------+ | +-----------+ | +-----------+ 
             |               |               |               |               |               |               
```

Wire Format Negotiation
-----------------------

OpenWire is an extensible in that it supports adding new encoding options while still being backward compatible with previous versions of the protocol. Every OpenWire protocol session initially starts with all encoding options turned off and at version 1 of command marshalers. An initial WIREFORMAT_INFO command is exchanged between the two nodes so that additional encoding features can be enabled. If both sides request an encoding feature to be enabled then it will be enabled. The command marshalers used will be the highest version that they both support.
```
+-------------------------+ +---------+  +---------+      +---------+
| WIREFORMAT_INFO command | | command |  | command | .... | command |
+-------------------------+ +---------+  +---------+      +---------+
```

Command Encoding
----------------

Every command is encoded as follows:

command encoding:
```
[=If SizePrefixDisabled =] 
[ option is not enabled. ]
[ +------+               ] +------+-------------------------+
[ | size |               ] | type | command-specific-fields |
[ +------+               ] +------+-------------------------+
[ | int  |               ] | byte | (size-1) octects        |
[ +------+               ] +------+-------------------------+
[========================]
```

Field|Description
---|---
size|Holds how many subsequent bytes are in the the command
type|Command type identifier
command-specific-fields|The data for the command. It is encoding depends on the type used

If a command type does not have any command specific content, then size would be 1.

Prefixing the size on every command is done to aid in non-blocking IO processing so that receiver can easily determine when a whole command has been received and can be processed. But finding out the size of command takes additional buffering on the sender size and is not needed if the receiver is doing blocking IO. If the SizePrefixDisabled option is enabled after the exchange of WIREFORMAT_INFO packets, then every subsequent command will not have the size prefixed.

Command Types
-------------

Type Identifier|Command
---|---
1|WIREFORMAT_INFO
2|BROKER_INFO
3|CONNECTION_INFO
4|SESSION_INFO
5|CONSUMER_INFO
6|PRODUCER_INFO
7|TRANSACTION_INFO
8|DESTINATION_INFO
9|REMOVE_SUBSCRIPTION_INFO
10|KEEP_ALIVE_INFO
11|SHUTDOWN_INFO
12|REMOVE_INFO
14|CONTROL_COMMAND
15|FLUSH_COMMAND
16|CONNECTION_ERROR
17|CONSUMER_CONTROL
18|CONNECTION_CONTROL
21|MESSAGE_DISPATCH
22|MESSAGE_ACK
23|ACTIVEMQ_MESSAGE
24|ACTIVEMQ_BYTES_MESSAGE
25|ACTIVEMQ_MAP_MESSAGE
26|ACTIVEMQ_OBJECT_MESSAGE
27|ACTIVEMQ_STREAM_MESSAGE
28|ACTIVEMQ_TEXT_MESSAGE
30|RESPONSE
31|EXCEPTION_RESPONSE
32|DATA_RESPONSE
33|DATA_ARRAY_RESPONSE
34|INTEGER_RESPONSE
40|DISCOVERY_EVENT
50|JOURNAL_ACK
52|JOURNAL_REMOVE
53|JOURNAL_TRACE
54|JOURNAL_TRANSACTION
55|DURABLE_SUBSCRIPTION_INFO
60|PARTIAL_COMMAND
61|PARTIAL_LAST_COMMAND
65|REPLAY
70|BYTE_TYPE
71|CHAR_TYPE
72|SHORT_TYPE
73|INTEGER_TYPE
74|LONG_TYPE
75|DOUBLE_TYPE
76|FLOAT_TYPE
77|STRING_TYPE
78|BOOLEAN_TYPE
79|BYTE_ARRAY_TYPE
90|MESSAGE_DISPATCH_NOTIFICATION
91|NETWORK_BRIDGE_FILTER
100|ACTIVEMQ_QUEUE
101|ACTIVEMQ_TOPIC
102|ACTIVEMQ_TEMP_QUEUE
103|ACTIVEMQ_TEMP_TOPIC
110|MESSAGE_ID
111|ACTIVEMQ_LOCAL_TRANSACTION_ID
112|ACTIVEMQ_XA_TRANSACTION_ID
120|CONNECTION_ID
121|SESSION_ID
122|CONSUMER_ID
123|PRODUCER_ID
124|BROKER_ID

### Command Field Encoding

All OpenWire commands use the same algorithm when encoding their fields. The algorithm restricts commands to use fields that are of the following type:

*   Java primitive types
*   String
*   Byte Arrays
*   N Sized Byte Arrays
*   Throwable
*   Nested OpenWire commands
*   Nested OpenWire command arrays
*   Cached Nested OpenWire commands

Notice that OpenWire commands can nest other OpenWire commands in it's fields. Care must be taken that only acyclic graphs of commands are marshaled.

#### String Type Encoding

Strings fields may be null. If it is null then it encodes to a single "0" byte.

string encoding:
```
             [=If not-null is 1===========]
+----------+ [ +-------+----------------+ ]
| not-null | [ | size  | encoded-string | ]
+----------+ [ +-------+----------------+ ]
| byte     | [ | short | size octects   | ]
+----------+ [ +-------+----------------+ ]
             [============================]
```

Field|Description
---|---
not-null|1 if the string is not null, 0 if the string is null
size|The number of bytes of the UTF-8 Encoded string
encoded-string|The UTF-8 encoded form of the string

### Byte Array Type Encoding

Byte array fields may be null. If it is null then it encodes to a single "0" byte.

byte-array encoding:
```
             [=If not-null is 1========]
+----------+ [ +------+--------------+ ]
| not-null | [ | size | data         | ]
+----------+ [ +------+--------------+ ]
| byte     | [ | int  | size octects | ]
+----------+ [ +------+--------------+ ]
             [=========================]
```
Field|Description
---|---
not-null|1 if the byte array is not null, 0 if it is null
size|The number of bytes in the byte array
data|The data of the byte array

### N Sized Byte Array Type Encoding

Fixed Size Byte array fields may NOT be null and their length must be N length. Used for byte arrays that are always a fixed size.

N-sized-byte-array encoding:
```
+-----------+
| data      |
+-----------+
| N octects |
+-----------+
```
Field|Description
---|---
data|The data of the byte array

### Throwable Type Encoding

Throwable fields may be null. If it is null then it encodes to a single "0" byte.

throwable encoding:
```
             [=If not-null is 1===========================================================================]
             [                               [=If StackTraceEnabled option is enabled.==================] ]
             [                               [             [=Repeated size times======================] ] ]
+----------+ [ +----------------+---------+  [ +-------+ [ +--------+--------+--------+-------------+ ] ] ]
| not-null | [ | exception-name | message |  [ | size  | [ | class  | method | file   | line-number | ] ] ]
+----------+ [ +----------------+---------+  [ +-------+ [ +--------+--------+--------+-------------+ ] ] ]
| byte     | [ | string         | string  |  [ | short | [ | string | string | string | int         | ] ] ]
+----------+ [ +----------------+---------+  [ +-------+ [ +--------+--------+--------+-------------+ ] ] ]
             [                               [           [============================================] ] ]
             [                               [==========================================================] ]
             [============================================================================================]
```

Field|Description
---|---
not-null|1 if the Throwable not null, 0 if the Throwable is null
exception-name|The class name of the exception
message|The exception message
size|The number of stack frames in the stack trace
class|The name of the class in a stack frame
method|The name of the method in a stack frame
file|The name of the file in a stack frame
line-number|The line number in a stack frame

If the StackTraceEnabled encoding option is enabled after the exchange of WIREFORMAT_INFO packets, then every Throwable filed will have stack trace data appended to it.

### Nested Command Type Encoding

Nested command fields may be null. If it is null then it encodes to a single "0" byte.

nested-object encoding:
```
             [=If not-null is 1===================]
+----------+ [ +------+-------------------------+ ]
| not-null | [ | type | command-specific-fields | ]
+----------+ [ +------+-------------------------+ ]
| byte     | [ | byte | variable sized          | ]
+----------+ [ +------+-------------------------+ ]
             [====================================]
```

Field|Description
---|---
not-null|1 if the Throwable not null, 0 if the Throwable is null
type|Command type identifier
command-specific-fields|The data for the command. It is encoding depends on the type used

### Cached Nested Command Type Encoding

Nested Command types can be cached so that subsequent marshaling operations of the same object result in a smaller on the wire size. By default the CacheEnabled option is not enabled and therefore standard nested-object encoding is used.

cached-object-encoding:
```
[=If CacheEnabled option is enabled=====================]
[                       [=If new-value is 1===========] ]
[ +-----------+-------+ [ +-------------------------+ ] ]
[ | new-value | key   | [ | command-specific-fields | ] ]
[ +-----------+-------+ [ +-------------------------+ ] ]
[ | byte      | short | [ | nested-object           | ] ]
[ +-----------+-------+ [ +-------------------------+ ] ]
[                       [=============================] ]
[=====================================================] ]
```
```
[=If CacheEnabled option is disabled =]
[ +-------------------------+         ]
[ | command-specific-fields |         ]
[ +-------------------------+         ]
[ | nested-object           |         ]
[ +-------------------------+         ]
[=====================================]
```

Field|Description
---|---
new-value|1 if the value is not in the cache, 0 if it already is in the cache
key|Key used to identify the value in the cache
command-specific-fields|The data for the command. It is encoding depends on the type used

Loose Encoding
--------------

The default encoding explained so far is called "loose encoding" and is the default encoding used when OpenWire if first initialized. Loose encoding is simple to implement does not add much CPU overhead to the marshaling/unmarshaling process. It is able to marshal an object graph in single pass of the object tree.

Tight Encoding
--------------

Another encoding option is supported by OpenWire which is called TightEncoding. When tight encoding is used, it uses bit streams to marshal all the boolean values that would have taken up a byte in loose encoding to a bit in the bit stream. To build the bit stream 2 passes through the object graph are taken so this is more CPU intensive marshaling process but it produces a smaller on the wire size.

