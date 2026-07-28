=======================================================================
The AcitveMQ Protocol Buffers Java Implementation
=======================================================================

Protocol Buffers is a data interchange format developed by 
Google.  You can get more information about Protocol Buffers
at:

 http://code.google.com/apis/protocolbuffers/


Unfortunately the the main Protocol Buffer's project made the 
Java API cumbersome to use since the messages are immutable.  They
Justify this decision by highlighting the fact it reduces end user
error that occur with Mutable messages.

This module brings you a slimmed down lean and mean API to accessing
Protocol Buffer data structures.  It provides little protection
from end users 'hurting themselves', but it does give power user
and easier to use API.

In addition, this module provides a Java based code generator so 
that it's easier to code generate your Protocol Buffer classes in 
a java based build system like Ant or Maven.

