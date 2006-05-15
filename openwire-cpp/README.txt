Apache ActiveMQ C++ Client
==========================


The ActiveMQ C++ client has support for both synchrounous and asynchrounous messaging as well as local transactions and more. To ease programming it uses smart pointers and STL extensively. For more information see included test programs.


How to build
============
To run the supplied makefiles you need to set two environment variables, CONFIG and OSTYPE. With the help of variables the makefiles can determine what settings needs to be set for your platform.

CONFIG can be set to either "debug" or "release" depending on what type of output you want. OSTYPE is set to "linux" or "macosx".

For Windows, use the Visual Studio 2005 project files.


Connection URI
==============
To connect to the ActiveMQ broker a URI is specified. The URI may have a set of configuration parameters that are used to configure the client.

Sample URI: "tcp://192.168.64.142:61616?trace=false&protocol=openwire&encoding=none"

Scheme
------
Name......: tcp
Desciption: Type of transport protocol
Mandatory.: Yes

Parameters
----------
Name......: protocol
Desciption: Type of wire protocol
Default...: "openwire"
Values....: "openwire"
Mandatory.: No

Name......: encoding
Desciption: Character encoding
Default...: "AsciiToUTF8"
Values....: "none", "AsciiToUTF8"
Mandatory.: No

Name......: trace
Desciption: Enables debug output to console
Default...: "false"
Values....: "true", "false"
Mandatory.: No

