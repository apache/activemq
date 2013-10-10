Prereqs
=======

- Install [Apache.NMS.ActiveMQ](http://activemq.apache.org/nms/download.html) 

Building
========

This will vary depending on where you installed your libraries.  Open the 
ActiveMQExamples solution in Visual Studio and update the references for the
Listener and Publisher project to point to where you Apache.NMS.dll,  
Apache.NMS.ActiveMQ.dll and Ionic.Zlib.dll are located.  

Build both projects in the solution.

Running the Examples
====================

In one terminal window run:

    ./Listener.exe

In another terminal window run:

    ./Publisher.exe

You can control to which ActiveMQ server the examples try to connect to by
setting the following environment variables: 

* `ACTIVEMQ_HOST`
* `ACTIVEMQ_PORT`
* `ACTIVEMQ_USER`
* `ACTIVEMQ_PASSWORD`
