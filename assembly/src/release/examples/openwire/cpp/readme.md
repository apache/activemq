Prereqs
=======

These examples use the [ActiveMQ-CPP](http://activemq.apache.org/cms) C++ library,


1. [Download the latest release from the ActiveMQ-CPP website)
2. [Build and Install](http://activemq.apache.org/cms/building.html)

Building
========

This will vary depending on where you installed your libraries and the compiler
you are using but on my Ubuntu system, I compiled the examples as follows:

    gcc Listener.cpp -o listener -I/usr/local/include/activemq-cpp-3.8.4 -I/usr/include/apr-1.0 -lactivemq-cpp -lstdc++
    gcc Publisher.cpp -o publisher -I/usr/local/include/activemq-cpp-3.8.4 -I/usr/include/apr-1.0 -lactivemq-cpp -lstdc++

Running the Examples
====================

Note: You may need to update set an environment variable so that the
activemq-cpp shared libraries can be loaded.  For example on my Ubuntu
system I had to add the following to my profile:

    export LD_LIBRARY_PATH=/usr/local/lib

In one terminal window run:

    ./listener

In another terminal window run:

    ./publisher

You can control to which stomp server the examples try to connect to by
setting the following environment variables:

* `ACTIVEMQ_HOST`
* `ACTIVEMQ_PORT`
* `ACTIVEMQ_USER`
* `ACTIVEMQ_PASSWORD`
