Prereqs
=======

These examples use the [ActiveMQ-CPP](http://activemq.apache.org/cms) C++ library,
but unfortunately they don't work with the latest 3.2.4 release, you have to either
wait for a subsequent release of do a source build of the project trunk.  Until then,
you will need to 

1. [Checkout the ActiveMQ-CPP trunk source code](http://activemq.apache.org/cms/source.html)
2. [Build and Install](http://activemq.apache.org/cms/building.html)

Building
========

This will vary depending on where you installed your libraries and the compiler 
you are using but on my Ubuntu system, I compiled the examples as follows:

    gcc Listener.cpp -o listener -I/usr/local/include/activemq-cpp-3.2.4 -I/usr/include/apr-1.0 -lactivemq-cpp -lstdc++ 
    gcc Publisher.cpp -o publisher -I/usr/local/include/activemq-cpp-3.2.4 -I/usr/include/apr-1.0 -lactivemq-cpp -lstdc++ 

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
