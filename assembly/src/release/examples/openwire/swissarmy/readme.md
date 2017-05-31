# Overview

** These examples are deprecated and will be removed in the future versions. Please refer to (http://activemq.apache.org/examples.html) for more information on the replacement. **

Before running the examples you should start ActiveMQ on your machine. Follow the 
installation instructions to use a binary distribution of ActiveMQ. To run the broker 
in a command shell, type: 

    bin/activemq

This starts up the ActiveMQ broker. 

h3. Running the examples from a binary distro

You can use [Ant](http://ant.apache.org) to compile and run the examples. To run a 
message producer run: 

    ant producer

To run a consumer, in another shell run: 

    ant consumer

You should then see messages being produced and consumed. You can also pass additional 
commands into these goals using variables that are available in the build.xml. Below 
is an example: 

    ant consumer -Durl=tcp://localhost:61616 -Dtopic=true -Ddurable=true -Dmax=5000
    ant producer -Durl=tcp://localhost:61616 -Dtopic=true -DtimeToLive=30000 -Dmax=5000

For a summary of all the available goals and options try: 

    ant help

