---
title: "JMeter Performance Tests"
slug: /jmeter-performance-tests
---


### JMeter performance test

You can use JMeter to test the performance of your ActiveMQ Server. Please refer to the [JMeter site](http://jakarta.apache.org/jmeter/) for detailed instructions on using this software.

### Downloading the JMeter Performance Test Binary

You can download the latest activemq-jmeter-*.zip distribution from the following

*   [Apache ActiveMQ versions](http://people.apache.org/repository/incubator-activemq/distributions/)
*   [3.x versions of ActiveMQ](http://dist.codehaus.org/activemq/distributions/).

### Building the JMeter Performance Test from source

1. Check out the latest head version of ActiveMQ from [Source](source). Build from source.
2. Run maven from the JMeter directory. This will build JMeter into the target directory.
3. Run JMeter.bat from the \\bin directory to start JMeter.
4. There are sample Test Plans located at \\bin\\testfiles directory that you could open to test ActiveMQ.

### Building a Test Plan

For a Producer Sampler  
1. Add a Thread Group.  
2. Add the producer sampler "Producer Sampler" from the Sampler menu.  
3. Highlight the Producer Sampler then right click to add the producer listener "View Producer Results" from the Listener menu.  
4. Go to the Thread Group element and set the Number of Threads, Ramp-Up Period and Loop Count to 1.

For a Consumer Sampler  
1. Add a Thread Group.  
2. Add the consumer sampler "Consumer Sampler" from the Sampler menu.  
3. Highlight the Consumer Sampler then right click to add the consumer listener "View Consumer Results" from the Listener menu.  
4. Go to the Thread Group element and set the Number of Threads, Ramp-Up Period and Loop Count to 1.

### The JMeter Performance Test Module consists of:

1. JMeter Producer Sampler

    *   A JMeter Sampler tells JMeter to send requests to the server. Pluggable Samplers allow unlimited testing capabilities.
    *   The Producer Sampler will send messages to an ActiveMQ Server.
    
    The Producer Sampler has the following parameters:
    
    *   Server URL - defines the server url i.e. tcp://localhost:61616
    *   Duration (min) - the duration of the test.
    *   Ramp Up (min) - the time in which the Producer will reach it's stable/peak state.
    *   No. of Producer - The number of Producer that would be sending the message.
    *   No. of Subject - The number of Subject that would be created then send. Note that the number of Subject should be less than or equal to the No. of Producer.
    *   Message Size (bytes) - The size of the message to send.
    *   Delivery Mode - Default is Non-Persistent.
    *   Messaging Domain - Default is Topic
    *   Message Interval - Default/Custom Interval. When Custom Interval was Selected user must provide Interval Value (mins).
    *   Message Queue Servers - List of available MQ Servers.

2. JMeter Producer Listener

    *   Listeners provide access to the information JMeter gathers about the test cases while JMeter runs.
    *   The Producer Listener would provide statistics with the number of messages sent and average messages per second.

3. JMeter Consumer Sampler

    The Consumer Sampler has the following parameters:
    
    *   Server URL - defines the server url i.e. tcp://localhost:61616
    *   Duration (min) - the duration of the test.
    *   Ramp Up (min) - the time in which the Producer will reach it's stable/peak state.
    *   No. of Consumer - The number of Consumer that would be sending the message.
    *   No. of Subject - The number of Subject that would be created then send. Note that the number of Subject should be less than or equal to the No. of Consumer.
    *   Delivery Mode - Default is non-Durable.
    *   Messaging Domain - Default is Topic.
    *   Message Queue Servers - List of available MQ Servers.

4. JMeter Consumer Listener

    *   The Consumer Listener would provide statistics with the number of messages received and average messages per second.

