---
title: "JMeter System Tests"
slug: /jmeter-system-tests
---


### JMeter System Tests

You can use JMeter to test your ActiveMQ Server. Please refer to the JMeter site for detailed instructions on using this software.  

The Test would check for duplicate messages and the order of messages received.

### Building a Test Plan

For a Producer System Test Sampler  
1. Add a Thread Group.  
2. Add the producer sampler "Producer System Test Sampler" from the Sampler menu.  
3. Go to the Thread Group element and set the Number of Threads, Ramp-Up Period and Loop Count to 1.

For a Consumer Sampler  
1. Add the consumer sampler "Consumer System Test Sampler" from the Sampler menu.  
2. Highlight te Consumer Sampler then right click to add the consumer listener "View Producer System Test Results" from the Listener menu.

### The JMeter System Test Module is consist of:

1. JMeter Producer System Test Sampler

    *   A JMeter Sampler tells JMeter to send requests to the server. Pluggable Samplers allow unlimited testing capabilities.
    *   The Producer System Test Sampler will send messages to an ActiveMQ Server.
    
    The Producer System Test Sampler has the following parameters:
    
    *   Server URL - defines the server url i.e. tcp://localhost:61616
    *   No. of Producer - The number of Producer that would be sending the message.
    *   No. of Subject - The number of Subject that would be created then send. Note that the number of Subject should be less than or equal to the No. of Producer.
    *   Number of Message - The number of the messages to send.
    *   Delivery Mode - Default is Non-Persistent.
    *   Messaging Domain - Default is Topic

2. JMeter Consumer System Test Sampler

    The Consumer Sampler has the following parameters:
    
    *   Server URL - defines the server url i.e. tcp://localhost:61616
    *   No. of Consumer - The number of Consumer that would be sending the message.
    *   No. of Subject - The number of Subject that would be created then send. Note that the number of Subject should be less than or equal to the No. of Consumer.
    *   Delivery Mode - Default is non-Durable.
    *   Messaging Domain - Default is Topic.

3. JMeter Producer System Test Listener

    *   Listeners provide access to the information JMeter gathers about the test cases while JMeter runs.
    *   The Producer System Test Listener would provide the Consumer Identifier, Producer Name, Message and Message Count.    
    *   Consumer Identifier - Identifies the Consumer that received the message.
    *   Producer Name - Producer name that send the message.
    *   Message - The message body received.
    *   Message Count - The number of messages received.

