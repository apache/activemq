## Overview

This is an example of how to use the ActiveMQ 5.x / OpenWire protocol to communicate with ActiveMQ

This example demos using JMS transactions

## Prereqs

- Install Java SDK
- Install [Maven](http://maven.apache.org/download.html) 

## Building

Run:

    mvn install

## Running the Examples

    mvn -Pclient

    You will be greeted with a prompt. You can type a message and press enter. Nothing will actually be sent to
    a consumer until you type `COMMIT` and press enter. After doing so, you should see the built-in consumer
    consume the message and output it. If you type `ROLLBACK`, your message will be rolledback and the consumer
    will not have an opportunity to see it. Hit ^C to exit
