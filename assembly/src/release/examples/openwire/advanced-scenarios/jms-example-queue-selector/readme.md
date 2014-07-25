## Overview

This is an example of how to use the ActiveMQ 5.x / OpenWire protocol to communicate with ActiveMQ

This example shows how to use selectors to filter out messages based on criteria

## Prereqs

- Install Java SDK
- Install [Maven](http://maven.apache.org/download.html) 

## Building

Run:

    mvn install

## Running the Examples

In one terminal window run:

    mvn -Pconsumer

In another terminal window run:

    mvn -Pproducer

In the consumer's terminal, you'll notice that not all of the messages that the producer sent were consumed.
Only those that had the property "intended" equal to "me"
