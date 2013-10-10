## Overview

This is an example of how to use the ActiveMQ 5.x / OpenWire protocol to communicate with ActiveMQ

This example demos using exclusive consumers (one single consumer)

## Prereqs

- Install Java SDK
- Install [Maven](http://maven.apache.org/download.html) 

## Building

Run:

    mvn install

## Running the Examples

You will want to run **multiple** instances of a consumer. To run the consumer open a
terminal and type

    mvn -Pconsumer

In another terminal window run:

    mvn -Pproducer

You'll notice that even though you have multiple consumers, only one consumer will be consuming messages. If you
kill the consumer that's currently receiving messages, one of the other consumers will pick up the consumption.
