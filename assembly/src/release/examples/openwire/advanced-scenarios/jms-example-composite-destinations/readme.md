## Overview

This is an example of how to use the ActiveMQ 5.x / OpenWire protocol to communicate with ActiveMQ

This example demonstrates how to use composite destinations for both sending and receiving.

## Prereqs

- Install Java SDK
- Install [Maven](http://maven.apache.org/download.html) 

## Building

Run:

    mvn install

## Running the Examples

In one terminal window run:

    mvn -P consumer

In another terminal window run:

    mvn -P producer

## What to look for
In the console of the producer, you should see log messages indicating messages have been successfully sent
to the broker.

In the console of the consumer, you should see log messages indicating messages have been received on each individual
destination:

* test-queue
* test-queue-foo
* test-queue-bar
* test-topic-foo
