## Overview

This is an example of how to use the ActiveMQ 5.x / OpenWire protocol to communicate with ActiveMQ

This example shows how to do request response with temporary destinations

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

You'll notice that the producer received a reply for the request it sent.
