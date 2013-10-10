## Overview

This is an example of how to use the ActiveMQ 5.x / OpenWire protocol to communicate with ActiveMQ

This example demos how to browse a queue without consuming from it

## Prereqs

- Install Java SDK
- Install [Maven](http://maven.apache.org/download.html) 

## Building

Run:

    mvn install

## Running the Examples

Run these steps in order:

1. In one terminal window run:

    mvn -Pproducer

2. In another terminal window run:

    mvn -Pbrowser