## Overview

This is an example of how to use the ActiveMQ 5.x / OpenWire protocol to communicate with ActiveMQ

This example does basic publish-subscribe messaging using Topics

## Prereqs

- Install Java SDK
- Install [Maven](http://maven.apache.org/download.html) 

## Building

Run:

    mvn install

## Running the Examples

In one terminal window run:

    mvn -Psubscriber

In another terminal window run:

    mvn -Ppublisher
