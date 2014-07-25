## Overview

This is an example of how to use the ActiveMQ 5.x / OpenWire protocol to communicate with ActiveMQ

This example demos using wildcards for consuming messages from hierarchies of destinations

## Prereqs

- Install Java SDK
- Install [Maven](http://maven.apache.org/download.html) 

## Building

Run:

    mvn install

## Running the Examples

    Run one consumer specifying which topicName to produce to (topicName) and what sort of wildcard
    you'd like to use to listen to that same topic hierarchy (wildcard)

    For example, to publish to "hello.world" topic and listen to "hello.world.*":

    mvn -Pconsumer -DtopicName="hello.world" -Dwildcard=".*"

    You start another consumer that listens to:

    mvn -Pconsumer -DtopicName="hello.world.hungray" -Dwildcard=".*"

    And when you type a message on this prompt, you should see it in the first consumer you started
