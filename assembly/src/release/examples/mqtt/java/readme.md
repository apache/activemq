## Overview

This is an example of how use the MQTT protocol with ActiveMQ.  

## Prereqs

- Install Java SDK
- Install [Maven](http://maven.apache.org/download.html) 

## Building

Run:

    mvn install

## Running the Examples

In one terminal window run:

    java -cp target/mqtt-example-0.1-SNAPSHOT.jar example.Listener

In another terminal window run:

    java -cp target/mqtt-example-0.1-SNAPSHOT.jar example.Publisher

You can control to which stomp server the examples try to connect to by
setting the following environment variables: 

* `MQTT_HOST`
* `MQTT_PORT`
* `MQTT_USER`
* `MQTT_PASSWORD`
