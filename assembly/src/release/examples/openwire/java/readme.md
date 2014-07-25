## Overview

This is an example of how use the Java JMS api with ActiveMQ.

## Prereqs

- Install Java SDK
- Install [Maven](http://maven.apache.org/download.html) 

## Building

Run:

    mvn install

## Running the Examples

In one terminal window run:

    java -cp target/openwire-example-0.1-SNAPSHOT.jar example.Listener

In another terminal window run:

    java -cp target/openwire-example-0.1-SNAPSHOT.jar example.Publisher

You can control to which server the examples try to connect to by
setting the following environment variables: 

* `ACTIVEMQ_HOST`
* `ACTIVEMQ_PORT`
* `ACTIVEMQ_USER`
* `ACTIVEMQ_PASSWORD`
