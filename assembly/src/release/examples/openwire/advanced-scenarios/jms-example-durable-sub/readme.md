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

    mvn -Psubscriber -DclientId=<client_id_goes_here>

In another terminal window run:

    mvn -Ppublisher

You can kill the subscriber at any point and it will not miss messages because the subscription it creates
is durable
