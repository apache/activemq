---
title: "The Broker will not start"
slug: /the-broker-will-not-start
---

It's been reported that during broker start some users get a message similar to:

15:26:29 INFO  Opening journal. Caught: javax.jms.JMSException: Failed to open transaction journal: java.io.IOException: Invalid argument

It could be [this problem](journal-is-already-opened-by-this-application) or the problem could be due to a bad os/jvm combination. See [Known Bad OS and JVM Combinations](known-bad-os-and-jvm-combinations)

