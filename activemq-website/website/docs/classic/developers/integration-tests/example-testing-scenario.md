---
title: "Example Testing Scenario"
slug: /example-testing-scenario
---


ActiveMQ Performance Module
---------------------------

*   [Users Manual](activemq-classic-performance-module-users-manual)

Example Testing Scenario
------------------------

This page gives a simple example of the kinds of thing we wanna do.

Assuming that all the test code is within a single Maven POM for now (e.g. activemq-integration-test version 4.0) which will deal with all the classpath issues.

We'll try describe the different ways this could work and give each implementation style a name so we can start revving different ways to solve this...

### Headless build

In this version there is no 'controller'; each build is considered to be a totally separate build.

Each build knows what to do; each test case generates an XML file which becomes a named deployment artifact.

e.g. imagine the following builds (which are really just running Java executables within a POM for classpath)

Box|Description|Command line
---|---|---
hostB|Broker|java org.apache.activemq.broker.console.Main tcp://$hostA:61616
hostC|Consumer|java org.apache.activemq.test.ConsumerMain --message-count=5000 --queue=true --destination=org.foo.bar tcp://$hostA:61616
hostD|Producer|java org.apache.activemq.test.ProducerMain --message-count=5000 --queue=true --destination=org.foo.bar tcp://$hostA:61616

In the above example - each build has to kinda wait for other things to start up to some time period. e.g. the producer and consumer wanna keep around for say 5 minutes trying to connect to the broker as they can be started in any order.

Ideally we might wanna run this as 3 maven commands as follows...
```
mvn activemq:broker
mvn activemq:perf-producer -Dmessage-count=5000 -Dqueue=true -Ddestination=org.foo.bar -Durl=tcp://$hostA:61616
mvn activemq:perf-consumer -Dmessage-count=5000 -Dqueue=true -Ddestination=org.foo.bar -Durl=tcp://$hostA:61616
```
### Controller build

The idea with the controller version is one of the tests (which is spun off first to try help) tries to coordinate among the test nodes.

e.g. we could spin the controller first...

Box|Description|Command line
---|---|---
hostA|Controller|mvn test

Then the test case fires off these processes while communicating with them...

Box|Description|Command line
---|---|---
hostB|Broker|java org.apache.activemq.broker.console.Main tcp://$hostA:61616
hostC|Consumer|java org.apache.activemq.test.ConsumerMain --message-count=5000 --queue=true --destination=org.foo.bar tcp://$hostA:61616
hostD|Producer|java org.apache.activemq.test.ProducerMain --message-count=5000 --queue=true --destination=org.foo.bar tcp://$hostA:61616

### Controller factory build

Fairly soon we're gonna have tons of builds firing off. We may want a single project to build with a raft of different test suites. Each single distributed integration/system/performance test might have many sub-builds (processes) to run.

So we might want to run a single JUnit test case which fires off different remote builds/processes.

e.g.
```
public class PerformanceTestSuite {
   public void testSmallMessages() {
   	  buildQueue.start("broker", "");
   	  buildQueue.start("consumer", "--messageCount=1000");
   	  buildQueue.start("producer", "--messageCount=1000");
   	  buildQueue.join(5 * MINUTES);
   }

   public void testLargeMessages() {
   	  buildQueue.start("broker", "");
   	  buildQueue.start("consumer", "--messageCount=1000 --messageSize=1M");
   	  buildQueue.start("producer", "--messageCount=1000 --messageSize=1M");
   	  buildQueue.join(10 * MINUTES);
   }
}
```
So these 2 test cases in JUnit in the controller build will each start 3 separate remote builds on the queue and wait for them to complete - or terminate them

