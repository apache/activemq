---
title: "Integration Tests"
slug: /integration-tests
---


The Integration Tests validate that ActiveMQ Resource Adapter operate correctly when deployed with a J2EE application running in an Application Server such as Apache Geronimo.

Getting Ready
-------------

The instructions on this page reference a few directories that you will need to substitute appropriate values for:

*   _activemq-core_ is where your ActiveMQ source code is located.
*   _geronimo-home_ is where your Geronimo home directory is located.

Building the ActiveMQ Resource Adapter
--------------------------------------

The ActiveMQ Resource Adapter allows a J2EE Application Server to provide transaction management, connection pooling and security to ActiveMQ connection when used by EJBs. The Resource Adapter is deployed in the client EAR that will be using the ActiveMQ.
```
cd activemq-ra
maven rar:install-snapshot
```

Building the Integration Test EJB module
----------------------------------------

The Integration Test EJB module provides a set of EJBs that the tests are run against.
```
cd systest/itests/ejb
maven
```

Building and Deploying the Integration Test EAR module
------------------------------------------------------

The EAR module packages up the Resource Adapter and the Integration Tests EJBs into a deployable EAR.
```
cd systest/itests/ear
maven -Dgeronimo.home=geronimo-home
```

Starting Geronimo with the Integration Test EAR configuration.
--------------------------------------------------------------

In a seperate console, issue the following commands
```
cd geronimo-home
java -jar bin/server.jar org/activemq/ear/itest
```

Building and running the Integration Tests.
-------------------------------------------

The following builds and runs the integration tests:
```
cd systest/itests/client
maven
```
