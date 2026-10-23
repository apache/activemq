---
title: "Pure Master Slave"
slug: /pure-master-slave
---

> **Warning**
> 
> The LevelDB store has been removed from production.
> This page only serves as an archival page for previous releases.
> Use [shared storage master/slave](./shared-file-system-master-slave.md) instead.


### Pure Master Slave

> This feature has been deprecated and will be removed in version 5.8
> 
> This feature will be removed in 5.8 as it has not evolved to be production ready. You are advised to use [shared storage master/slave](./shared-file-system-master-slave.md) or the [Replicated LevelDB Store](replicated-leveldb-store). See [AMQ-4165](https://issues.apache.org/jira/browse/AMQ-4165)

A Pure Master Slave configuration provides a basic shared nothing, fully replicated topology which does not depend on a shared file system or shared database.

### How Pure Master Slave works

*   A slave of a master broker consumes all message states from the master - messages, acknowledgments and transactional states.  
    Whilst a Slave is actively connected to the Master - it does not allow or start any network or transport connectors, it's sole purpose is to duplicate the state of the master.

*   The master broker will only respond to a client when a message exchange has been successfully passed to the slave. For example, a commit  
    in a clients transaction will not complete until the master and the slave have processed the commit.

*   In the event the master fails (e.g. hardware failure) the slave has optionally two modes of operation
    1.  starts all it's network and transport connectors - allowing clients connected to the master to resume on the slave.
    2.  or can be configured to close down. In this mode, the slave is simply used to duplicate state for the master.

*   clients should use a failover transport for connecting to the master broker first and then the slave. e.g. using a URL such as
```
failover://(tcp://masterhost:61616,tcp://slavehost:61616)?randomize=false
```
The **randomize** property just disables randomness so that the transport will always try the master first, then the slave if it can't connect to that. Note that the slave does not accept connections until it becomes the master

### Limitations of Pure Master Slave

*   Only one slave can be connected to the Master
*   A failed master cannot be re-introduced without shutting down the the slave broker (no automatic failback)
*   There is no automatic synchronization between brokers. This is a manual process.

### Recovering a Pure Master Slave topology

This is a manual process - once a master has failed, the only sure way to ensure that the toplogy is synchronized again is manually:

*   shutdown the slave broker (The clients do not need to be shutdown - they will wait until the topology is re-established if they are failover clients
*   copy the data directory from the slave over the data directory of the master broker
*   re-start the master and the slave

### Configuring Pure Master Slave

You should not configure a connection between the master and a slave. The connection is automatically established with the slave's configuration. If you explicitly configure a network connection, you may encounter race conditions when the master broker is under heavy load.

A master broker doesn't need any special configuration - it's a normal broker until a slave broker attaches itself.  
To identify a broker as a slave - there is just one property to set (see below) as this [example shows](http://svn.apache.org/repos/asf/activemq/trunk/activemq-core/src/test/resources/org/apache/activemq/broker/ft/slave2.xml) - so configuration is nice and easy:
```
<broker masterConnectorURI="tcp://masterhost:62001" shutdownOnMasterFailure="false">
  <persistenceAdapter>
      <journaledJDBC journalLogFiles="5" dataDirectory="${activemq.base}/data/broker2" />
    </persistenceAdapter>

    <transportConnectors>
	  <transportConnector uri="tcp://slavehost:61616"/>
   </transportConnectors>
</broker>
```
Broker Property|default|Description
---|---|---
masterConnectorURI|null|URI to the master broker e.g. `tcp://masterhost:62001`
shutdownOnMasterFailure|false|if true, the slave will shut down if the master fails otherwise the slave will take over as being the new master. The slave ensures that there is a separate copy of each message and acknowledgement on another machine which can protect against catastrophic hardware failure. If the master fails you might want the slave to shut down as well as you may always want to duplicate messages to 2 physical locations to prevent message loss on catastrophic data centre or hardware failure. If you would rather the system keep on running after a master failure then leave this flag as false.
waitForSlave|false|version 5.2+, if true, a master will wait till a slave has attached before completing its startup sequence
shutdownOnSlaveFailure|false|version 5.2+, if true, a master will shutdown if the slave connection is lost, ensuring that the master will not become out of sync with the slave.

### Configuring the authentication of the Slave

In ActiveMQ 4.1 or later you can use a `<masterConnector/>` element as an alternative XML configuration mechanism as shown in the following [example](http://svn.apache.org/repos/asf/activemq/trunk/activemq-core/src/test/resources/org/apache/activemq/broker/ft/slave2.xml) to configure the user and password that the slave will use to connect to the master
```
<broker brokerName="slave" useJmx="false"  deleteAllMessagesOnStartup="true"  xmlns="http://activemq.apache.org/schema/core">
  <services>
    <masterConnector remoteURI= "tcp://localhost:62001" userName="James" password="Cheese"/>
  </services>

  <transportConnectors>
    <transportConnector uri="tcp://localhost:62002"/>
  </transportConnectors>
</broker>
```
