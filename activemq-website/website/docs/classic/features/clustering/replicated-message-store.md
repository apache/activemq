---
title: "Replicated Message Store"
slug: /replicated-message-store
---


If messages are stored on the hard drive of a broker or inside a single database; then you have a single point of failure with respect to the message persistence. If you lose the entire machine, disk or database, you have lost messages.

For some high end users, messages should never be lost as they could have a large impact on the business. These kinds of users often require some kind of [DR](dr) stategy to support message replication so that they can loose an entire data centre and still not loose a message.

There are various solutions to the problem of reducing chance of message loss.

### Use RAID disks

If you're using RAID disks striped sufficiently, you could just restart the machine - or move the disk to a new machine and restart the broker. This could be sufficient if you're a small business but if you've stringent [DR](dr) requirements then if you loose a datacentre the RAID option is not a solution.

### SAN or shared network drive

If you use one of the file based persistence mechanisms such as the default high performance journal and Apache Derby, you could write to a SAN or shared network drive and in times of failure, start up a new broker using the files from the failed broker.

Also 4.1 allows you to start many brokers reading from the same shared file system to support high availability via the [Shared File System Master Slave](shared-file-system-master-slave) feature.

### Master/Slave

An alternative is to use the [MasterSlave](masterslave) feature to pair brokers together so that all messages are replicated to both brokers, the master and slave to ensure that there are two physical copies of the message so that catastrophic hardware failures (such as loss of an entire data centre) can be handled.

### Clustered JDBC databases

Various databases like Oracle and MySQL support clustered databases; so we can use these databases with the JDBC MessageStore to get a clustered message store. Note if this option is used then the high performance journal must be disabled (which severely affects performance)

#### Use C-JDBC

If you don't have or can't afford a clustered database then you could use [C-JDBC](http://c-jdbc.objectweb.org/) to replicate state across a number of physical datbases to avoid single point of failure and provide a [DR](dr) solution. As noted above using a replicated JDBC approach is very slow as it requires us to disable the high performance journal.

