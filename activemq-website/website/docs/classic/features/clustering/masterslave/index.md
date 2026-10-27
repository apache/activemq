---
title: "MasterSlave"
slug: /masterslave
---

Introduction to Master / Slave
------------------------------

The following are the different kinds of Master/Slave configurations available:

Master Slave Type|Requirements|Pros|Cons
---|---|---|---
[Shared File System Master Slave](shared-file-system-master-slave)|A shared file system such as a SAN|Run as many slaves as required. Automatic recovery of old masters|Requires shared file system
[JDBC Master Slave](jdbc-master-slave)|A Shared database|Run as many slaves as required. Automatic recovery of old masters|Requires a shared database. Also relatively slow as it cannot use the high performance journal

If you are using a shared network file system such as a SAN we recommend a [Shared File System Master Slave](shared-file-system-master-slave). If you are happy to dispense with the high performance journal and are using pure JDBC as your persistence engine then you should use [JDBC Master Slave](jdbc-master-slave) instead.

