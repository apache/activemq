---
title: "Sybase"
description: "Configure an ActiveMQ Classic JDBC dataSource for Sybase ASE with jConnect or jTDS, and use a separate lockDataSource to stop the transaction log growing."
slug: /sybase
---


Sybase
------

A dataSource targetting a Sybase ASE database can be configured as follows:

`Sybase Datasource**

  <bean id="sybase-ds" class="org.apache.commons.dbcp.BasicDataSource" destroy-method="close">

    <!\-\- using jConnect -->
    <property name="driverClassName" value="com.sybase.jdbc4.jdbc.SybDriver"/>
    <property name="url" value="jdbc:sybase:Tds:localhost:4000/activemq"/>
    <!\-\- using JTDS -->
    <!\-\- <property name="driverClassName" value="net.sourceforge.jtds.jdbc.Driver"/> -->
    <!\-\- <property name="url" value="jdbc:jtds:sybase://localhost:4000/activemq"/> -->

    <property name="username" value="XXX" />
    <property name="password" value="YYY" />
    <!\-\- and so on with pool config -->
  </bean>

When working on a given Sybase ASE database, each operation (e.g. statement execution) is appended to the transaction log. This log grows over time, and space has to be freed in order to let the server keep running. ASE is able to free space up to the oldest opened transaction; that is, if you keep a transaction open for too long, the transaction log only grows - until the allocated space is full and the database goes down. So when using the Master-Slave feature of ActiveMQ with a database lock on table ACTIVEMQ_LOCK a synchronization monitor (see [http://activemq.apache.orgFeatures/Clustering/MasterSlaveFeatures/Clustering/MasterSlave/Features/Clustering/MasterSlave/jdbc-master-slave.md](http://activemq.apache.orgFeatures/Clustering/MasterSlaveFeatures/Clustering/MasterSlave/Features/Clustering/MasterSlave/jdbc-master-slave)), you have to be careful. If you use only one dataSource, hence only one database, then the lock mechanism shall keep a transaction indefinitely open, and while messages are persisted the transaction log shall grow until you run out of space. It is therefore strongly advised to use a dedicated database for this lock: just define another dataSource targeting a minimal database holding no data (only the ACTIVEMQ_LOCK table) and use it as a lockDataSource in your configuration.

Example:

<persistenceAdapter>
  <jdbcPersistenceAdapter dataSource="#sybase-ds" lockDataSource="#another-sybase-ds"/>
</persistenceAdapter>

