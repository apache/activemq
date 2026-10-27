---
title: "Pluggable storage lockers"
description: "Choose a storage locker for shared-storage ActiveMQ Classic brokers: Shared File Locker, Database Locker or Lease Database Locker with KahaDB or JDBC."
slug: /pluggable-storage-lockers
---

As of the 5.7.0 release of ActiveMQ the choice of storage locking mechanism, as used by a persistence adapter, has been made pluggable. This feature is only meaningful to brokers configured in a shared storage master/slave topology. Prior to release 5.7.0 the storage locking mechanism (and thus master election) was dictated by the choice of persistence adapter. With the KahaDB persistence adapter, for example, the storage locking mechanism was based on a shared file lock. Similarly, the JDBC persistence adapter used a database backed storage lock.

Now that the choice of storage locker is divorced from that of the persistence adapter one can mix and match combinations of the two. Storage locker pluggability is made possible by the [Locker](https://fisheye6.atlassian.com/browse/activemq/trunk/activemq-broker/src/main/java/org/apache/activemq/broker/Locker.java?hb=true) interface that all pluggable lockers must implement. This interface makes it easy to implement a custom storage locker that meets local requirements.

Every persistence adapter, however, has its own default locker which works as before.

Lockers
-------

Every locker must implement the [Locker](https://fisheye6.atlassian.com/browse/activemq/trunk/activemq-broker/src/main/java/org/apache/activemq/broker/Locker.java?hb=true) interface. The locker interface has the following properties:

Property Name|Default Value|Description
---|---|---
`lockAcquireSleepInterval`|`10000`|The polling interval (in milliseconds) between lock acquire attempts.
`failIfLocked`|`false`|Should the broker start fail if the lock is not immediately available. When `true` slave brokers will not start.

Persistence Adapters
--------------------

Every persistence adapter (or any other broker service that wishes to use locks) must implement the [Lockable](https://fisheye6.atlassian.com/browse/activemq/trunk/activemq-broker/src/main/java/org/apache/activemq/broker/Lockable.java?r=1383400) interface. This interface has the following properties:


Property Name|Default Value|Description
---|---|---
`useLock`|`true`|Should the persistence adapter use the configured locker. Intended primarily for use during development to temporarily disable the use of the locker without having to remove its configuration.
`lockKeepAlivePeriod`|`0`|The duration (in milliseconds) to keep the lock alive, when greater than `0`.

Existing Lockers
----------------

### Shared File Locker

The Shared File Locker is the default locker for the KahaDB persistence adapter. It locks a file to ensure that only the broker holding the lock (the master) is granted access to the message store.

#### Example:
```
<persistenceAdapter>
	<kahaDB directory="target/activemq-data" lockKeepAlivePeriod="10000">
		<locker>
			<shared-file-locker lockAcquireSleepInterval="5000"/>
		</locker>
	</kahaDB>
</persistenceAdapter>
```

The `lockKeepAlivePeriod` attribute is not applicable to versions of ActiveMQ older than 5.9.0.

> Consequences of lockKeepAlivePeriod = 0
> 
> For this locker `lockKeepAlivePeriod` should be greater than `0`.This period is the frequency with which the master broker makes lock keep alive calls.
> 
> When `lockKeepAlivePeriod = 0` slave brokers are still unable to obtain the file lock. However, if some third party modifies the lock file (either modification or deletion) the master broker will not detect the change. Therefore a slave broker's next attempt (per its configured `lockAcquireSleepInterval`) to obtain the file lock will succeed. When this happens there will be two master brokers in the cluster. _This situation will result in message store corruption_!
> 
> When `lockKeepAlivePeriod` is greater than `0`, the master broker will make a lock keep alive call every `lockKeepAlivePeriod` milliseconds. Therefore the master broker will detect any lock file changes when it makes its next keep alive call. Upon detecting said change the master broker will demote itself to a slave broker.

> Note that as of ActiveMQ 5.9.0 the KahaDB persistence adapter can also use the Lease Database Locker (see below).

### Database Locker

The Database Locker is the default locker for the JDBC persistence adapter. It locks a database table in a transaction to ensure that only single resource is used.

#### Example:
```
<persistenceAdapter>
    <jdbcPersistenceAdapter dataDirectory="${activemq.data}" dataSource="#mysql-ds" lockKeepAlivePeriod="10000">
        <locker>
            <database-locker lockAcquireSleepInterval="5000"/>
        </locker>
    </jdbcPersistenceAdapter>
</persistenceAdapter>
```

The Database Locker uses its `keepAlive` method to ensure the broker still holds the lock. You can set the keep alive period using the `lockKeepAlivePeriod` property. The default period is 30000 ms. If a broker fails to acquire the lock on the database, it will retry every `lockAcquireSleepInterval` milliseconds.

This locker opens a JDBC transaction against a database table (`activemq_lock`) that lasts as long as the broker remains alive. This locks the entire table and prevents another broker from accessing the store. In most cases this will be a fairly long running JDBC transaction which occupies resources on the database over time.

A problem with this locker can arise when the master broker crashes or loses its connection to the database causing the lock to remain in the database until the database responds to the half closed socket connection via a TCP timeout. The database lock expiry requirement can prevent the slave from starting some time. In addition, if the database supports failover, and the connection is dropped in the event of a replica failover, that JDBC transaction will be rolled back. The broker sees this as a failure. Both master and slave brokes will again compete for a lock.

### Lease Database Locker

The Lease Database Locker was created to solve the shortcomings of the Database Locker. The Lease Database Locker does not open a long running JDBC transaction. Instead it lets the master broker acquire a lock that's valid for a fixed (usually short) duration after which it expires. To retain the lock the master broker must periodically extend the lock's lease before it expires. Simultaneously the slave broker checks periodically to see if the lease has expired. If, for whatever reason, the master broker fails to update its lease on the lock the slave will take ownership of the lock becoming the new master in the process. The leased lock can survive a DB replica failover.

#### Example:
```
<persistenceAdapter>
	<jdbcPersistenceAdapter dataDirectory="${activemq.data}" dataSource="#mysql-ds" lockKeepAlivePeriod="5000">
		<locker>
			<lease-database-locker lockAcquireSleepInterval="10000"/>
		</locker>
	</jdbcPersistenceAdapter>
</persistenceAdapter>
```

In order for this mechanism to work correctly, each broker in a master/slave(s) cluster must have a unique value for the `brokerName` attribute as defined on the `<broker/>` tag. Alternatively, use unique values for the `leaseHolderId` attribute on the `<lease-database-locker/>` tag as this value is used to create a lease lock definition.

The lease based lock is acquired by blocking at startup. It is then retained for a period whose duration (in ms) is given by the `lockKeepAlivePeriod` attribute. To retain the lock the master broker periodically extends its lease by `lockAcquireSleepInterval` milliseconds each time. In theory, therefore, the master broker is always (`lockAcquireSleepInterval - lockKeepAlivePeriod`) ahead of the slave broker with regard to the lease. It is imperative that `lockAcquireSleepInterval > lockKeepAlivePeriod`, to ensure the lease is always current. As of ActiveMQ 5.9.0 a warning message is logged if this condition is not met.

In the simplest case, the clocks between master and slave must be in sync for this solution to work properly. If the clocks cannot be in sync, the locker can use the system time from the database CURRENT TIME and adjust the timeouts in accordance with their local variance from the DB system time. If `maxAllowableDiffFromDBTime` is greater than zero the local periods will be adjusted by any delta that exceeds `maxAllowableDiffFromDBTime`.

> It is important to know if the default rules your JDBC driver uses for converting `TIME` values are JDBC compliant. If you're using MySQL, for example, the driver's JDBC URL should contain `useJDBCCompliantTimezoneShift=true` to ensure that `TIME` value conversion is JDBC compliant. If not the locker could report a large time difference when it compares the retrieved lease expiration time against the current system time. Consult your JDBC driver's documentation for more details.

As of ActiveMQ 5.9.0 the lease database locker can be used in conjunction with the KahaDB persistence adapter. However, this particular combination requires that the lease database locker element contains a `<statements/>` child element. In the example below the `lockTableName` is also configured, although doing so is not mandatory.
```
<persistenceAdapter>
	<kahaDB directory="target/activemq-data" lockKeepAlivePeriod="5000">
		<locker>
            <!\-\- When used with the KahaDB persistence adapter the 'dataSource' attribute must be defined on the locker itself: -->
            <lease-database-locker lockAcquireSleepInterval="10000" dataSource="#mysql-ds">
                <statements>
                    <!\-\- Default locker attributes and SQL statements may be overridden here 
                         using one or more <statements attribute\_or\_statement="value"/> entries: -->  
                    <statements lockTableName="activemq_lock"/>
                </statements>
            </lease-database-locker>
		</locker>
	</kahaDB>
</persistenceAdapter>
```
> To see the complete list of attributes and SQL statements that can be overridden see the [Statements](https://fisheye6.atlassian.com/browse/activemq/trunk/activemq-jdbc-store/src/main/java/org/apache/activemq/store/jdbc/Statements.java?hb=true) class.

When the KahaDB persistence adapter is configured to use the `lease-database-locker` you must configure the broker to use your own IO exception handler as neither the `DefaultIOExceptionHandler` nor the `JDBCIOExceptionHandler` will work correctly with this combination. See [Configurable IOException Handlers](configurable-ioexception-handling) for details on how to write a handler.

> As of ActiveMQ 5.11, however, the `JDBCIOExceptionHandler` has been deprecated. It has been replaced by the `org.apache.activemq.util.LeaseLockerIOExceptionHandler` that will work with any persistence adapter that supports pluggable storage lockers, regardless if one is configured.

