---
title: "Failover Transport Reference"
description: "Configure failover:(uri1,uri2) in ActiveMQ Classic: reconnect options, randomize, timeout, priorityBackup, nested options and broker-side cluster updates."
slug: /failover-transport-reference
---

### The Failover Transport

The Failover transport layers reconnect logic on top of any of the other transports. The configuration syntax allows you to specify any number of composite URIs. The Failover transport randomly chooses one of the composite URIs and attempts to establish a connection to it. If it does not succeed, or if it subsequently fails, a new connection is established choosing one of the other URIs randomly from the list.

> Configuration Syntax
> ```
> failover:(uri1,...,uriN)?transportOptions&nestedURIOptions
> ```
> or
> ```
> failover:uri1,...,uriN
> ```

Example:
```
failover:(tcp://localhost:61616,tcp://remotehost:61616)?initialReconnectDelay=100
```
#### Transport Options

Option Name|Default Value|Description
---|---|---
`backup`|`false`|Initialize and hold a second transport connection - to enable fast failover.
`initialReconnectDelay`|`10`|The delay (in ms) before the _first_ reconnect attempt.
`maxCacheSize`|`131072`|Size in bytes for the cache of tracked messages. Applicable only if `trackMessages` is `true`.
`maxReconnectAttempts`|`-1 | 0`|**From ActiveMQ 5.6**: default is `-1`, retry forever. `0` means disables re-connection, e.g: just try to connect once. **Before ActiveMQ 5.6**: default is `0`, retry forever. **All ActiveMQ versions**: a value `>0` denotes the maximum number of reconnect attempts before an error is sent back to the client.
`maxReconnectDelay`|`30000`|The maximum delay (in ms) between the _second and subsequent_ reconnect attempts.
`nested.*`|`null`|**From ActiveMQ 5.9:** common URI options that will be applied to each URI in the list**.**
`randomize`|`true`|If `true`, choose a URI at random from the list to use for reconnect.
`reconnectDelayExponent`|`2.0`|The exponent used during exponential back-off attempts.
`reconnectSupported`|`true`|Determines whether the client should respond to broker `ConnectionControl` events with a reconnect (see: `rebalanceClusterClients`).
`startupMaxReconnectAttempts`|`-1`|A value of `-1` denotes that the number of connection attempts at startup should be unlimited. A value of  `>=0` denotes the number of reconnect attempts at startup that will be made after which an error is sent back to the client when the client makes a subsequent reconnect attempt. **Note**: once successfully connected the `maxReconnectAttempts` option prevails.
`timeout`|`-1`|**From ActiveMQ 5.3**: set the timeout on send operations (in ms) without interruption of re-connection process. 
`trackMessages`|`false`|Keep a cache of in-flight messages that will flushed to a broker on reconnect.
`updateURIsSupported`|`true`|**From** **ActiveMQ 5.4:** determines whether the client should accept updates from the broker to its list of known URIs.
`updateURIsURL`|`null`|**From ActiveMQ 5.4:** a URL (or path to a local file) to a text file containing a comma separated list of URIs to use for reconnect in the case of failure.
`useExponentialBackOff`|`true`|If `true` an exponential back-off is used between reconnect attempts.
`warnAfterReconnectAttempts`|`10`|**From ActiveMQ 5.10:** a value `>0` specifies the number of reconnect attempts before a warning is logged. A logged warning indicates that there is no current connection but re-connection is being attempted. A value of `<=0` disables the logging of warnings about reconnect attempts. 

#### Using Randomize

The Failover transport chooses a URI at random by default. This effectively load-balances clients over multiple brokers. However, to have a client connect to a primary first and only connect to a secondary backup broker when the primary is unavailable, set `randomize=false`.

Example:
```
failover:(tcp://primary:61616,tcp://secondary:61616)?randomize=false
```

##### Notes

Under the Failover transport send operations will, by default, block indefinitely when the broker becomes unavailable. There are two options available for handling this scenario. First, either set a [TransportListener](http://activemq.apache.org/components/classic/documentation/maven/apidocs/org/apache/activemq/transport/TransportListener.html) directly on the [ActiveMQConnectionFactory](http://activemq.apache.org/components/classic/documentation/maven/apidocs/org/apache/activemq/ActiveMQConnectionFactory.html), so that it is in place before any request that may require a network hop or second, set the `timeout` option. The `timeout` option causes the current send operation to fail after the specified timeout.

Example:
```
failover:(tcp://primary:61616)?timeout=3000
```
In this example if the connection isn't established the send operation will timeout after 3 seconds. It is important to note that the connection _is not killed_ when a timeout occurs. It is possible, therefore, to resend the affected message(s) later using the _same_ connection once a broker becomes available.

##### Transactions

The Failover transport tracks transactions by default. In-flight transactions are replayed upon re-connection. For simple scenarios this works as expected. However, there is an assumption regarding acknowledged (or consumer) transactions in that the previously received messages will automatically be replayed upon re-connection. This, however, is not always true when there are many connections and consumers, as re-delivery order is not guaranteed as stale outstanding acknowledgements can interfere with newly delivered messages. This can lead to unacknowledged messages.

**From ActiveMQ 5.3.1**: re-delivery order **_is_** tracked and a transaction will fail to commit if outstanding messages are not redelivered after failover. A `javax.jms.TransactionRolledBackException` is thrown if the commit fails. In doubt transactions will result in a rollback such that they can be replayed by the application. In doubt transactions occur when failover happens when a commit message is in-flight. It is not possible to know the exact point of failure. Did failure happen because the transaction commit message was not delivered or was the commit reply lost? In either case, it becomes necessary to rollback the transaction so that the application can get an indication of the failure and deal with any potential problem.

##### Broker-side Options for Failover

**From ActiveMQ 5.4**: the `TransportConnector` has options available so that the broker can update clients automatically with information regarding the presence of new brokers that are available (or are no longer available) for failover.

The options are:

Option Name|Default Value|Description
---|---|---
`updateClusterClients`|`false`|If `true`, pass information to connected clients about changes in the topology of the broker cluster.
`rebalanceClusterClients`|`false`|If `true`, connected clients will be asked to re-balance across a cluster of brokers when a new broker joins the network of brokers (note: `priorityBackup=true` can override).
`updateClusterClientsOnRemove`|`false`|If `true`, will update clients when a cluster is removed from the network. Having this as separate option enables clients to be updated when new brokers join, but _not_ when brokers leave.
`updateClusterFilter`|`null`|Comma separated list of regular expression filters used to match broker names of brokers to designate as being part of the failover cluster for the clients.

Example:
```
<broker>
  ...
  <transportConnectors>
    <transportConnector name="openwire" uri="tcp://0.0.0.0:61616" updateClusterClients="true" updateClusterFilter=".\*A.\*,.\*B.\*"/>
  </<transportConnectors>
  ...
</broker>
```
When `updateClusterClients=true` clients need only be configured with the details of one broker in a cluster in order to connect.

Example:
```
failover:(tcp://primary:61616)
```
When new brokers join the cluster the client is automatically informed of the new broker's URI. The new URI is then available for failover when one of the other known brokers becomes unavailable.

> Additional Information
> 
> See the following blog entry about using the cluster client updates and re-balancing features titled [New Features in ActiveMQ 5.4: Automatic Cluster Update and Rebalance](http://bsnyderblog.blogspot.com/2010/10/new-features-in-activemq-54-automatic.html).

##### Priority Backup

**From ActiveMQ 5.6**: if brokers are available in both local and remote networks, it's possible to specify a preference for local brokers over remote brokers using the `priorityBackup` and `priorityURIs` options.

Consider the following URL:
```
failover:(tcp://local:61616,tcp://remote:61616)?randomize=false&priorityBackup=true
```
Given this URL a client will try to connect and stay connected to the `local` broker. If `local` broker fails, it will of course fail over to `remote`. However, as `priorityBackup` parameter is used, the client will constantly try to reconnect to `local`. Once the client can do so, the client will re-connect to it without any need for manual intervention.

By default, only the first URI in the list is considered prioritized (`local`). In most cases this will suffice. However, in some cases it may be necessary to have multiple "local" URIs. The `priorityURIs` option can be used to specify which URIs are considered prioritized.

Example:
```
failover:(tcp://local1:61616,tcp://local2:61616,tcp://remote:61616)?randomize=false&priorityBackup=true&priorityURIs=tcp://local1:61616,tcp://local2:61616
```
In this case the client will prioritize either `local1` or `local2` brokers and (re-)connect to them if they are available.

##### Configuring Nested URI Options.

**From ActiveMQ 5.9**: common URI options can be configured by appending them to the query string of the failover URI where each common URI option has the prefix: `nested.` 

Example - instead of doing this:
```
failover:(tcp://broker1:61616?wireFormat.maxInactivityDuration=1000,tcp://broker2:61616?wireFormat.maxInactivityDuration=1000,tcp://broker3:61616?wireFormat.maxInactivityDuration=1000) 
```
do this:
```
failover:(tcp://broker1:61616,tcp://broker2:61616,tcp://broker3:61616)?nested.wireFormat.maxInactivityDuration=1000
```

> Any option that can applied to the query string of an individual URI is a candidate for use with the `nested` option.

> **Option Precedence**
> 
> If the same option is specified as both an individual URI option _and_ as a nested option, the nested option definition will take precedence.

