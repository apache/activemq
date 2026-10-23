---
title: "Title"
slug: /user-manual
---

       

Apollo 1.7.1 User Manual
========================

*   [Creating a Broker](#creating-a-broker)
*   [Broker Configuration](#broker-configuration)
*   *   [Automatic Configuration Reloading](#automatic-configuration-reloading)
    *   [Adjusting JVM Settings](#adjusting-jvm-settings)
    *   [Understanding the `apollo.xml` File](#understanding-the-apolloxml-file)
    *   *   [Connectors](#connectors)
        *   *   [TCP Transports](#tcp-transports)
            *   [WebSocket Transports](#websocket-transports)
            *   [UDP Transports](#udp-transports)
        *   [Virtual Hosts](#virtual-hosts)
        *   *   [Queues](#queues)
            *   [Topics](#topics)
            *   [Durable Subscriptions](#durable-subscriptions)
            *   [Mirrored Queues](#mirrored-queues)
            *   [Message Stores](#message-stores)
    *   [Support Platforms](#support-platforms)
    *   [Security](#security)
    *   [Working Around Java 7 SSL Bugs](#working-around-java-7-ssl-bugs)
    *   *   [The SSL/TLS Transport](#the-ssltls-transport)
        *   [Authentication](#authentication)
        *   *   [Using Custom Login Modules](#using-custom-login-modules)
        *   [Authorization](#authorization)
        *   *   [Ordering](#ordering)
        *   [Resource Actions](#resource-actions)
        *   [Resource Kinds](#resource-kinds)
        *   [Encrypting Passwords in the Configuration](#encrypting-passwords-in-the-configuration)
    *   [Web Based Administration](#web-based-administration)
*   [Managing Brokers](#managing-brokers)
*   *   [Running a Broker Instance in the Foreground](#running-a-broker-instance-in-the-foreground)
    *   [Managing a Background Broker Instance](#managing-a-background-broker-instance)
    *   *   [On Linux/Unix](#on-linuxunix)
        *   [On Windows](#on-windows)
    *   [Viewing Broker State](#viewing-broker-state)
    *   [Exporting/Importing Stores](#exportingimporting-stores)
*   [Messaging Protocols Manuals](#messaging-protocols-manuals)

Creating a Broker
-----------------

A broker instance is the directory containing all the configuration and runtime data such as logs and data files associated with a broker process. It is recommended that you do _not_ create the instance directory under the directory where the Apollo distribution is installed.

On unix systems, it is a common convention to store this kind of runtime data under the `/var/lib` directory. For example, to create an instance at '/var/lib/mybroker', run:

    cd /var/lib
    apollo create mybroker

A broker instance directory will contain the following sub directories:

*   `bin` : holds execution scripts associated with this instance.
*   `etc` : hold the instance configuration files
*   `data` : holds the data files used for storing persistent messages
*   `log` : holds rotating log files
*   `tmp` : holds temporary files that are safe to delete between broker runs

At this point you may want to adjust the default configuration located in etc directory.

Broker Configuration
--------------------

Each broker instance can be tuned and configured by editing one of the following files.

*   `bin/apollo-broker` : You can update the start script to control JVM level configuration options like JVM memory sizing.
*   `etc/apollo.xml` : The primary configuration file for the broker. It controls the opened ports, the queues, security, virtual host settings and more.
*   `etc/log4j.properties` : This is a standard [log4j](http://logging.apache.org/log4j/1.2/manual.html) configuration file which controls the broker's logging.
*   `etc/keystore` : A Java key store used to hold cryptographic security keys and certificates. It is only need for brokers using SSL.
*   `etc/login.conf` : A standard JAAS \[login.conf\] configuration file used to define the JAAS authentication domains available to the broker.
*   `etc/users.properties` : Holds userid/password mappings of users that can access the broker. Referenced by the `etc/login.conf` file.
*   `etc/groups.properties` : Holds groups to users mappings so that you can simplify access control lists (ACL) by using group instead listing individual users.
*   `black-list.txt` : A list of IP address which are banned from connecting to the broker.

### Automatic Configuration Reloading

Once a broker is started, you can edit any of the configuration files in the `etc` directory and your changes will be automatically reloaded. The configuration update will be applied in the least non-disruptive way possible. For example, if you removed a connector, the port that connector was listening on will be released and no new connections will be accepted. Connections that were previously accepted by that connector will continue to operate normally.

### Adjusting JVM Settings

You can define the following environment variables in the `bin/apollo-broker` start script to customize the JVM settings:

*   `JAVACMD` : The path to the java executable to use
*   `JVM_FLAGS` : The first JVM flags passed. Defaults to `-server -Xmx1G`, you may want to lower or raise the maximum memory based on your actual usage.
*   `APOLLO_OPTS` : Additional JVM options you can add
*   `APOLLO_DEBUG` : Set to true to enabled debugging on port 5005
*   `APOLLO_PROFILE` : Set to true to YourKit based profiling
*   `JMX_OPTS` : The JMX JVM options used, defaults to -Dcom.sun.management.jmxremote

Make sure you define the variables before the `apollo-broker` script executes `apollo` and that the variables get exported in the case of the unix script.

### Understanding the `apollo.xml` File

There are many XSD aware XML editors which make editing XML configuration files less error prone. If you're using one of these editors, you can configure it to use this apollo.xsd file.

The simplest valid `apollo.xml` defines a single virtual host and a single connector.

                               s
    <broker xmlns="http://activemq.apache.org/schema/activemq/apollo">
    
      <virtual_host id="default">
        <host_name>localhost</host_name>
        <null_store/>
      </virtual_host>
    
      <connector id="tcp" bind="tcp://0.0.0.0:61613"/>
    
    </broker>
    

The broker, virtual host, and connector are assigned an id which which is used to by the REST based administration console to identify the corresponding runtime resource. The virtual host will not persist any messages sent to it since it is using the `null_store`.

Brokers can be configured with multiple virtual hosts and connectors.

When a broker is first started up, it will validate the configuration file against the the XSD Schema and report any errors/warnings it finds but it will continue to start the broker even it finds problems. You would want to the broker to abort starting up if any issues are found with the schema validation you should set the `broker` element's `validation` attribute to `strict`. Example:

    
    <broker validation="strict" 
       xmlns="http://activemq.apache.org/schema/activemq/apollo">
      ...
    </broker>
    

If you would like the broker to automatically trigger a Java heap garbage collection (GC) cycle periodically, add a `auto_gc` element within the `broker` element. GC cycles will automatically kick in when they are needed, but if your monitoring broker heap usage with an external monitoring tool, then periodically forcing a GC cycle might be nice since then your monitoring tool can more accurate track actual heap usage. Set the `interval` attribute of the `auto_gc` to the number of seconds between forced GC cycles. If interval is not set, it will default to 30.

Example:

    
    <broker>
      ...
      <auto_gc interval="10">
      ...
    </broker>
    

#### Connectors

A broker connector is used to accept new connections to the broker. A `connector` element can be configured with the following attributes

*   `enabled` : if set to false, then the connector host will be disabled.
    
*   `bind` : The transport that the connector will listen on, it includes the ip address and port that it will bind to. Transports are specified using a URI syntax.
    
*   `connection_limit` : The maximum number of concurrently open connections this connector will accept before it stops accepting additional connections. If not set, then there is no limit.
    
*   `protocol` : Defaults to `any` which means that any of the broker's supported protocols can connect via this transport.
    
*   `receive_buffer_size_auto_tune` : Sets whether or not to auto tune the internal socket receive buffer (aka the socket's SO_RCVBUF). Auto tuning happens every 1 second. Default is true
    
*   `send_buffer_size_auto_tune` : Sets whether or not to auto tune the internal socket send buffer (aka the socket's SO_SNDBUF). Auto tuning happens every 1 second. Default is true
    

By default, the broker will 'auto-tune' a connector's transports to be between '64k' and '2k' based on the max number of connections established against the broker in the last 5 minutes and the size of the JVM heap. Set `receive_buffer_size_auto_tune` and `send_buffer_size_auto_tune` to false to disable this auto tuning.

Furthermore, the connector element may contain protocol specific configuration elements. For example, to have the broker set the `user_id` header of messages to the id of user that sent the message, you would use the following configuration:

    
    <connector id="tcp" bind="tcp://0.0.0.0:61613">
      <stomp add_user_header="user_id"/>
    </connector>
    

If you're using the `any` protocol then actual protocol being used will be detected by examining the client's initial request. You can use the `detect` element within a `connector` element to configure the protocol detection settings. The `detect` element supports the following attributes:

*   `timeout` : maximum amount of time (in milliseconds) that protocol detection is allowed to take. Defaults to 5000 milliseconds. If a client does not send an initial request before the timeout expires, the connection is closed.

Example of how to set the protocol detection timeout to 30 seconds:

    
    <connector id="tcp" bind="tcp://0.0.0.0:61613">
      <detect timeout="30000"/>
    </connector>
    

##### TCP Transports

The TCP transport uses the `tcp://` URI scheme. It uses the URI host and port to determine to which local interfaces to bind. For example:

*   `tcp://0.0.0.0:61613` binds to all IPv4 interfaces on port 61613
*   `tcp://[::]:61613` binds to all IPv4 and IPv6 interfaces on port 61613
*   `tcp://127.0.0.1:0` binds to the loopback interface on a dynamic port

The TCP URI also supports several query parameters to fine tune the settings used on the socket at the time of creation. The supported parameters are:

*   `backlog` : Sets the listen backlog size. Defaults to 100.
    
*   `keep_alive` : Enable or disable the SO_KEEPALIVE socket option  
    (aka setting the socket's SO_KEEPALIVE). Defaults to true.
    
*   `traffic_class` : Sets traffic class or type-of-service octet in the IP header for packets sent from the transport (aka setting the socket's IP_TOS).  
    Defaults to `8` which means the traffic should be optimized for throughput.
    
*   `max_read_rate` : Sets the maximum bytes per second that this transport will receive data at. This setting throttles reads so that the rate is not exceeded. Defaults to 0 which disables throttling.
    
*   `max_write_rate` : Sets the maximum bytes per second that this transport will send data at. This setting throttles writes so that the rate is not exceeded. Defaults to 0 which disables throttling.
    
*   `receive_buffer_size` : Sets the initial size of the internal socket receive buffer (aka setting the socket's SO_RCVBUF)
*   `send_buffer_size` : Sets the initial size of the internal socket send buffer (aka setting the socket's SO_SNDBUF)

Example which uses a couple of options:

    
    <connector id="tcp" bind="tcp://0.0.0.0:61613?receive_buffer_size=1024&amp;max_read_rate=65536"/>
    

Note that

    &amp;

was used to separate the option values instead of just `&` since the URI is within an XML file. In the URI string, we specify what the buffer sizes should be when the socket is created, but their values can change if auto-tuning is enabled.

##### WebSocket Transports

HTML 5 introduced WebSockets, as a standardized way to communicate asynchronously with the server from a web page. This is an ideal channel for implementing asynchronous messaging in web pages. It can be used to encapsulate other protocols like STOMP and it avoids needing to use Comet techniques like long polling to deliver data to the Browser. Furthermore, since JavaScript easily handles text and JSON formatted data, the STOMP protocol is a natural choice for the messaging protocol to be used over WebSocket.

The WebSocket transport uses the `ws://` URI scheme and the secure WebSocket transport uses the `wss://` URI scheme. Like the TCP transport, this transport uses the URI host and port to determine to which local interfaces to bind. For example:

*   `ws://0.0.0.0:61623` binds to all IPv4 interfaces on port 61623
*   `ws://[::]:61623` binds to all IPv4 and IPv6 interfaces on port 61623
*   `wss://127.0.0.1:0` binds to the loopback interface on a dynamic port

The WebSocket URI also supports the following query parameters to fine tune the settings used on the socket:

*   `binary_transfers` : Should data be sent to the client as binary blobs. Currently not all browsers support binary WebSocket data. Defaults to false.
*   `cors_origin` : Specify cross-origin resource sharing limmitations, including `*` all or individual server names
*   `max_text_message_size` : Specify the size of text messages that the websocket connector can handle
*   `max_binary_message_size` : Specify the size of binary messages that the websocket connector can handle
*   `max_idle_time` : timeout limitations of the underlying websocket socket

Example configuraiton:

    
    <connector id="ws" bind="ws://0.0.0.0:61623?binary_transfers=false"/>
    

One thing worth noting is that web sockets (just as Ajax) implements the same origin policy, so by default you can access only brokers running on the same host as where the web page originated from.

If you want to allow cross origin resource sharing (CORS) of the WebSocket connector, by different hosts then you should add `cors_origin` query parameter to the bind URI with a common seperated list of domains that are allowed to access the WebSocket connector. Use `*` to allow access from any domain. Example:

    
    <connector id="ws" bind="ws://0.0.0.0:61623?cors_origin=*"/>
    

###### WebSocket Clients

You can use one of the following JavaScript libraries to access the broker over WebSockets:

*   [Stomple](http://github.com/krukow/stomple)
*   [stomp-websocket](http://github.com/jmesnil/stomp-websocket)

The Apollo distribution include a couple of simple WebSocket based chat example in the following distribution directories:

*   `examples/stomp/websocket`
*   `examples/mqtt/websocket`

##### UDP Transports

The UDP transport uses the `udp://` URI scheme. It uses the URI host and port to determine to which local interfaces to bind. For example:

*   `udp://0.0.0.0:61615` binds to all IPv4 interfaces on port 61615
*   `udp://[::]:61615` binds to all IPv4 and IPv6 interfaces on port 61615
*   `udp://127.0.0.1:0` binds to the loopback interface on a dynamic port

The UDP transport MUST be configured to use a UDP specific protocol handler. That is done by setting the `protocol` attribute on the `connector` element.

Example:

    
    <connector id="udp" bind="udp://0.0.0.0:61615" protocol="udp"/>
    

The supported protocols that can be used with the udp transport are:

*   `udp` : Takes the all the data in a received UDP datagram and forwards it as a binary message to a configured topic. This protocol can be configured by nesting a `udp` element within the `connector` element. The supported options on the `udp` element are:
    
    *   `topic` : The name of the topic to send messages. Defaults to `udp`.
    *   `buffer_size` : The amount of memory to use to buffer between the socket and topic endpoint. Defaults to `640k`.
*   `stomp-udp` : Expects the received UDP datagram to contain a STOMP frame. The STOMP frame is processed and routed just like any STOMP frame that would be received on a TCP transport. This protocol can be configured by nesting a `stomp` element within the `connector` element. See the [Stomp Protocol Options section](stomp-manual.md#stomp-protocol-options) for more details.
    

#### Virtual Hosts

A virtual hosts allows Apollo to support multi tenant style configurations. Each virtual host is highly isolated each with its own persistence, security, and runtime constraints configuration.

Protocols like STOMP 1.1, inform the broker of which host the client is attempting to connect with. The broker will search its list of virtual hosts to find the first host who has a configured `host_name` that matches. Protocols which do NOT support virtual hosts, will just connect to the first virtual host defined in the configuration file.

*   `host_name` : a host name that the virtual host is known as. This element should be repeated multiple times if the host has many host names.

A `virtual_host` element may be configured with the following attributes:

*   `enabled` : if set to false, then the virtual host will be disabled.
    
*   `purge_on_startup` : if set to true, the persistent state of the broker will be purged when the broker is started up.
    

The `virtual_host` can also define multiple `topic`, `queue`, and `dsub` elements to secure or tune how message delivery works for different topics or queues. If none are defined, then sensible default settings are used which allows destinations to be auto created as they get accessed by applications.

Finally `virtual_host` configuration should also include a message store configuration element to enable message persistence on the virtual host.

##### Queues

When a new queue is first created in the broker, its configuration will be determined by the first `queue` element which matches the queue being created. The attributes matched against are:

*   `id` : The name of the queue, you can use wild cards to match multiple or don't set to match all queues.

If the queue definition is not using a wild card in the id, then the queue will be created when the broker first starts up.

A `queue` element may be configured with the following attributes:

*   `mirrored` : If set to true, If set to true, then once the queue is created all messages sent to the queue will be mirrored to a topic of the same name and all messages sent to the topic will be mirror to the queue. See the [Mirrored Queues](#mirrored-queues) documentation for more details. Defaults to false.
    
*   `tail_buffer` : The amount of memory buffer space allocated for holding freshly enqueued message. Defaults to `640k`.
    
*   `persistent` : If set to false, then the queue will not persistently store its message. Defaults to true.
    
*   `message_group_graceful_handoff` : When set to true, the queue will drain message group consumers of messages before allowing new messages to dispatched to messages groups which have been moved to a different consumer due to re-balancing. Defaults to true.
    
*   `round_robin` : Should the destination dispatch messages to consumers using round robin distribution strategy? Defaults to true. If set to false, then messages will be dispatched to the first attached consumers until those consumers start throttling the broker.
    
*   `swap` : If set to false, then the queue will not swap messages out of memory. Defaults to true.
    
*   `swap_range_size` : The number max number of flushed queue entries to load from the store at a time. Note that Flushed entires are just reference pointers to the actual messages. When not loaded, the batch is referenced as sequence range to conserve memory. Defaults to 10000.
    
*   `quota` : The maximum amount of disk space the queue is allowed to grow to. If set to -1 then there is no limit. You can use settings values like: `500mb` or `1g` just plain byte count like `1024000`. Once the quota is Exceeded, the producers will block until the usage decreases. Defaults to no limit.
    
*   `quota_messages` : The maximum number of messages queue is allowed to grow to. If not set then there is no limit. Defaults to no limit.
    
*   `auto_delete_after`: If not set to `0` then the queue will automatically delete once there have been no consumers, producers or messages on it for the configured number of seconds. Defaults to 30 if not set.
    
*   `fast_delivery_rate`: The message delivery rate (in bytes/sec) at which  
    the queue considers the consumers fast enough to start slowing down enqueue rate to match the consumption rate if the consumers are at the tail of the queue. Defaults to `1M`
    
*   `catchup_enqueue_rate`: The rate that producers will be throttled to when queue consumers are considered to be fast. This allows consumers to catch up and reach the tail of the queue. If not set, then it is computed to be 1/2 the current consumer delivery rate.
    
*   `max_enqueue_rate`: The maximum enqueue rate of the queue. Producers will be flow controlled once this enqueue rate is reached. If not set then it is disabled
    
*   `dlq`: Is the dead letter queue configured for the destination. A dead letter queue is used for storing messages that failed to get processed by consumers. If not set, then messages that fail to get processed will be dropped. If '*' appears in the name it will be replaced with the queue's id.
    
*   `nak_limit`: Once a message has been nacked the configured number of times the message will be considered to be a poison message and will get moved to the dead letter queue if that's configured or dropped. If set to less than one, then the message will never be considered to be a poison message. Defaults to zero.
    
*   `dlq_expired`: Should expired messages be sent to the dead letter queue?  
    Defaults to false.
    
*   `full_policy`: Once the queue is full, the `full_policy` controls how the queue behaves when additional messages attempt to be enqueued onto the queue.
    
    You can set it to one of the following options:
    
    *   `block`: The producer blocks until some space frees up.
    *   `drop tail`: Drops the new messages being enqueued on the queue.
    *   `drop head`: Drops old messages at the front of the queue.
    
    If the queue is persistent then it is considered full when the max quota size is reached. If the queue is not persistent then the queue is considered full once its `tail_buffer` fills up. Defaults to 'block' if not specified.
    

Example configuraiton:

    
    ...
      <virtual_host id="default">
        ...
        <queue id="app1.**" dlq="dlq.*" nak_limit="3" auto_delete_after="0"/>
        ...
      </virtual_host>
    ...
    

##### Topics

When a new topic is first created in the broker, its configuration will be determined by the first `topic` element which matches the topic being created. The attributes matched against are:

*   `id` : The name of the topic, you can use wild cards to match against multiple or don't set to match all topics.

If the topic definition is not using a wild card in the id, then the topic will be created when the broker first starts up.

A `topic` element may be configured with the following attributes:

*   `slow_consumer_policy` : Valid values are `block` and `queue`. Defaults to `block`. This setting defines how topic subscriptions are handled which affects slow consumer scenarios. If set to `queue` then each subscription gets a temporary queue which can swap messages to disk when you have a slow consumer so that produces do not slow down to the speed of the slowest consumer. If set to `block`, the producers block on slow consumers which makes producers only as fast as the slowest consumer on the topic.
    
*   `auto_delete_after`: If not set to `0` then the topic will automatically delete once there have been no consumers or producers on it for the configured number of seconds. Defaults to 30 if not set.
    

A `topic` that has the `slow_consumer_policy` set to `queue` can customize the settings of the per subscription queues by adding a nested `subscription` element. The `subscription` element supports the following configuration attributes of the `queue` element: `tail_buffer`, `persistent`, `swap` `swap_range_size`, `quota`, `full_policy`, `fast_delivery_rate`, `catchup_enqueue_rate`, `max_enqueue_rate`, `dlq`, `nak_limit`. Example:

    
    ...
      <virtual_host id="default">
        ...
        <topic id="example" slow_consumer_policy="queue">
          <subscription tail_buffer="4k"/>
        </topic>
        ...
      </virtual_host>
    ...
    

##### Durable Subscriptions

When a new durable subscription is first created in the broker, its configuration will be determined by the first `dsub` element which matches the durable subscription being created. The attributes matched against are:

*   `id` : The name of the subscription.
*   `id_regex` : A regular expression used to match against the subscription id

If you want to create the durable subscription when the broker first starts up, then you must set the `topic` attribute and optionally the `selector` attribute.

*   `topic` : The topic which the durable subscription will subscribe to.
*   `selector` : A selector expression which filters out messages

A `dsub` element may be configured with all the attributes available on the `queue` element.

##### Mirrored Queues

A mirrored queue, once create will copy all messages sent to the queue to a topic of the same name and conversely the queue will receive a copy of all messages sent to the topic.

Mirrored queues can be used to mix queue and topic behavior on one logical destination. For example, let's assumed `foo` is configured as a mirrored destination and you have 2 subscribers on queue `foo` and 2 subscribers on topic `foo`. On the producer side, publishers can send either the queue or topic and get the same results. On the consumer side, the 2 consumers the the queue foo will get queue semantics and message from the queue will be load balanced between the 2 consumers. The 2 consumers on the topic foo will each get a copy of every message sent. You can even create durable subscriptions on the topic which then effectively becomes a queue which mirrors the original queue.

It is important to note that the mirroring will not start until the queue is created which typically happens you first send a message to the queue or subscribe to it.

##### Message Stores

A message store is used to implement reliable messaging and message swapping which are both optional features that disabled if no message store is configured on the virtual host. If no message store is configured, then all message routing is performed in memory and queue will quickly “fill up” when you have slow or no consumers since the messages cannot get swapped to disk.

Apollo supports multiple message store implementations. The implementations currently supported are:

*   [LevelDB Store](#leveldb-store) : is a file based message store implemented using the [Google's LevelDB](http://en.wikipedia.org/wiki/LevelDB) library to maintain indexes into log files holding the messages. This the default implementation used.
*   [BDB Store](#bdb-store) : is a file based message store implemented using the [Sleepycat BDB](http://en.wikipedia.org/wiki/Berkeley_DB) library. This implemenation should work equally well on all platforms since it's a pure java implementation.

###### LevelDB Store

The LevelDB store is the default store which a newly created Broker instance will use.

It is enabled when your `virtual_host` element contains a `leveldb_store` element.

    
      ...
      <virtual_host id="default">
        ...
        <leveldb_store directory="${apollo.base}/data"/>
        ..
      </virtual_host>
      ...
    

A `leveldb_store` element may be configured with the following attributes:

*   `directory` : The directory which the store will use to hold its data files. The store will create the directory if it does not already exist.
*   `flush_delay` : The flush delay is the amount of time in milliseconds that a store will delay persisting a messaging unit of work in hopes that it will be invalidated shortly thereafter by another unit of work which would negate the operation. Defaults to 500.
*   `read_threads` : The number of concurrent IO reads to allow. The value defaults to 10.
*   `sync` : If set to `false`, then the store does not sync logging operations to disk. The value defaults to `true`.
*   `log_size` : The max size (in bytes) of each data log file before log file rotation occurs. The value defaults to 104857600 (100 MB).
*   `log_write_buffer_size`: That maximum amount of log data to build up before writing to the file system. The value defaults to 4194304 (4 MB).
*   `verify_checksums` : If set to `true` to force checksum verification of all data that is read from the file system on behalf of a particular read. By default, no such verification is done.
*   `paranoid_checks` : Make the store error out as soon as possible if it detects internal corruption. The value defaults to false.
*   `index_max_open_files` : Number of open files that can be used by the index. The value defaults to 1000.
*   `index_block_restart_interval` : Number keys between restart points for delta encoding of keys. The value defaults to 16.
*   `index_write_buffer_size` : Amount of index data to build up in memory before converting to a sorted on-disk file. The value defaults to 4194304 (4 MB).
*   `index_block_size` : The size of index data packed per block. The value defaults to 4096 (4 K).
*   `index_cache_size` : The maximum amount of memory to use to cache index blocks. The value defaults to 268435456 (256 MB).
*   `index_compression` : The type of compression to apply to the index blocks.  
    Can be `snappy` or `none`. The value defaults to `snappy`.
*   `log_compression` : The type of compression to apply to the log records.  
    Can be `snappy` or `none`. The value defaults to `snappy`.
*   `auto_compaction_ratio`: This ratio is used to determine when to compact the leveldb indexes. When you take the ratio of disk space used by the leveldb indexes to the number queue entries and it exceeds the configured `auto_compaction_ratio` then the leveldb index will be scheduled for compaction. If set to 0, then auto compactions are disabled. The value defaults to 100.

### Support Platforms

The LevelDB store uses a JNI driver on Linux, OS X, and supported Windows versions, but falls back to an experimental pure Java driver on other platforms.

The supported Windows versions are Vista, Server 2008 and later that have the MS VC++ 2010 Redistributable package installed:

*   If you're running a 32 bit JVM, install: [Microsoft Visual C++ 2010 Redistributable Package (x86)](http://www.microsoft.com/en-us/download/details.aspx?id=5555)
*   If you're running a 64 bit JVM, install: [Microsoft Visual C++ 2010 Redistributable Package (x64)](http://www.microsoft.com/en-us/download/details.aspx?id=14632)

###### BDB Store

Apache cannot redistribute the BDB library due to the terms of its license, but you can easily get a free copy directly from Oracle. Before you can start using the BDB Store you must first download it from Oracle at [je-5.0.34.jar](http://download.oracle.com/maven/com/sleepycat/je/5.0.34/je-5.0.34.jar) and then copy it into the `${APOLLO_HOME}/lib` directory.

For those of you with curl installed, you can just run:

    curl http://download.oracle.com/maven/com/sleepycat/je/5.0.34/je-5.0.34.jar > ${APOLLO_HOME}/lib/je-5.0.34.jar

Once that is done, you can enable the store by adding a `bdb_store` element inside your `virtual_host`. Example:

    
      ...
      <virtual_host id="default">
        ...
        <bdb_store directory="${apollo.base}/data"/>
        ..
      </virtual_host>
      ...
    

A `bdb_store` element may be configured with the following attributes:

*   `directory` : The directory which the store will use to hold its data files. The store will create the directory if it does not already exist.
*   `flush_delay` : The flush delay is the amount of time in milliseconds that a store will delay persisting a messaging unit of work in hopes that it will be invalidated shortly thereafter by another unit of work which would negate the operation. Defaults to 500.
*   `read_threads` : The number of concurrent read threads to use when accessing the store. The value defaults to 10.

### Security

### Working Around Java 7 SSL Bugs

As noted by issue [APLO-287](https://issues.apache.org/jira/browse/APLO-287), it seems some versions of Java 7 have problems with SSL sessions that need to use the Diffie-Hellman cypher suite. If you run into this issue, just copy the Bouncy Castle [bcprov-jdk15on-148.jar](http://www.bouncycastle.org/download/bcprov-jdk15on-148.jar) to Apollo's lib directory and restart your broker.

#### The SSL/TLS Transport

Apollo supports SSL/TLS for transport level security to avoid 3rd parties listening in on the communications between the broker and its clients. To enable it, you just need to add a connector which binds using on of the secure transports such as `ssl://`. It also requires having a `key_storage` configuration element under the `broker` to configure the where the encryption keys and certificates are stored.

Example:

    
      ...
      <key_storage 
         file="${apollo.base}/etc/keystore" 
         password="password" 
         key_password="password"/>
    
      <connector id="stomp-secure" bind="ssl://0.0.0.0:61614"/>
      ...
    

The `connector` element's `bind` attribute controls which secure transport algorithm gets used by the sever. Supported values are:

*   `ssl://` \- Use the JVM default version of the SSL algorithm.
*   `sslv*://` \- Use a specific SSL version where `*` is a version supported by your JVM. Example: `sslv3`
*   `tls://` \- Use the JVM default version of the TLS algorithm.
*   `tlsv*://` \- Use a specific TLS version where `*` is a version supported by your JVM. Example: `tlsv1.1`

The attributes that you can configure on the `key_storage` element are:

*   `file` : Path to where the key store is located.
*   `password` : The key store password
*   `key_alias` : The alias of private key to use. Defaults to the first key found in the key store.
*   `key_password` : The password to the keys in the key store.
*   `store_type` : The type of key store, defaults to `JKS`.
*   `trust_algorithm` : The trust management algorithm, defaults to `SunX509`.
*   `key_algorithm` : The key management algorithm, defaults to `SunX509`.

The SSL/TLS transport is an extension of the TCP transport and as such it supports all the same URI options which the TCP transport supports plus the following:

*   `client_auth` : can be set to one of the following: `want`, `need` or `none`. Defaults to `want`. If set to `need`, then the SSL connection will not be accepted if the client does not provide a certificate that is trusted by the key store. If set to `none`, then we will not request the client to send his certificates. If set to `want`, then we will request the client send his certficates but allow the connection to continue even if the does not have any tusted certs.

#### Authentication

The first step to securing the broker is authenticating users. The default Apollo configurations use file based authentication. Authentication is performed using JAAS who's config file is located in the instance directory under `etc/login.conf`. JAAS configuration files can define multiple named authentication domains. The `broker` element and `virtual_host` elements can be configured to authenticate against these domains.

The `authentication` element defined at the broker level will get used to authenticate broker level administration functions and to authenticate any virtual hosts which did not define an `authentication` element. If you want to disable authentication in a virtual host, you set the `enable` attribute to `false`.

    
    <broker xmlns="http://activemq.apache.org/schema/activemq/apollo">
      <authentication domain="internal"/>
    
      <virtual_host id="wine.com">
        <authentication domain="external"/>
        <host_name>wine.com</host_name>
      </virtual_host>
    
      <virtual_host id="internal.wine.com">
        <host_name>internal.wine.com</host_name>
      </virtual_host>
    
      <virtual_host id="test">
        <authentication enabled="false"/>
        <host_name>cheeze.com</host_name>
      </virtual_host>
    
      <connector id="tcp" bind="tcp://0.0.0.0:61613"/>
    </broker>
    

The above example uses 2 JAAS domains, `internal` and `external`. Broker The `wine.com` host will use the external domain, the `internal.wine.com` host will use the internal domain and the `test` host will not authenticate users.

##### Using Custom Login Modules

Apollo uses JAAS to control against which systems users authenticate. The default Apollo configurations use file based authentication but with a simple change of the JAAS configuration, you can be authenticating against local UNIX account or LDAP. Please reference the [JAAS documentation](http://download.oracle.com/javase/1.5.0/docs/guide/security/jaas/JAASRefGuide.html#AppendixB) for more details on how to edit the `etc/login.conf` file.

Since different JAAS login modules produce principals of different class types, you may need to configure which of those class types to recognize as the user principal and the principal used to match against the access control lists (ACLs).

The default user principal classes recognized are `org.apache.activemq.jaas.UserPrincipal` and `javax.security.auth.x500.X500Principal`. You can change the default by adding `user_principal_kind` elements under the `authentication` element. The first principal who's type matches this list will be selected as the user's identity for informational purposes.

Similarly, default acl principal class recognized is `org.apache.activemq.jaas.GroupPrincipal`. You can configure it by adding `acl_principal_kinds elements under the`authentication` element. The ACL entries which do not have an explicit kind will default to using the the kinds listed here.

Example of customizing the principal kinds used:

    
      ...
      <authentication domain="apollo">
        <user_principal_kind>com.sun.security.auth.UnixPrincipal</user_principal_kind>
        <user_principal_kind>com.sun.security.auth.LdapPrincipal</user_principal_kind>
        <acl_principal_kind>com.sun.security.auth.UnixPrincipal</acl_principal_kind>
        <acl_principal_kind>com.sun.security.auth.LdapPrincipal</acl_principal_kind>
      </authentication>
      ...
    </broker>
    

#### Authorization

User authorization to broker resources is accomplished by configuring an access control rules using a `access_rule` elements in the `broker` or `virtual_host` elements. The rules defines which principals are allowed or denied access to perform actions against server resources. An example list of rule is shown below:

    
    <broker>
      <access_rule deny="guest" action="send"/>
      <access_rule allow="*"    action="send"/>
      <access_rule allow="app1" action="receive"/>
    </broker>
    

The `allow` and `deny` attributes define the principals which are allowed or denied access. If set to “+" then it matches all principals but requires at at least one. If set to “*" the it matches all principals and even matches the case where there are no principals associated with the subject.

Either `allow` or `deny` must be defined. You can optionally define one or more of the following attributes to further narrow down when the rule matches an authorization check:

*   `separator`: If set, then the `allow` and `deny` fields will be interpreted to be a list of principles separated by the separator value.
    
*   `principal_kind`: A space separated list of class names of which will be matched against the principle type. If set to `*` then it matches all principal classes. Defaults to the default principal kinds configured on the broker or virtual host.
    
*   `action`: A space separated list of actions that will match the rule. Example 'create destroy'. You can use `*` to match all actions. Defaults to `*`.
    
*   `kind`: A space separated list of broker resource types that will match this rule. You can use `*` to match all key. Example values 'broker queue'. Defaults to `*`.
    
*   `id`: The identifier of the resource that will match this rule. You can use `*` to match all resources. If the `kind` is set to `queue` or `topic` then you can use a destination wild card to match against the destination id. Defaults to `*`
    
*   `id_regex`: A regular expression to be applied against the id of the resource.
    
*   `connector`: The id of the connector the user must be connected on for the rule to match. You can use `*` to match all connectors. Defaults to `*`.
    

If no access rules match an authorization check then access is denied.

##### Ordering

The order in which rules are defined are significant. The first entry that matches determines if he will have access. For example, let's say a user is groups 'blue' and 'red', and you are matching against the following rules:

    
    <access_rule deny="blue" action="send"/>
    <access_rule allow="red" action="send"/>
    

Then the user would not be allowed to send since the deny rule was matched first. If the order in the ACL list were reversed, like so:

    
    <access_rule allow="red" action="send"/>
    <access_rule deny="blue" action="send"/>
    

Then the user would be allowed access to the resource since the allow rule matched first. When a single rule defines both `allow` and `deny` attributes and they both match then the action is denied.

    
    <access_rule deny="blue" allow="red" action="send"/>
    

#### Resource Actions

You can configure the `action` attribute of an access rules with one or more of the following values:

*   `admin` : use of the administrative web interface
*   `monitor` : read only use of the administrative web interface
*   `config` : use of the administrative web interface to access and change the broker configuration.
*   `connect` : allows connections to the connector or virtual host
*   `create` : allows creation
*   `destroy` : allows destruction
*   `send` : allows the user to send to the destination
*   `receive` : allows the user to send to do non-destructive reads from the destination
*   `consume` : allows the user to do destructive reads against a destination
*   `*` : All actions

#### Resource Kinds

You can configure the `kind` attribute of an access rules with one or more of the following values: `broker`, `connector`, `virtual_host`, `topic`, `queue`, `dsub`, or `*`. `*` matches all resource kinds.

The `broker` and `connector` kinds can only be configured in rules defined in the `broker` element.

#### Encrypting Passwords in the Configuration

The `etc/apollo.xml` file supports using `${<property-name>}` style syntax. You can use any system properties and if the `etc/apollo.xml.properties` file exists, then any of the properties defined there. Any of the properties values in the `etc/apollo.xml.properties` can be replaced with encrypted versions by using the `apollo encrypt` command.

Let's say you your current `key_storage` contains plain text passwords that need to be replaced with encrypted versions:

    
      ...
      <key_storage 
         file="${apollo.base}/etc/keystore" 
         password="open" 
         key_password="sesame"/>
      ...
    

Let's first find out what the encrypted versions of the passwords would be. Apollo encrypts and decrypts values using the password stored in the `APOLLO_ENCRYPTION_PASSWORD` environment variable.

The following is an example of how you can encrypt the previous passwords:

    $ export APOLLO_ENCRYPTION_PASSWORD='keepmesafe'
    $ apollo encrypt open
    ENC(6r7HKCib0H8S+OuSfV+muQ==)
    $ apollo encrypt sesame
    ENC(FP+H2FIg++sSaOxg/ISknw==)

Once you have the encrypted passwords, you can add them to the `etc/apollo.xml.properties` file. Example:

    store.pass=ENC(6r7HKCib0H8S+OuSfV+muQ==)
    key.pass=ENC(FP+H2FIg++sSaOxg/ISknw==)

Finally the last step of securing the configuration is to replace the plain text passwords with variable references to the corresponding property names:

    
      ...
      <key_storage 
         file="${apollo.base}/etc/keystore" 
         password="${store.pass}" 
         key_password="${key.pass}"/>
      ...
    

When you use encrypted passwords in your configuration, you MUST make sure that the `APOLLO_ENCRYPTION_PASSWORD` environment variable is set to the proper value before starting the broker.

### Web Based Administration

Apollo starts a web based administration interface on [`http://127.0.0.1:61680`](http://127.0.0.1:61680) and [`https://127.0.0.1:61681`](https://127.0.0.1:61681) by default. Note that it binds to the loopback interface so that only local web browsers can access the interface.

If the broker has authentication enabled and has defined an ACL configuring the admins of the broker, then the web interface will perform basic authentication and will only grant access to those users which are in the admin ACL.

If you want to disable the web the interface then you should remove the `web_admin` configuration elements. If you want to allow remote administration, you should update the configuration so it bind either the `0.0.0.0` or `[::]` address.

For example:

    
    <broker xmlns="http://activemq.apache.org/schema/activemq/apollo">
      ...
      <web_admin bind="http://0.0.0.0:61680"/>
      <web_admin bind="https://0.0.0.0:61681"/>
      ...
    </broker>
    

A `web_admin` element may be configured with the following attributes:

*   `bind` : The address and port to bind the web interface on in URL syntax.

If you want to allow cross origin resource sharing (CORS) of the web admin APIs, then you should add `cors_origin` query parameter to the bind URI with a common seperated list of domains that are allowed to access the web admin APIs. Use `*` to allow access from any domain. Example:

    
    <broker xmlns="http://activemq.apache.org/schema/activemq/apollo">
      ...
      <web_admin bind="http://0.0.0.0:61680?cors_origin=*"/>
      <web_admin bind="https://0.0.0.0:61681?cors_origin=www.foo.com,bar.com"/>
      ...
    </broker>
    

Managing Brokers
----------------

The rest of this section's example assume that you have created a broker instance under the `/var/lib/mybroker` directory or `c:\mybroker` directory if you're on Windows.

### Running a Broker Instance in the Foreground

To start the broker instance in the foreground simply execute `bin/apollo-broker run`. Example:

    /var/lib/mybroker/bin/apollo-broker run

To stop it, press `Ctrl-C` to send the termination signal to the process.

### Managing a Background Broker Instance

#### On Linux/Unix

If you are on Unix, you can use the `bin/apollo-broker-service` script to manage the broker service. This script is compatible with the `/etc/init.d` style scripts most Unix/Linux systems use to control background services.

On a Ubuntu OS, you install the service and have it run on start up by running:

    sudo ln -s /var/lib/mybroker/bin/apollo-broker-service /etc/init.d/apollo
    sudo update-rc.d apollo defaults

On a Redhat OS, you install the service and have it run on start up by running:

    sudo ln -s /var/lib/mybroker/bin/apollo-broker-service /etc/init.d/apollo
    sudo chkconfig apollo --add

On other Unixes, you install the service and have it run on start up by running:

    sudo ln -s /var/lib/mybroker/bin/apollo-broker-service /etc/init.d/apollo
    sudo ln -s /etc/init.d/apollo /etc/rc0.d/K80apollo
    sudo ln -s /etc/init.d/apollo /etc/rc1.d/K80apollo
    sudo ln -s /etc/init.d/apollo /etc/rc3.d/S20apollo
    sudo ln -s /etc/init.d/apollo /etc/rc5.d/S20apollo
    sudo ln -s /etc/init.d/apollo /etc/rc6.d/K80apollo

You can directly use the script to perform the following functions:

*   starting: `apollo-broker-service start`
*   stopping: `apollo-broker-service stop`
*   restarting: `apollo-broker-service restart`
*   checking the status: `apollo-broker-service status`

When the broker is started in the background, it creates a `data/apollo.pid` file which contains the process id of the process executing the broker. This file is typically used to integrate with an external watch dog process such as [Monit](http://mmonit.com/monit/).

#### On Windows

Windows users can install the broker as background service using the `bin\apollo-broker-service` executable.

To install as a background service you would execute:

    c:\mybroker\bin\apollo-broker-service install

Uninstalling the service is done using:

    c:\mybroker\bin\apollo-broker-service uninstall

You can now use the standard Windows service tools to control starting and stopping the broker or you can also use the same executable to:

*   start the service: `apollo-broker-service start`
*   stop the service: `apollo-broker-service stop`
*   check the status: `apollo-broker-service restart`

If you want to customize the JVM options use to start the background service, you will need to edit the `bin\apollo-broker-service.xml` file. All service console and event logging perform by the background service are stored under `log\apollo-broker-service.*`.

### Viewing Broker State

Apollo provides a web based interface for administrators to inspect the runtime state of the Broker. If you're running the broker on your local machine, just open your web browser to [`http://localhost:61680`](http://localhost:61680) or [`https://localhost:61681`](httsp://localhost:61681).

The web interface will display the status of the the connectors and show attached connections. It will also allow you to drill into each configured virtual host and view the topics and queues being used.

Please see the [Management API](management-api.md) documentation for more information on how to use the web based interface as a RESTful API.

### Exporting/Importing Stores

Exporting compresses all the data in a virtual host's message store in a zip archive. Importing reverses the export process and restore the exported data back to a virtual host's message store. Exporting/Importing is typically used to:

*   backup a message store
*   migrate between different message store implementations

The stores which support exporting and importing are:

*   [BDB Store](#bdb-store)
*   [LevelDB Store](#leveldb-store)

The broker must be stopped before using the import or export commands. Be careful when importing an archive, since it will first purge the message store of any data!

Use the `apollo-broker store-export` command to export the data. For example:

    /var/lib/mybroker/bin/apollo-broker store-export myarchive.tgz

The above command will load the `mybroker`'s configuration and export the first virtual host's messages store to the `myarchive.tgz`. You can use the `--virtual-host` command line option to be more specific of which virtual host you wish to export.

Use the `apollo-broker store-import` command to import the data. For example:

    /var/lib/mybroker/bin/apollo-broker store-import myarchive.tgz

Just like in the case of the `store-export` command, it will load the `mybroker`'s configuration and import the archive into the first virtual host's message store.

Messaging Protocols Manuals
---------------------------

*   [STOMP Protocol Manual](stomp-manual)
*   AMQP Protocol Manual
*   [OpenWire Protocol Manual](openwire-manual)
*   [MQTT Protocol Manual](mqtt-manual)