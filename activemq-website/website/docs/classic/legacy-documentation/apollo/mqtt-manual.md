---
title: "Apollo 1.7.1 MQTT Protocol Manual"
slug: /mqtt-manual
---

       

Apollo 1.7.1 MQTT Protocol Manual
=================================

*   [The MQTT Protocol](#the-mqtt-protocol)
*   *   [MQTT Protocol Options](#mqtt-protocol-options)
    *   [Client Libraries](#client-libraries)
    *   [Connecting](#connecting)
    *   [Destination Types](#destination-types)
    *   [Clean Sessions](#clean-sessions)
    *   [Topic Retained Messages](#topic-retained-messages)
    *   [Last Will and Testament Message](#last-will-and-testament-message)
    *   [Reliable Messaging](#reliable-messaging)
    *   *   [At Most Once](#at-most-once)
        *   [At Least Once](#at-least-once)
        *   [Exactly Once](#exactly-once)
    *   [Wildcard Subscriptions](#wildcard-subscriptions)
    *   [Keep Alive](#keep-alive)
    *   [Destination Name Restrictions](#destination-name-restrictions)

The MQTT Protocol
-----------------

Apollo allows clients to connect using the MQTT which is an open-source protocol specification intended for limited-resource devices on unreliable networks using a publish-subscribe domain. These types of devices usually require a small footprint and are not well suited for text-based protocols such as HTTP or STOMP or even traditional binary protocols such as Openwire or AMQP. MQTT is a compact binary protocol that is optimized for these types of limited devices and unreliable networks.

In previous releases, MQTT was supported in Apollo as a separate plugin. As of now, that plugin has become part of the main development trunk and MQTT support is available out of the box without any other configuration or packaging of third-party plugins.

Since MQTT is a wire-level protocol, any client that implements the protocol should be able to connect to Apollo and also interoperate with other MQTT-compatibe message brokers.

To learn more about the details of MQTT, see [the MQTT Specification](http://public.dhe.ibm.com/software/dw/webservices/ws-mqtt/mqtt-v3r1.html)

### MQTT Protocol Options

To start using the MQTT protocol, use a valid MQTT v3.1 client and connect to the port on which Apollo is listening. Apollo will do protocol detection and will automatically recognize the MQTT payloads and treat the connection as an MQTT connection. You don't have to open a special port for MQTT (or STOMP, Openwire, AMQP, etc. they can all be auto-detected). To force specific protocols over a certain connector there are two ways you can do this. You can choose to not use protocol detection at all and set the connector to be specifically for mqtt:

    
    <connector id="tcp" bind="tcp://0.0.0.0:61613" protocol="mqtt"/>
    

Alternatively, you can limit which protocols can be “detected” using the `<detect>` configuration element like this:

    
    <connector id="tcp" bind="tcp://0.0.0.0:61613">
      <detect protocols="mqtt openwire" />
    </connector>
    

The `protocols` attribute in the `<detect>` element takes space delimited protoco values. The `protocol` attribtue of the `<connector>` element takes a single protocol, not space delimited. It defaults to `any`

If you wish to tune the MQTT defaults, you can use the `mqtt` configuration element within the `connector` element in the `apollo.xml` configuration file :

    
    <connector id="tcp" bind="tcp://0.0.0.0:61613">
      <mqtt max_message_length="1000" />
    </connector>
    

The `mqtt` element supports the following configuration attributes:

*   `max_message_length` : The size (in bytes) of the largest message that can be sent to the broker. Defaults to 100MB
*   `protocol_filters` : A filter which can filter frames being sent/received to and from a client. It can modify the frame or even drop it.
*   `die_delay` : How long after a connection is deemed to be “dead” before the connection actually closes; default: 5000ms

The mqtt configuration element can also be used to control how the destination headers are parsed and interpreted. The supported attributes are:

*   `queue_prefix` : a tag used to identify destination types; default: null
*   `path_separator` : used to separate segments in a destination name; default: `/`
*   `any_child_wildcard` : indicate all child-level destinations that match the wildcard; default: `+`
*   `any_descendant_wildcard` : indicate destinations that match the wildcard recursively; default: `#`
*   `regex_wildcard_start` : pattern used to identify the start of a regex
*   `regex_wildcard_end` : pattern used to identify the end of a regex
*   `part_pattern` : allows you to specify a regex that constrains the naming of topics. default: `[ a-zA-Z0-9\_-\%\~\:\(\)]+`

### Client Libraries

Apollo supports v3.1 of the MQTT protocol. The following clients will work:

*   Java : [mqtt-client](https://github.com/fusesource/mqtt-client), [MeQanTT](https://github.com/AlbinTheander/MeQanTT)
*   C : [libmosquitto](http://mosquitto.org/man/libmosquitto-3.html)
*   Erlang : [erlmqtt](https://github.com/squaremo/erlmqtt), [my-mqtt4erl](http://code.google.com/p/my-mqtt4erl/)
*   .NET : [MQTTDotNet](http://sourceforge.net/projects/mqttdotnet/), [nMQTT](https://github.com/markallanson/nmqtt)
*   Perl : [net-mqtt-perl](https://github.com/beanz/net-mqtt-perl), [anyevent-mqtt-perl]https://github.com/beanz/anyevent-mqtt-perl()
*   Python : [nyamuk](https://github.com/iwanbk/nyamuk)
*   Ruby : [mqtt-ruby](https://github.com/njh/ruby-mqtt), [ruby-em-mqtt](https://rubygems.org/gems/em-mqtt)
*   Javascript : [Node.js MQTT Client](http://ceit.uq.edu.au/content/simple-mqtt-cient-nodejs)
*   Delphi : [TMQTTCLient](http://jamiei.com/blog/code/mqtt-client-library-for-delphi/)
*   Device specific: [Arduino](http://knolleary.net/arduino-client-for-mqtt/), [mbed](http://ceit.uq.edu.au/content/mqttclient-mbed-version-20), [Nanode](http://github.com/njh/NanodeMQTT/), Netduino

To see an up-to-date listing of client libraries, please [the MQTT website for its software](http://mqtt.org/software) listings

The Apollo distribution ships with an `examples` directory where you can find some simple examples of how to use some of those clients to send and receive messages from a broker instance.

### Connecting

The default broker configuration secures access to the broker so that only the `admin` user can connect. The default password for the `admin` user is `password`.

MQTT clients cannot specify a Virtual Host (see [the section on Virtual Hosts in the user guide](user-manual.md#virtual-hosts)) so the default virtual host will be used. This is usually the first Virtual Host defined in the Apollo.xml configuration file.

### Destination Types

The MQTT protocol is a publish/subscribe protocol. It does not permit true point-to-point messaging achieved using Queues. Therefore Apollo allows only the use of Topics for MQTT messaging. The concept of a subscription and durable subscription to Topics is similar to what you'd find in other protocols and is controlled by the MQTT CONNECT frame's `clean session` attribute.

### Clean Sessions

When a client sends a connect frame with the `clean session` flag set to cleared (false), any previously used session with the same client_id will be re-used. This means while the client was away, that subscription could have received messages. This is the equivalent of a durable subscription in Apollo.

If the `clean session` flag is set (true), then a new session will be started and any sessions tha may have been lingering would be removed. This is equivalent to a normal topic subscription in Apollo.

### Topic Retained Messages

If a message has been published with the retain flag set, then the message will be 'remembered' by the topic so that if a new subscription arrives, the last retained message is sent to the subscription. For example if you're publishing measurements and you want the last mesasurement published to always be available to a client that subscribes to the topic, you can set the retain flag on the PUBLISH frame.

Note: retained messages are not retained between broker restarts for Quality of Service setting of AT MOST ONCE (QoS=0).

### Last Will and Testament Message

You can set a `will` message and assocaited QoS for the message when a client first connects to Apollo. The will message is basically a message that will only get sent if there is an unexpected error with the connection and it must be dropped. This can be useful in situations where you have devices that could drop but when they do, you want to know. So if a medical sensor client drops from the broker, a will message could be sent to an “alarm” Topic and handled by the system as a high-priority alert.

### Reliable Messaging

MQTT allows a client to publish a message with the following Quality of Service parameters (QoS):

*   At Most Once (QoS=0)
*   At Least Once (QoS=1)
*   Exactly Once (QoS=2)

#### At Most Once

This QoS will attempt to deliver the message to a client, but it will have the lowest reliability of the three options. If you publish with a QoS=0, At Most Once, then the broker will not send back an Ack saying it received the message, nor will it retry if the broker fails. This QoS is most similar to non-persistent messages in, for example, JMS.

#### At Least Once

This QoS setting will ensure that the message is delivered at least once to clients. When publishing with this setting, Apollo will send back a PUBACK frame which acknowledges that the broker has received the message and has taken “ownership” for delivering the message. If the client that published the message with QoS=1 does not recieve the PUBACK in a specified period of time, the client may wish to re-publish the message again with the DUP flag set on the PUBLISH frame. It's possible the broker received the first attempt to publish the message and subsequently published it to listening clients. So if the PUBACK got lost somehwere and the client sends the message again, there will be no duplicate detection, and the broker will send the message again to the topic's subscribers.

#### Exactly Once

This QoS is the strongest level of reliability afforded by the MQTT protocol. This assures the publisher that its message will not only get to its intended subscribers, but that the message will not be duplicated as it could with QoS=1. This QoS, however, comes with increased network overhead.

When a message is published, the broker will store the message ID and send the message to the Topic where it would be persisted if there are any durable subscriptions. It will then send the PUBREC frame back to the client implying the broker has received the message. At this point the broker will expect the client to send the PUBREL frame to clear the message ID from its session state and complete the send with the broker sending a PUBCOMP.

### Wildcard Subscriptions

Wild cards can be used in destination names when subscribing as a consumer. This allows you to subscribe to multiple destinations or hierarchy of destinations.

*   `/` is used to separate names in a path
*   `+` is used to match any name in a path
*   `#` is used to recursively match path names

For example using the above, these subscriptions are possible

*   `PRICE/#` : Any price for any product on any exchange
*   `PRICE/STOCK/#` : Any price for a stock on any exchange
*   `PRICE/STOCK/NASDAQ/+` : Any stock price on NASDAQ
*   `PRICE/STOCK/+/IBM` : Any IBM stock price on any exchange

### Keep Alive

Apollo will only set a keep-alive/heart-beat monitor if the client has specified a `keepAlive` value in the CONNECT frame. If one is specified, the actual value used by Apollo will be 1.5 * the keep alive value. This is in keeping with the MQTT spec.

### Destination Name Restrictions

Destination names are restricted to using the characters `a-z`, `A-Z`, `0-9`, `_`, `-` `%`, `~`, `:`, ' ', '(', ')' or `.` in addition to composite separator `,` and the wild card `*`. Any other characters must be UTF-8 and then URL encoded if you wish to preserve their significance.