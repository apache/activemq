---
title: "TimeStampPlugin"
slug: /timestampplugin
---


The TimeStampPlugin is a Broker interceptor which updates a JMS Client's time stamp on the message with a broker time stamp. This can be useful when the clocks on client machines are known to not be correct and you can only trust the time set on the broker machines.

Enabling this plugin will break JMS compliance since the time stamp that the producer sees on the messages after as `send()` will be different from the time stamp the consumer will observe when he receives the message.

By default this plugin is _not_ enabled in ActiveMQ.

#### Options

Attribute|Default Value|Description
---|---|---
`futureOnly`|`false`|When `true` the plugin will _never_ set a message's time stamp and expiration time to a value lower than the original values. When `false` the plugin _always_ update a message's time stamp and expiration time.
`ttlCeiling`|`0`|When not zero, this value (in ms) limit the expiration time.
`zeroExpirationOverride`|`0`|When not zero this value (in ms) will override the expiration time for messages that do not have an expiration already set.

To enable the TimeStampPlugin add the following to your ActiveMQ Broker configuration.

Example:
```
<plugins>
  <!-- 86,400,000 ms = 1 day -->
  <timeStampingBrokerPlugin ttlCeiling="86400000" zeroExpirationOverride="86400000"/>
</plugins>
```

Broker/Consumer Clock Synchronization

When the consumer’s local clock is running ahead of the broker’s local clock then messages might not be consumed by your consumer when this plug-in is loaded with default configuration as the consumer could perceive the messages as already expired.

If the clock difference between broker and consumer is _greater than_ the message expiration time _and_ if the consumer’s clock is running ahead, then set `futureOnly="true"`.

See the following [blog post](http://tmielke.blogspot.com/2011/01/sync-your-machine-clocks.html) for more details.

