---
title: "Slow networks drop large messages"
slug: /slow-networks-drop-large-messages
---

Note

This article only applies to older versions of ActiveMQ, i.e. 5.3 and 5.4.0. From 5.4.2 onwards this issue has been adressed in the bugs [AMQ-2511](https://issues.apache.org/jira/browse/AMQ-2511) and [AMQ-2088](https://issues.apache.org/jira/browse/AMQ-2088).

You may experience problems sending large messages using networks across slow links.

The symptoms are usually that the network connection will be dropped and continuously re-established.

This problem arises because the ActiveMQ message broker continuously checks for activity on a socket, and whilst building a large message in the TCP socket buffer, the [inactivity](activemq-classic-inactivitymonitor) monitor will time out - see [Configuring Wire Formats](configuring-wire-formats) and the [TCP Transport Reference](tcp-transport-reference).  
The [inactivity monitor](activemq-classic-inactivitymonitor) uses an activity flag that is only updated after a message is received (in case the connection is not idle).  
The monitor then clears the activity flag, waits the timeout amount and peeks at the activity flag. If it's still cleared, then we assume the connection was inactive. When the connection is NOT in use, a keep alive message is sent. But in case the connection is in use because of a larger message being sent/received, no keep alive will be sent and the activity flag will not be updated. That can lead to inactivity timeouts.  
Ideally the activity flag would get cleared every time a few bytes arrive from the network... but right now it is only cleared when an entire message is assembled.

You can either set the wireFormat.maxInactivityDuration property to a high value - or disable it (set it to zero) to work around this issue.

e.g. configure your network connection like this:

<?xml version="1.0" encoding="UTF-8"?>

<beans xmlns="http://activemq.org/config/1.0">

  <broker brokerName="receiver">
    <transportConnectors>
      <transportConnector uri="tcp://localhost:62002"/>
    </transportConnectors>

    <networkConnectors>
      <networkConnector uri="static:(tcp://somehost:62001?wireFormat.maxInactivityDuration=0)"/>
    </networkConnectors>

    <persistenceAdapter>
      <memoryPersistenceAdapter/>
    </persistenceAdapter>
  </broker>

</beans>

### See also

[ActiveMQ InactivityMonitor](activemq-classic-inactivitymonitor)

