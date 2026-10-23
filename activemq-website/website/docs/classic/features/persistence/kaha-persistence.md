---
title: "Kaha Persistence"
description: "Learn what Kaha persistence is in ActiveMQ and how to configure the kahaPersistenceAdapter in broker XML for ActiveMQ 5.0 and later or 4.1 and earlier."
slug: /kaha-persistence
---


Kaha Persistence
----------------

Kaha Peristence is a storage solution written especially for message persistence and is part of the ActiveMQ project. It's tuned to provide optimal performance for typical message usage patterns, which involves writing/reading and discarding messages that are persisted very quickly.

Data stored in Kaha is appended to data logs - the log files are discarded once there is no longer interest in the data contained in the log.

### Configuring Kaha Persistence

In the broker XML specify the persistence adaptor to be Kaha e.g.

ActiveMQ 5.0 and above:
```
<broker brokerName="broker" persistent="true" useShutdownHook="false">
  <transportConnectors>
    <transportConnector uri="tcp://localhost:61616"/>
  </transportConnectors>
  <persistenceAdapter>
    <kahaPersistenceAdapter directory="activemq-data" maxDataFileLength="33554432"/>
  </persistenceAdapter>
</broker>
```
ActiveMQ 4.1 and earlier:
```
<broker brokerName="broker" persistent="true" useShutdownHook="false">
  <transportConnectors>
    <transportConnector uri="tcp://localhost:61616"/>
  </transportConnectors>
  <persistenceAdapter>
    <kahaPersistenceAdapter dir="activemq-data" maxDataFileLength="33554432"/>
  </persistenceAdapter>
</broker>
```
