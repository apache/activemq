---
title: "OSGi support (in development)"
slug: /osgi-support-in-development
---


Split packages (2015-11-17)
---------------------------

Package|Modules
---|---
org/apache/activemq/artemis/uri|artemis-core-client, artemis-jms-client, artemis-server
org/apache/activemq/artemis/core/protocol/core/impl/wireformat|artemis-core-client, artemis-server
org/apache/activemq/artemis/spi/core/protocol    |artemis-core-client, artemis-server
org/apache/activemq/artemis/core/protocol/core/impl|artemis-core-client, artemis-server
org/apache/activemq/artemis/core/transaction/impl|artemis-core-client, artemis-server
org/apache/activemq/artemis/core/security|artemis-core-client, artemis-server
org/apache/activemq/artemis/core/protocol|artemis-core-client, artemis-server
org/apache/activemq/artemis/core/buffers/impl|artemis-commons, artemis-core-client
org/apache/activemq/artemis/core/server/management|artemis-core-client, artemis-server
org/apache/activemq/artemis/api/core/management|artemis-core-client, artemis-server
org/apache/activemq/artemis/core/server|artemis-commons (1 class), artemis-core-client (1 class), artemis-server
org/apache/activemq/artemis/spi/core/remoting|artemis-core-client, artemis-server
org/apache/activemq/artemis/core/protocol/core|artemis-core-client, artemis-server
org/apache/activemq/artemis/utils|artemis-commons, artemis-core-client, artemis-server
org/apache/activemq/artemis/api/core|artemis-commons, artemis-core-client
org/apache/activemq/artemis/core/remoting/impl/netty|artemis-core-client, artemis-server
org/apache/activemq/artemis/core/settings|artemis-core-client, artemis-server

Package|Modules
---|---
org/apache/activemq/artemis/core/protocol/stomp|artemis-protocols/artemis-stomp-protocol, artemis-server
org/apache/activemq/artemis/core/protocol/hornetqartemis-protocols/artemis-hornetq-protocol, artemis-protocols/artemis-hqclient-protocol

