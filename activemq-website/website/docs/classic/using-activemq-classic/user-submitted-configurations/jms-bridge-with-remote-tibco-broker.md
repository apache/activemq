---
title: "JMS Bridge With Remote TIBCO Broker"
description: "Bridge ActiveMQ Classic queues to a remote TIBCO EMS server using jmsBridgeConnectors and a TibjmsQueueConnectionFactory with inbound and outbound bridges."
slug: /jms-bridge-with-remote-tibco-broker
---


### Example of a configuration that shows how to use a BridgeConnector to make a connection to a remote TIBCO EMS broker.
```
<beans>
    <!-- Allows us to use system properties as variables in this configuration file -->
    <bean class="org.springframework.beans.factory.config.PropertyPlaceholderConfigurer"/>

    <broker useJmx="true" persistent="false" xmlns="http://activemq.org/config/1.0"
            brokerName="localhost" dataDirectory="${activemq.base}/data">

        <!-- Dependencies: tibjms.jar must be in the activemq lib directory -->

        <!-- bridging definitions for traffic to/from remote activemq instance -->
        <jmsBridgeConnectors>
            <jmsQueueConnector outboundQueueConnectionFactory="#tibco">
                <inboundQueueBridges>
                    <inboundQueueBridge
                            inboundQueueName = "queue.ping"
                            localQueueName   = "queue.incomingMsgs.ping"
                            selector         = "JMSType LIKE 'foo%'"/>
                </inboundQueueBridges>
                <outboundQueueBridges>
                    <outboundQueueBridge
                            outboundQueueName = "queue.pong"
                            localQueueName    = "queue.incomingMsgs.ping"
                            selector          = "JMSType LIKE 'foo%'"/>
                 </outboundQueueBridges>
            </jmsQueueConnector>
        </jmsBridgeConnectors>
    </broker>

    <!-- JMS ConnectionFactory to use for local bridging -->
    <bean id="tibco" class="com.tibco.tibjms.TibjmsQueueConnectionFactory">
        <property name="serverUrl" value="tcp://remote_tibco:7222" />
    </bean>
</beans>
```
