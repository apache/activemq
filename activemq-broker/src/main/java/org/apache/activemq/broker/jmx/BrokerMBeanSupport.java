/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.broker.jmx;

import java.util.Hashtable;

import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import org.apache.activemq.broker.region.policy.AbortSlowConsumerStrategy;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ConsumerInfo;
import org.apache.activemq.command.ProducerInfo;
import org.apache.activemq.transaction.XATransaction;
import org.apache.activemq.util.JMXSupport;

public class BrokerMBeanSupport {

    // MBean Name Creation

    public static ObjectName createBrokerObjectName(String jmxDomainName, String brokerName) throws MalformedObjectNameException  {
        String objectNameStr = jmxDomainName + ":type=Broker,brokerName=";
        objectNameStr += JMXSupport.encodeObjectNamePart(brokerName);
        return new ObjectName(objectNameStr);
    }

    public static ObjectName createDestinationName(ObjectName brokerObjectName, ActiveMQDestination destination) throws MalformedObjectNameException {
        return createDestinationName(brokerObjectName.toString(), destination);
    }

    public static ObjectName createDestinationName(String brokerObjectName, ActiveMQDestination destination) throws MalformedObjectNameException {
        String objectNameStr = brokerObjectName;
        objectNameStr += createDestinationProperties(destination);
        return new ObjectName(objectNameStr);
    }

    public static ObjectName createDestinationName(String brokerObjectName, String type, String name) throws MalformedObjectNameException {
        String objectNameStr = brokerObjectName;
        objectNameStr += createDestinationProperties(type, name);
        return new ObjectName(objectNameStr);
    }

    private static String createDestinationProperties(ActiveMQDestination destination){
        String result = "";
        if (destination != null){
            result = createDestinationProperties(destination.getDestinationTypeAsString(), destination.getPhysicalName());
        }
        return result;
    }

    private static String createDestinationProperties(String type, String name){
        return ",destinationType="+ JMXSupport.encodeObjectNamePart(type) +
               ",destinationName=" + JMXSupport.encodeObjectNamePart(name);
    }

    public static ObjectName createSubscriptionName(ObjectName brokerObjectName, String connectionClientId, ConsumerInfo info) throws MalformedObjectNameException {
        return createSubscriptionName(brokerObjectName.toString(), connectionClientId, info);
    }

    public static ObjectName createSubscriptionName(String brokerObjectName, String connectionClientId, ConsumerInfo info) throws MalformedObjectNameException {
        String objectNameStr = brokerObjectName;
        objectNameStr += createDestinationProperties(info.getDestination()) + ",endpoint=Consumer";
        objectNameStr += ",clientId=" + JMXSupport.encodeObjectNamePart(connectionClientId);
        objectNameStr += ",consumerId=";

        if (info.isDurable()){
            objectNameStr += "Durable(" + JMXSupport.encodeObjectNamePart(connectionClientId + ":" + info.getSubscriptionName()) +")";
        } else {
            objectNameStr += JMXSupport.encodeObjectNamePart(info.getConsumerId().toString());
        }

        return new ObjectName(objectNameStr);
    }

    public static ObjectName createProducerName(ObjectName brokerObjectName, String connectionClientId, ProducerInfo info) throws MalformedObjectNameException {
        return createProducerName(brokerObjectName.toString(), connectionClientId, info);
    }

    public static ObjectName createProducerName(String brokerObjectName, String connectionClientId, ProducerInfo producerInfo) throws MalformedObjectNameException {
        String objectNameStr = brokerObjectName;

        if (producerInfo.getDestination() == null) {
            objectNameStr += ",endpoint=dynamicProducer";
        } else {
            objectNameStr += createDestinationProperties(producerInfo.getDestination()) + ",endpoint=Producer";
        }

        objectNameStr += ",clientId=" + JMXSupport.encodeObjectNamePart(connectionClientId);
        objectNameStr += ",producerId=" + JMXSupport.encodeObjectNamePart(producerInfo.getProducerId().toString());

        return new ObjectName(objectNameStr);
    }

    public static ObjectName createXATransactionName(ObjectName brokerObjectName, XATransaction transaction) throws MalformedObjectNameException {
        return createXATransactionName(brokerObjectName.toString(), transaction);
    }

    public static ObjectName createXATransactionName(String brokerObjectName, XATransaction transaction) throws MalformedObjectNameException {
        String objectNameStr = brokerObjectName;

        objectNameStr += "," + "transactionType=RecoveredXaTransaction";
        objectNameStr += "," + "xid=" + JMXSupport.encodeObjectNamePart(transaction.getTransactionId().toString());

        return new ObjectName(objectNameStr);
    }

    public static ObjectName createLog4JConfigViewName(String brokerObjectName) throws MalformedObjectNameException {
        String objectNameStr = brokerObjectName;

        objectNameStr += "," + "service=Log4JConfiguration";

        return new ObjectName(objectNameStr);
    }

    public static ObjectName createPersistenceAdapterName(String brokerObjectName, String name) throws MalformedObjectNameException {
        String objectNameStr = brokerObjectName;

        objectNameStr += "," + "service=PersistenceAdapter";
        objectNameStr += "," + "instanceName=" + JMXSupport.encodeObjectNamePart(name);

        return new ObjectName(objectNameStr);
    }

    public static ObjectName createAbortSlowConsumerStrategyName(ObjectName brokerObjectName, AbortSlowConsumerStrategy strategy) throws MalformedObjectNameException {
        return createAbortSlowConsumerStrategyName(brokerObjectName.toString(), strategy);
    }

    public static ObjectName createAbortSlowConsumerStrategyName(String brokerObjectName, AbortSlowConsumerStrategy strategy) throws MalformedObjectNameException {
        String objectNameStr = brokerObjectName;
        objectNameStr += ",service=SlowConsumerStrategy,instanceName="+ JMXSupport.encodeObjectNamePart(strategy.getName());
        ObjectName objectName = new ObjectName(objectNameStr);
        return objectName;
    }

    public static ObjectName createConnectorName(ObjectName brokerObjectName, String type, String name) throws MalformedObjectNameException {
        return createConnectorName(brokerObjectName.toString(), type, name);
    }

    public static ObjectName createConnectorName(String brokerObjectName, String type, String name) throws MalformedObjectNameException {
        String objectNameStr = brokerObjectName;
        objectNameStr += ",connector=" + type + ",connectorName="+ JMXSupport.encodeObjectNamePart(name);
        ObjectName objectName = new ObjectName(objectNameStr);
        return objectName;
    }

    public static ObjectName createNetworkConnectorName(ObjectName brokerObjectName, String type, String name) throws MalformedObjectNameException {
        return createNetworkConnectorName(brokerObjectName.toString(), type, name);
    }

    public static ObjectName createVirtualDestinationSelectorCacheName(ObjectName brokerObjectName, String type, String name) throws MalformedObjectNameException {
        String objectNameStr = brokerObjectName.toString();
        objectNameStr += ",service=" + type + ",virtualDestinationSelectoCache="+ JMXSupport.encodeObjectNamePart(name);
        ObjectName objectName = new ObjectName(objectNameStr);
        return objectName;
    }

    public static ObjectName createNetworkConnectorName(String brokerObjectName, String type, String name) throws MalformedObjectNameException {
        String objectNameStr = brokerObjectName;
        objectNameStr += ",connector=" + type + ",networkConnectorName="+ JMXSupport.encodeObjectNamePart(name);
        ObjectName objectName = new ObjectName(objectNameStr);
        return objectName;
    }

    public static ObjectName createConnectionViewByType(ObjectName connectorName, String type, String name) throws MalformedObjectNameException {
        String objectNameStr = connectorName.toString();
        objectNameStr += ",connectionViewType=" + JMXSupport.encodeObjectNamePart(type);
        objectNameStr += ",connectionName="+ JMXSupport.encodeObjectNamePart(name);
        return new ObjectName(objectNameStr);
    }

    public static ObjectName createNetworkBridgeObjectName(ObjectName connectorName, String remoteAddress) throws MalformedObjectNameException {
        Hashtable<String, String> map = new Hashtable<String, String>(connectorName.getKeyPropertyList());
        map.put("networkBridge", JMXSupport.encodeObjectNamePart(remoteAddress));
        return new ObjectName(connectorName.getDomain(), map);
    }

    public static ObjectName createNetworkOutBoundDestinationObjectName(ObjectName networkName, ActiveMQDestination destination) throws MalformedObjectNameException {
        String str = networkName.toString();
        str += ",direction=outbound" + createDestinationProperties(destination);
        return new ObjectName(str);

    }

    public static ObjectName createNetworkInBoundDestinationObjectName(ObjectName networkName, ActiveMQDestination destination) throws MalformedObjectNameException {
        String str = networkName.toString();
        str += ",direction=inbound" + createDestinationProperties(destination);
        return new ObjectName(str);

    }


    public static ObjectName createProxyConnectorName(ObjectName brokerObjectName, String type, String name) throws MalformedObjectNameException {
        return createProxyConnectorName(brokerObjectName.toString(), type, name);
    }

    public static ObjectName createProxyConnectorName(String brokerObjectName, String type, String name) throws MalformedObjectNameException {
        String objectNameStr = brokerObjectName;
        objectNameStr += ",connector=" + type + ",proxyConnectorName="+ JMXSupport.encodeObjectNamePart(name);
        ObjectName objectName = new ObjectName(objectNameStr);
        return objectName;
    }

    public static ObjectName createJmsConnectorName(ObjectName brokerObjectName, String type, String name) throws MalformedObjectNameException {
        return createJmsConnectorName(brokerObjectName.toString(), type, name);
    }

    public static ObjectName createJmsConnectorName(String brokerObjectName, String type, String name) throws MalformedObjectNameException {
        String objectNameStr = brokerObjectName;
        objectNameStr += ",connector=" + type + ",jmsConnectors="+ JMXSupport.encodeObjectNamePart(name);
        ObjectName objectName = new ObjectName(objectNameStr);
        return objectName;
    }

    public static ObjectName createJobSchedulerServiceName(ObjectName brokerObjectName) throws MalformedObjectNameException {
        return createJobSchedulerServiceName(brokerObjectName.toString());
    }

    public static ObjectName createJobSchedulerServiceName(String brokerObjectName) throws MalformedObjectNameException {
        String objectNameStr = brokerObjectName;
        objectNameStr += ",service=JobScheduler,name=JMS";
        ObjectName objectName = new ObjectName(objectNameStr);
        return objectName;
    }

    public static ObjectName createHealthServiceName(ObjectName brokerObjectName) throws MalformedObjectNameException {
        return createHealthServiceName(brokerObjectName.toString());
    }

    public static ObjectName createHealthServiceName(String brokerObjectName) throws MalformedObjectNameException {
        String objectNameStr = brokerObjectName;
        objectNameStr += ",service=Health";
        ObjectName objectName = new ObjectName(objectNameStr);
        return objectName;
    }

    // MBean Query Creation

    public static ObjectName createConnectionQuery(String jmxDomainName, String brokerName, String name) throws MalformedObjectNameException {
        ObjectName brokerMBeanName = createBrokerObjectName(jmxDomainName, brokerName);
        return createConnectionQuery(brokerMBeanName.toString(), name);
    }

    public static ObjectName createConnectionQuery(String brokerMBeanName, String name) throws MalformedObjectNameException {
        return new ObjectName(brokerMBeanName + ","
                              + "connector=*," + "connectorName=*," + "connectionViewType=*,"
                              + "connectionName=" + JMXSupport.encodeObjectNamePart(name));
    }

    public static ObjectName createQueueQuery(String brokerMBeanName) throws MalformedObjectNameException {
        return createConnectionQuery(brokerMBeanName, "*");
    }

    public static ObjectName createQueueQuery(String brokerMBeanName, String destinationName) throws MalformedObjectNameException {
        return new ObjectName(brokerMBeanName + ","
                              + "destinationType=Queue,"
                              + "destinationName=" + JMXSupport.encodeObjectNamePart(destinationName));
    }

    public static ObjectName createTopicQuery(String brokerMBeanName) throws MalformedObjectNameException {
        return createConnectionQuery(brokerMBeanName, "*");
    }

    public static ObjectName createTopicQuery(String brokerMBeanName, String destinationName) throws MalformedObjectNameException {
        return new ObjectName(brokerMBeanName + ","
                              + "destinationType=Topic,"
                              + "destinationName=" + JMXSupport.encodeObjectNamePart(destinationName));
    }

    public static ObjectName createConsumerQueury(String jmxDomainName, String clientId) throws MalformedObjectNameException {
        return createConsumerQueury(jmxDomainName, null, clientId);
    }

    public static ObjectName createConsumerQueury(String jmxDomainName, String brokerName, String clientId) throws MalformedObjectNameException {
        return new ObjectName(jmxDomainName + ":type=Broker,brokerName="
                              + (brokerName != null ? brokerName : "*") + ","
                              + "destinationType=*,destinationName=*,"
                              + "endpoint=Consumer,"
                              + "clientId=" + JMXSupport.encodeObjectNamePart(clientId) + ","
                              + "consumerId=*");
    }

    public static ObjectName createProducerQueury(String jmxDomainName, String clientId) throws MalformedObjectNameException {
        return createProducerQueury(jmxDomainName, null, clientId);
    }

    public static ObjectName createProducerQueury(String jmxDomainName, String brokerName, String clientId) throws MalformedObjectNameException {
        return new ObjectName(jmxDomainName + ":type=Broker,brokerName="
                + (brokerName != null ? brokerName : "*") + ","
                + "destinationType=*,destinationName=*,"
                + "endpoint=Producer,"
                + "clientId=" + JMXSupport.encodeObjectNamePart(clientId) + ","
                + "producerId=*");
    }

}
