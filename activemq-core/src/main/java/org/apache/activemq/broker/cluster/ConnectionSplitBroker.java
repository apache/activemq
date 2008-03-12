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
package org.apache.activemq.broker.cluster;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.Session;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.advisory.AdvisoryBroker;
import org.apache.activemq.advisory.AdvisorySupport;
import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.BrokerFilter;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.broker.TransportConnection;
import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.broker.region.Subscription;
import org.apache.activemq.command.ActiveMQMessage;
import org.apache.activemq.command.BrokerId;
import org.apache.activemq.command.ConnectionId;
import org.apache.activemq.command.ConnectionInfo;
import org.apache.activemq.command.ConsumerId;
import org.apache.activemq.command.ConsumerInfo;
import org.apache.activemq.command.DataStructure;
import org.apache.activemq.command.Message;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Monitors for client connections that may fail to another broker - but this
 * broker isn't aware they've gone. Can occur with network glitches or client
 * error
 * 
 * @version $Revision$
 */
public class ConnectionSplitBroker extends BrokerFilter implements
        MessageListener {
    private static final Log LOG = LogFactory
            .getLog(ConnectionSplitBroker.class);

    private Connection connection;

    private Map<ConnectionId, ConnectionContext> clientMap = new ConcurrentHashMap<ConnectionId, ConnectionContext>();
    private Map<ConsumerId,ConsumerInfo>consumerMap = new ConcurrentHashMap<ConsumerId,ConsumerInfo>();
    public ConnectionSplitBroker(Broker next) {
        super(next);
    }

    public void addConnection(ConnectionContext context, ConnectionInfo info)
            throws Exception {
        if (info != null) {
            removeStaleConnection(info);
            clientMap.put(info.getConnectionId(), context);
        }
        super.addConnection(context, info);
    }

    public void removeConnection(ConnectionContext context,
            ConnectionInfo info, Throwable error) throws Exception {
        if (info != null) {
            clientMap.remove(info.getConnectionId());
        }
        super.removeConnection(context, info, error);
    }
    
    public Subscription addConsumer(ConnectionContext context, ConsumerInfo info) throws Exception{
       
        if (info.isNetworkSubscription()) {
            List<ConsumerId>list = info.getNetworkConsumerIds();
            for (ConsumerId id:list) {
                consumerMap.put(id,info);
            }
        }else {
            ConsumerInfo networkInfo = consumerMap.get(info.getConsumerId());
            if (networkInfo != null) {
                networkInfo.removeNetworkConsumerId(info.getConsumerId());
                if (networkInfo.isNetworkConsumersEmpty()) {
                    consumerMap.remove(info.getConsumerId());
                    super.removeConsumer(context,networkInfo);
                }
                
            }
        }
        return super.addConsumer(context, info);
    }

   
    public void removeConsumer(ConnectionContext context, ConsumerInfo info) throws Exception{
        if (info.isNetworkSubscription()) {
            List<ConsumerId>list = info.getNetworkConsumerIds();
            for (ConsumerId id:list) {
                consumerMap.remove(id);
            }
        }
        super.removeConsumer(context, info);
    }

    public void start() throws Exception {
        super.start();
        ActiveMQConnectionFactory fac = new ActiveMQConnectionFactory(
                getBrokerService().getVmConnectorURI());
        fac.setCloseTimeout(1);
        fac.setWarnAboutUnstartedConnectionTimeout(10000);
        fac.setWatchTopicAdvisories(false);
        fac.setAlwaysSessionAsync(true);
        fac.setClientID(getBrokerId().toString() + ":" + getBrokerName()
                + ":ConnectionSplitBroker");
        connection = fac.createConnection();
        connection.start();
        Session session = connection.createSession(false,
                Session.AUTO_ACKNOWLEDGE);
        MessageConsumer consumer = session.createConsumer(AdvisorySupport
                .getConnectionAdvisoryTopic());
        consumer.setMessageListener(this);
    }

    public synchronized void stop() throws Exception {
        if (connection != null) {
            connection.stop();
            connection = null;
        }
        super.stop();
    }

    public void onMessage(javax.jms.Message m) {
        ActiveMQMessage message = (ActiveMQMessage) m;

        DataStructure o = message.getDataStructure();
        if (o != null && o.getClass() == ConnectionInfo.class) {
            ConnectionInfo info = (ConnectionInfo) o;
            
            String brokerId = null;
            try {
                brokerId = message
                        .getStringProperty(AdvisorySupport.MSG_PROPERTY_ORIGIN_BROKER_ID);
                if (brokerId != null
                        && !brokerId.equals(getBrokerId().getValue())) {
                    // see if it already exits
                    removeStaleConnection(info);
                }
            } catch (JMSException e) {
                LOG.warn("Failed to get message property "
                        + AdvisorySupport.MSG_PROPERTY_ORIGIN_BROKER_ID, e);
            }

        }

    }

    protected boolean contains(BrokerId[] brokerPath, BrokerId brokerId) {
        if (brokerPath != null) {
            for (int i = 0; i < brokerPath.length; i++) {
                if (brokerId.equals(brokerPath[i])) {
                    return true;
                }
            }
        }
        return false;
    }
    
    protected void removeStaleConnection(ConnectionInfo info) {
     // see if it already exits
        ConnectionContext old = clientMap.remove(info
                .getConnectionId());
        if (old != null && old.getConnection() != null) {
            String str = "connectionId=" + old.getConnectionId()
                    + ",clientId=" + old.getClientId();
            LOG.warn("Removing stale connection: " + str);
            try {
                // remove connection states
                TransportConnection connection = (TransportConnection) old
                        .getConnection();
                connection.processRemoveConnection(old
                        .getConnectionId());
                connection.stopAsync();
            } catch (Exception e) {
                LOG.error("Failed to remove stale connection: "
                        + str, e);
            }
        }
    }

}
