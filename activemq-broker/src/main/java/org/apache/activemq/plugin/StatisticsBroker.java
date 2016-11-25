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
package org.apache.activemq.plugin;

import java.io.File;
import java.net.URI;
import java.util.Set;

import javax.jms.JMSException;
import javax.management.ObjectName;

import org.apache.activemq.advisory.AdvisorySupport;
import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.BrokerFilter;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.broker.ProducerBrokerExchange;
import org.apache.activemq.broker.jmx.BrokerViewMBean;
import org.apache.activemq.broker.jmx.SubscriptionViewMBean;
import org.apache.activemq.broker.region.Destination;
import org.apache.activemq.broker.region.DestinationStatistics;
import org.apache.activemq.broker.region.RegionBroker;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQMapMessage;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.command.ProducerId;
import org.apache.activemq.command.ProducerInfo;
import org.apache.activemq.state.ProducerState;
import org.apache.activemq.usage.SystemUsage;
import org.apache.activemq.util.IdGenerator;
import org.apache.activemq.util.LongSequenceGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
/**
 * A StatisticsBroker You can retrieve a Map Message for a Destination - or
 * Broker containing statistics as key-value pairs The message must contain a
 * replyTo Destination - else its ignored
 *
 */
public class StatisticsBroker extends BrokerFilter {
    private static Logger LOG = LoggerFactory.getLogger(StatisticsBroker.class);
    static final String STATS_DESTINATION_PREFIX = "ActiveMQ.Statistics.Destination";
    static final String STATS_BROKER_PREFIX = "ActiveMQ.Statistics.Broker";
    static final String STATS_BROKER_RESET_HEADER = "ActiveMQ.Statistics.Broker.Reset";
    static final String STATS_SUBSCRIPTION_PREFIX = "ActiveMQ.Statistics.Subscription";
    static final String STATS_DENOTE_END_LIST = STATS_DESTINATION_PREFIX + ".List.End.With.Null";
    private static final IdGenerator ID_GENERATOR = new IdGenerator();
    private final LongSequenceGenerator messageIdGenerator = new LongSequenceGenerator();
    protected final ProducerId advisoryProducerId = new ProducerId();
    protected BrokerViewMBean brokerView;

    /**
     *
     * Constructor
     *
     * @param next
     */
    public StatisticsBroker(Broker next) {
        super(next);
        this.advisoryProducerId.setConnectionId(ID_GENERATOR.generateId());
    }

    /**
     * Sets the persistence mode
     *
     * @see org.apache.activemq.broker.BrokerFilter#send(org.apache.activemq.broker.ProducerBrokerExchange,
     *      org.apache.activemq.command.Message)
     */
    @Override
    public void send(ProducerBrokerExchange producerExchange, Message messageSend) throws Exception {
        ActiveMQDestination msgDest = messageSend.getDestination();
        ActiveMQDestination replyTo = messageSend.getReplyTo();
        if (replyTo != null) {
            String physicalName = msgDest.getPhysicalName();
            boolean destStats = physicalName.regionMatches(true, 0, STATS_DESTINATION_PREFIX, 0,
                    STATS_DESTINATION_PREFIX.length());
            boolean brokerStats = physicalName.regionMatches(true, 0, STATS_BROKER_PREFIX, 0, STATS_BROKER_PREFIX
                    .length());
            boolean subStats = physicalName.regionMatches(true, 0, STATS_SUBSCRIPTION_PREFIX, 0, STATS_SUBSCRIPTION_PREFIX
                    .length());
            BrokerService brokerService = getBrokerService();
            RegionBroker regionBroker = (RegionBroker) brokerService.getRegionBroker();
            if (destStats) {
                String destinationName = physicalName.substring(STATS_DESTINATION_PREFIX.length(), physicalName.length());
                if (destinationName.startsWith(".")) {
                    destinationName = destinationName.substring(1);
                }
                String destinationQuery = destinationName.replace(STATS_DENOTE_END_LIST,"");
                boolean endListMessage = !destinationName.equals(destinationQuery);
                ActiveMQDestination queryDestination = ActiveMQDestination.createDestination(destinationQuery,msgDest.getDestinationType());
                Set<Destination> destinations = getDestinations(queryDestination);

                for (Destination dest : destinations) {
                    DestinationStatistics stats = dest.getDestinationStatistics();
                    if (stats != null) {
                        ActiveMQMapMessage statsMessage = new ActiveMQMapMessage();
                        statsMessage.setString("brokerName", regionBroker.getBrokerName());
                        statsMessage.setString("brokerId", regionBroker.getBrokerId().toString());
                        statsMessage.setString("destinationName", dest.getActiveMQDestination().toString());
                        statsMessage.setLong("size", stats.getMessages().getCount());
                        statsMessage.setLong("enqueueCount", stats.getEnqueues().getCount());
                        statsMessage.setLong("dequeueCount", stats.getDequeues().getCount());
                        statsMessage.setLong("dispatchCount", stats.getDispatched().getCount());
                        statsMessage.setLong("expiredCount", stats.getExpired().getCount());
                        statsMessage.setLong("inflightCount", stats.getInflight().getCount());
                        statsMessage.setLong("messagesCached", stats.getMessagesCached().getCount());
                        // we are okay with the size without decimals so cast to long
                        statsMessage.setLong("averageMessageSize", (long) stats.getMessageSize().getAverageSize());
                        statsMessage.setInt("memoryPercentUsage", dest.getMemoryUsage().getPercentUsage());
                        statsMessage.setLong("memoryUsage", dest.getMemoryUsage().getUsage());
                        statsMessage.setLong("memoryLimit", dest.getMemoryUsage().getLimit());
                        statsMessage.setDouble("averageEnqueueTime", stats.getProcessTime().getAverageTime());
                        statsMessage.setDouble("maxEnqueueTime", stats.getProcessTime().getMaxTime());
                        statsMessage.setDouble("minEnqueueTime", stats.getProcessTime().getMinTime());
                        statsMessage.setLong("consumerCount", stats.getConsumers().getCount());
                        statsMessage.setLong("producerCount", stats.getProducers().getCount());
                        statsMessage.setJMSCorrelationID(messageSend.getCorrelationId());
                        sendStats(producerExchange.getConnectionContext(), statsMessage, replyTo);
                    }
                }
                if(endListMessage){
                    ActiveMQMapMessage statsMessage = new ActiveMQMapMessage();
                    statsMessage.setJMSCorrelationID(messageSend.getCorrelationId());
                    sendStats(producerExchange.getConnectionContext(),statsMessage,replyTo);
                }

            } else if (subStats) {
                sendSubStats(producerExchange.getConnectionContext(), getBrokerView().getQueueSubscribers(), replyTo);
                sendSubStats(producerExchange.getConnectionContext(), getBrokerView().getTopicSubscribers(), replyTo);
            } else if (brokerStats) {

                if (messageSend.getProperties().containsKey(STATS_BROKER_RESET_HEADER)) {
                    getBrokerView().resetStatistics();
                }

                ActiveMQMapMessage statsMessage = new ActiveMQMapMessage();
                SystemUsage systemUsage = brokerService.getSystemUsage();
                DestinationStatistics stats = regionBroker.getDestinationStatistics();
                statsMessage.setString("brokerName", regionBroker.getBrokerName());
                statsMessage.setString("brokerId", regionBroker.getBrokerId().toString());
                statsMessage.setLong("size", stats.getMessages().getCount());
                statsMessage.setLong("enqueueCount", stats.getEnqueues().getCount());
                statsMessage.setLong("dequeueCount", stats.getDequeues().getCount());
                statsMessage.setLong("dispatchCount", stats.getDispatched().getCount());
                statsMessage.setLong("expiredCount", stats.getExpired().getCount());
                statsMessage.setLong("inflightCount", stats.getInflight().getCount());
                // we are okay with the size without decimals so cast to long
                statsMessage.setLong("averageMessageSize",(long) stats.getMessageSize().getAverageSize());
                statsMessage.setLong("messagesCached", stats.getMessagesCached().getCount());
                statsMessage.setInt("memoryPercentUsage", systemUsage.getMemoryUsage().getPercentUsage());
                statsMessage.setLong("memoryUsage", systemUsage.getMemoryUsage().getUsage());
                statsMessage.setLong("memoryLimit", systemUsage.getMemoryUsage().getLimit());
                statsMessage.setInt("storePercentUsage", systemUsage.getStoreUsage().getPercentUsage());
                statsMessage.setLong("storeUsage", systemUsage.getStoreUsage().getUsage());
                statsMessage.setLong("storeLimit", systemUsage.getStoreUsage().getLimit());
                statsMessage.setInt("tempPercentUsage", systemUsage.getTempUsage().getPercentUsage());
                statsMessage.setLong("tempUsage", systemUsage.getTempUsage().getUsage());
                statsMessage.setLong("tempLimit", systemUsage.getTempUsage().getLimit());
                statsMessage.setDouble("averageEnqueueTime", stats.getProcessTime().getAverageTime());
                statsMessage.setDouble("maxEnqueueTime", stats.getProcessTime().getMaxTime());
                statsMessage.setDouble("minEnqueueTime", stats.getProcessTime().getMinTime());
                statsMessage.setLong("consumerCount", stats.getConsumers().getCount());
                statsMessage.setLong("producerCount", stats.getProducers().getCount());
                String answer = brokerService.getTransportConnectorURIsAsMap().get("tcp");
                answer = answer != null ? answer : "";
                statsMessage.setString("openwire", answer);
                answer = brokerService.getTransportConnectorURIsAsMap().get("stomp");
                answer = answer != null ? answer : "";
                statsMessage.setString("stomp", answer);
                answer = brokerService.getTransportConnectorURIsAsMap().get("ssl");
                answer = answer != null ? answer : "";
                statsMessage.setString("ssl", answer);
                answer = brokerService.getTransportConnectorURIsAsMap().get("stomp+ssl");
                answer = answer != null ? answer : "";
                statsMessage.setString("stomp+ssl", answer);
                URI uri = brokerService.getVmConnectorURI();
                answer = uri != null ? uri.toString() : "";
                statsMessage.setString("vm", answer);
                File file = brokerService.getDataDirectoryFile();
                answer = file != null ? file.getCanonicalPath() : "";
                statsMessage.setString("dataDirectory", answer);
                statsMessage.setJMSCorrelationID(messageSend.getCorrelationId());
                sendStats(producerExchange.getConnectionContext(), statsMessage, replyTo);
            } else {
                super.send(producerExchange, messageSend);
            }
        } else {
            super.send(producerExchange, messageSend);
        }
    }

    BrokerViewMBean getBrokerView() throws Exception {
        if (this.brokerView == null) {
            ObjectName brokerName = getBrokerService().getBrokerObjectName();
            this.brokerView = (BrokerViewMBean) getBrokerService().getManagementContext().newProxyInstance(brokerName,
                    BrokerViewMBean.class, true);
        }
        return this.brokerView;
    }

    @Override
    public void start() throws Exception {
        super.start();
        LOG.info("Starting StatisticsBroker");
    }

    @Override
    public void stop() throws Exception {
        super.stop();
    }

    protected void sendSubStats(ConnectionContext context, ObjectName[] subscribers, ActiveMQDestination replyTo) throws Exception {
        for (int i = 0; i < subscribers.length; i++) {
            ObjectName name = subscribers[i];
            SubscriptionViewMBean subscriber = (SubscriptionViewMBean)getBrokerService().getManagementContext().newProxyInstance(name, SubscriptionViewMBean.class, true);
            ActiveMQMapMessage statsMessage = prepareSubscriptionMessage(subscriber);
            sendStats(context, statsMessage, replyTo);
        }
    }

    protected ActiveMQMapMessage prepareSubscriptionMessage(SubscriptionViewMBean subscriber) throws JMSException {
        Broker regionBroker = getBrokerService().getRegionBroker();
        ActiveMQMapMessage statsMessage = new ActiveMQMapMessage();
        statsMessage.setString("brokerName", regionBroker.getBrokerName());
        statsMessage.setString("brokerId", regionBroker.getBrokerId().toString());
        statsMessage.setString("destinationName", subscriber.getDestinationName());
        statsMessage.setString("clientId", subscriber.getClientId());
        statsMessage.setString("connectionId", subscriber.getConnectionId());
        statsMessage.setLong("sessionId", subscriber.getSessionId());
        statsMessage.setString("selector", subscriber.getSelector());
        statsMessage.setLong("enqueueCounter", subscriber.getEnqueueCounter());
        statsMessage.setLong("dequeueCounter", subscriber.getDequeueCounter());
        statsMessage.setLong("dispatchedCounter", subscriber.getDispatchedCounter());
        statsMessage.setLong("dispatchedQueueSize", subscriber.getDispatchedQueueSize());
        statsMessage.setInt("prefetchSize", subscriber.getPrefetchSize());
        statsMessage.setInt("maximumPendingMessageLimit", subscriber.getMaximumPendingMessageLimit());
        statsMessage.setBoolean("exclusive", subscriber.isExclusive());
        statsMessage.setBoolean("retroactive", subscriber.isRetroactive());
        statsMessage.setBoolean("slowConsumer", subscriber.isSlowConsumer());
        return statsMessage;
    }

    protected void sendStats(ConnectionContext context, ActiveMQMapMessage msg, ActiveMQDestination replyTo)
            throws Exception {
        msg.setPersistent(false);
        msg.setTimestamp(System.currentTimeMillis());
        msg.setPriority((byte) javax.jms.Message.DEFAULT_PRIORITY);
        msg.setType(AdvisorySupport.ADIVSORY_MESSAGE_TYPE);
        msg.setMessageId(new MessageId(this.advisoryProducerId, this.messageIdGenerator.getNextSequenceId()));
        msg.setDestination(replyTo);
        msg.setResponseRequired(false);
        msg.setProducerId(this.advisoryProducerId);
        boolean originalFlowControl = context.isProducerFlowControl();
        final ProducerBrokerExchange producerExchange = new ProducerBrokerExchange();
        producerExchange.setConnectionContext(context);
        producerExchange.setMutable(true);
        producerExchange.setProducerState(new ProducerState(new ProducerInfo()));
        try {
            context.setProducerFlowControl(false);
            this.next.send(producerExchange, msg);
        } finally {
            context.setProducerFlowControl(originalFlowControl);
        }
    }

}
