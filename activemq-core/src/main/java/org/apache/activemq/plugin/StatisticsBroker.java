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

import org.apache.activemq.advisory.AdvisorySupport;
import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.BrokerFilter;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.broker.ProducerBrokerExchange;
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
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import java.io.File;
import java.net.URI;
import java.util.Set;
/**
 * A StatisticsBroker You can retrieve a Map Message for a Destination - or
 * Broker containing statistics as key-value pairs The message must contain a
 * replyTo Destination - else its ignored
 * 
 */
public class StatisticsBroker extends BrokerFilter {
    private static Log LOG = LogFactory.getLog(StatisticsBroker.class);
    static final String STATS_DESTINATION_PREFIX = "ActiveMQ.Statistics.Destination";
    static final String STATS_BROKER_PREFIX = "ActiveMQ.Statistics.Broker";
    private static final IdGenerator ID_GENERATOR = new IdGenerator();
    private final LongSequenceGenerator messageIdGenerator = new LongSequenceGenerator();
    protected final ProducerId advisoryProducerId = new ProducerId();

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
    public void send(ProducerBrokerExchange producerExchange, Message messageSend) throws Exception {
        ActiveMQDestination msgDest = messageSend.getDestination();
        ActiveMQDestination replyTo = messageSend.getReplyTo();
        if (replyTo != null) {
            String physicalName = msgDest.getPhysicalName();
            boolean destStats = physicalName.regionMatches(true, 0, STATS_DESTINATION_PREFIX, 0,
                    STATS_DESTINATION_PREFIX.length());
            boolean brokerStats = physicalName.regionMatches(true, 0, STATS_BROKER_PREFIX, 0, STATS_BROKER_PREFIX
                    .length());
            if (destStats) {
                String queueryName = physicalName.substring(STATS_DESTINATION_PREFIX.length(), physicalName.length());
                ActiveMQDestination queryDest = ActiveMQDestination.createDestination(queueryName,msgDest.getDestinationType());
                Set<Destination> set = getDestinations(queryDest);
                for (Destination dest : set) {
                    DestinationStatistics stats = dest.getDestinationStatistics();
                    if (stats != null) {
                        ActiveMQMapMessage statsMessage = new ActiveMQMapMessage();
                        statsMessage.setString("destinationName", dest.getActiveMQDestination().toString());
                        statsMessage.setLong("size", stats.getMessages().getCount());
                        statsMessage.setLong("enqueueCount", stats.getEnqueues().getCount());
                        statsMessage.setLong("dequeueCount", stats.getDequeues().getCount());
                        statsMessage.setLong("dispatchCount", stats.getDispatched().getCount());
                        statsMessage.setLong("expiredCount", stats.getExpired().getCount());
                        statsMessage.setLong("inflightCount", stats.getInflight().getCount());
                        statsMessage.setLong("messagesCached", stats.getMessagesCached().getCount());
                        statsMessage.setInt("memoryPercentUsage", dest.getMemoryUsage().getPercentUsage());
                        statsMessage.setLong("memoryUsage", dest.getMemoryUsage().getUsage());
                        statsMessage.setLong("memoryLimit", dest.getMemoryUsage().getLimit());
                        statsMessage.setDouble("averageEnqueueTime", stats.getProcessTime().getAverageTime());
                        statsMessage.setDouble("maxEnqueueTime", stats.getProcessTime().getMaxTime());
                        statsMessage.setDouble("minEnqueueTime", stats.getProcessTime().getMinTime());
                        statsMessage.setLong("consumerCount", stats.getConsumers().getCount());
                        statsMessage.setLong("producerCount", stats.getProducers().getCount());
                        sendStats(producerExchange.getConnectionContext(), statsMessage, replyTo);
                    }
                }
            } else if (brokerStats) {
                ActiveMQMapMessage statsMessage = new ActiveMQMapMessage();
                BrokerService brokerService = getBrokerService();
                RegionBroker regionBroker = (RegionBroker) brokerService.getRegionBroker();
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
                sendStats(producerExchange.getConnectionContext(), statsMessage, replyTo);
            } else {
                super.send(producerExchange, messageSend);
            }
        } else {
            super.send(producerExchange, messageSend);
        }
    }

    public void start() throws Exception {
        super.start();
        LOG.info("Starting StatisticsBroker");
    }

    public void stop() throws Exception {
        super.stop();
    }

    protected void sendStats(ConnectionContext context, ActiveMQMapMessage msg, ActiveMQDestination replyTo)
            throws Exception {
        msg.setPersistent(false);
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
