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

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import javax.jms.Connection;
import javax.jms.InvalidSelectorException;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.openmbean.CompositeData;
import javax.management.openmbean.CompositeDataSupport;
import javax.management.openmbean.CompositeType;
import javax.management.openmbean.OpenDataException;
import javax.management.openmbean.TabularData;
import javax.management.openmbean.TabularDataSupport;
import javax.management.openmbean.TabularType;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.jmx.OpenTypeSupport.OpenTypeFactory;
import org.apache.activemq.broker.region.Destination;
import org.apache.activemq.broker.region.Subscription;
import org.apache.activemq.broker.region.policy.AbortSlowConsumerStrategy;
import org.apache.activemq.broker.region.policy.SlowConsumerStrategy;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQMessage;
import org.apache.activemq.command.ActiveMQTextMessage;
import org.apache.activemq.command.Message;
import org.apache.activemq.filter.BooleanExpression;
import org.apache.activemq.filter.MessageEvaluationContext;
import org.apache.activemq.selector.SelectorParser;
import org.apache.activemq.store.MessageStore;
import org.apache.activemq.util.URISupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DestinationView implements DestinationViewMBean {
    private static final Logger LOG = LoggerFactory.getLogger(DestinationViewMBean.class);
    protected final Destination destination;
    protected final ManagedRegionBroker broker;

    public DestinationView(ManagedRegionBroker broker, Destination destination) {
        this.broker = broker;
        this.destination = destination;
    }

    public void gc() {
        destination.gc();
    }

    @Override
    public String getName() {
        return destination.getName();
    }

    @Override
    public void resetStatistics() {
        destination.getDestinationStatistics().reset();
    }

    @Override
    public long getEnqueueCount() {
        return destination.getDestinationStatistics().getEnqueues().getCount();
    }

    @Override
    public long getDequeueCount() {
        return destination.getDestinationStatistics().getDequeues().getCount();
    }

    @Override
    public long getForwardCount() {
        return destination.getDestinationStatistics().getForwards().getCount();
    }

    @Override
    public long getDispatchCount() {
        return destination.getDestinationStatistics().getDispatched().getCount();
    }

    @Override
    public long getInFlightCount() {
        return destination.getDestinationStatistics().getInflight().getCount();
    }

    @Override
    public long getExpiredCount() {
        return destination.getDestinationStatistics().getExpired().getCount();
    }

    @Override
    public long getConsumerCount() {
        return destination.getDestinationStatistics().getConsumers().getCount();
    }

    @Override
    public long getQueueSize() {
        return destination.getDestinationStatistics().getMessages().getCount();
    }

    @Override
    public long getStoreMessageSize() {
        MessageStore messageStore = destination.getMessageStore();
        return messageStore != null ? messageStore.getMessageStoreStatistics().getMessageSize().getTotalSize() : 0;
    }

    public long getMessagesCached() {
        return destination.getDestinationStatistics().getMessagesCached().getCount();
    }

    @Override
    public int getMemoryPercentUsage() {
        return destination.getMemoryUsage().getPercentUsage();
    }

    @Override
    public long getMemoryUsageByteCount() {
        return destination.getMemoryUsage().getUsage();
    }

    @Override
    public long getMemoryLimit() {
        return destination.getMemoryUsage().getLimit();
    }

    @Override
    public void setMemoryLimit(long limit) {
        destination.getMemoryUsage().setLimit(limit);
    }

    @Override
    public double getAverageEnqueueTime() {
        return destination.getDestinationStatistics().getProcessTime().getAverageTime();
    }

    @Override
    public long getMaxEnqueueTime() {
        return destination.getDestinationStatistics().getProcessTime().getMaxTime();
    }

    @Override
    public long getMinEnqueueTime() {
        return destination.getDestinationStatistics().getProcessTime().getMinTime();
    }

    /**
     * @return the average size of a message (bytes)
     */
    @Override
    public long getAverageMessageSize() {
        // we are okay with the size without decimals so cast to long
        return (long) destination.getDestinationStatistics().getMessageSize().getAverageSize();
    }

    /**
     * @return the max size of a message (bytes)
     */
    @Override
    public long getMaxMessageSize() {
        return destination.getDestinationStatistics().getMessageSize().getMaxSize();
    }

    /**
     * @return the min size of a message (bytes)
     */
    @Override
    public long getMinMessageSize() {
        return destination.getDestinationStatistics().getMessageSize().getMinSize();
    }


    @Override
    public boolean isPrioritizedMessages() {
        return destination.isPrioritizedMessages();
    }

    @Override
    public CompositeData[] browse() throws OpenDataException {
        try {
            return browse(null);
        } catch (InvalidSelectorException e) {
            // should not happen.
            throw new RuntimeException(e);
        }
    }

    @Override
    public CompositeData[] browse(String selector) throws OpenDataException, InvalidSelectorException {
        Message[] messages = destination.browse();
        ArrayList<CompositeData> c = new ArrayList<CompositeData>();

        MessageEvaluationContext ctx = new MessageEvaluationContext();
        ctx.setDestination(destination.getActiveMQDestination());
        BooleanExpression selectorExpression = selector == null ? null : SelectorParser.parse(selector);

        for (int i = 0; i < messages.length; i++) {
            try {

                if (selectorExpression == null) {
                    c.add(OpenTypeSupport.convert(messages[i]));
                } else {
                    ctx.setMessageReference(messages[i]);
                    if (selectorExpression.matches(ctx)) {
                        c.add(OpenTypeSupport.convert(messages[i]));
                    }
                }

            } catch (Throwable e) {
                LOG.warn("exception browsing destination", e);
            }
        }

        CompositeData rc[] = new CompositeData[c.size()];
        c.toArray(rc);
        return rc;
    }

    /**
     * Browses the current destination returning a list of messages
     */
    @Override
    public List<Object> browseMessages() throws InvalidSelectorException {
        return browseMessages(null);
    }

    /**
     * Browses the current destination with the given selector returning a list
     * of messages
     */
    @Override
    public List<Object> browseMessages(String selector) throws InvalidSelectorException {
        Message[] messages = destination.browse();
        ArrayList<Object> answer = new ArrayList<Object>();

        MessageEvaluationContext ctx = new MessageEvaluationContext();
        ctx.setDestination(destination.getActiveMQDestination());
        BooleanExpression selectorExpression = selector == null ? null : SelectorParser.parse(selector);

        for (int i = 0; i < messages.length; i++) {
            try {
                Message message = messages[i];
                message.setReadOnlyBody(true);
                if (selectorExpression == null) {
                    answer.add(message);
                } else {
                    ctx.setMessageReference(message);
                    if (selectorExpression.matches(ctx)) {
                        answer.add(message);
                    }
                }

            } catch (Throwable e) {
                LOG.warn("exception browsing destination", e);
            }
        }
        return answer;
    }

    @Override
    public TabularData browseAsTable() throws OpenDataException {
        try {
            return browseAsTable(null);
        } catch (InvalidSelectorException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public TabularData browseAsTable(String selector) throws OpenDataException, InvalidSelectorException {
        OpenTypeFactory factory = OpenTypeSupport.getFactory(ActiveMQMessage.class);
        Message[] messages = destination.browse();
        CompositeType ct = factory.getCompositeType();
        TabularType tt = new TabularType("MessageList", "MessageList", ct, new String[] { "JMSMessageID" });
        TabularDataSupport rc = new TabularDataSupport(tt);

        MessageEvaluationContext ctx = new MessageEvaluationContext();
        ctx.setDestination(destination.getActiveMQDestination());
        BooleanExpression selectorExpression = selector == null ? null : SelectorParser.parse(selector);

        for (int i = 0; i < messages.length; i++) {
            try {
                if (selectorExpression == null) {
                    rc.put(new CompositeDataSupport(ct, factory.getFields(messages[i])));
                } else {
                    ctx.setMessageReference(messages[i]);
                    if (selectorExpression.matches(ctx)) {
                        rc.put(new CompositeDataSupport(ct, factory.getFields(messages[i])));
                    }
                }
            } catch (Throwable e) {
                LOG.warn("exception browsing destination", e);
            }
        }

        return rc;
    }

    @Override
    public String sendTextMessageWithProperties(String properties) throws Exception {
        String[] kvs = properties.split(",");
        Map<String, String> props = new HashMap<String, String>();
        for (String kv : kvs) {
            String[] it = kv.split("=");
            if (it.length == 2) {
                props.put(it[0],it[1]);
            }
        }
        return sendTextMessage(props, props.remove("body"), props.remove("username"), props.remove("password"));
    }

    @Override
    public String sendTextMessage(String body) throws Exception {
        return sendTextMessage(Collections.EMPTY_MAP, body);
    }

    @Override
    public String sendTextMessage(Map headers, String body) throws Exception {
        return sendTextMessage(headers, body, null, null);
    }

    @Override
    public String sendTextMessage(String body, String user, @Sensitive String password) throws Exception {
        return sendTextMessage(Collections.EMPTY_MAP, body, user, password);
    }

    @Override
    public String sendTextMessage(Map<String, String> headers, String body, String userName, @Sensitive String password) throws Exception {

        String brokerUrl = "vm://" + broker.getBrokerName();
        ActiveMQDestination dest = destination.getActiveMQDestination();

        ActiveMQConnectionFactory cf = new ActiveMQConnectionFactory(brokerUrl);
        Connection connection = null;
        try {
            connection = cf.createConnection(userName, password);
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageProducer producer = session.createProducer(dest);
            ActiveMQTextMessage msg = (ActiveMQTextMessage) session.createTextMessage(body);

            for (Iterator<Entry<String, String>> iter = headers.entrySet().iterator(); iter.hasNext();) {
                Entry<String, String> entry = iter.next();
                msg.setObjectProperty(entry.getKey(), entry.getValue());
            }

            producer.setDeliveryMode(msg.getJMSDeliveryMode());
            producer.setPriority(msg.getPriority());
            long ttl = 0;
            if (msg.getExpiration() != 0) {
                ttl = msg.getExpiration() - System.currentTimeMillis();
            } else {
                String timeToLive = headers.get("timeToLive");
                if (timeToLive != null) {
                    ttl = Integer.valueOf(timeToLive);
                }
            }
            producer.setTimeToLive(ttl > 0 ? ttl : 0);
            producer.send(msg);

            return msg.getJMSMessageID();

        } finally {
            if (connection != null) {
                connection.close();
            }
        }
    }

    @Override
    public int getMaxAuditDepth() {
        return destination.getMaxAuditDepth();
    }

    @Override
    public int getMaxProducersToAudit() {
        return destination.getMaxProducersToAudit();
    }

    public boolean isEnableAudit() {
        return destination.isEnableAudit();
    }

    public void setEnableAudit(boolean enableAudit) {
        destination.setEnableAudit(enableAudit);
    }

    @Override
    public void setMaxAuditDepth(int maxAuditDepth) {
        destination.setMaxAuditDepth(maxAuditDepth);
    }

    @Override
    public void setMaxProducersToAudit(int maxProducersToAudit) {
        destination.setMaxProducersToAudit(maxProducersToAudit);
    }

    @Override
    public float getMemoryUsagePortion() {
        return destination.getMemoryUsage().getUsagePortion();
    }

    @Override
    public long getProducerCount() {
        return destination.getDestinationStatistics().getProducers().getCount();
    }

    @Override
    public boolean isProducerFlowControl() {
        return destination.isProducerFlowControl();
    }

    @Override
    public void setMemoryUsagePortion(float value) {
        destination.getMemoryUsage().setUsagePortion(value);
    }

    @Override
    public void setProducerFlowControl(boolean producerFlowControl) {
        destination.setProducerFlowControl(producerFlowControl);
    }

    @Override
    public boolean isAlwaysRetroactive() {
        return destination.isAlwaysRetroactive();
    }

    @Override
    public void setAlwaysRetroactive(boolean alwaysRetroactive) {
        destination.setAlwaysRetroactive(alwaysRetroactive);
    }

    /**
     * Set's the interval at which warnings about producers being blocked by
     * resource usage will be triggered. Values of 0 or less will disable
     * warnings
     *
     * @param blockedProducerWarningInterval the interval at which warning about
     *            blocked producers will be triggered.
     */
    @Override
    public void setBlockedProducerWarningInterval(long blockedProducerWarningInterval) {
        destination.setBlockedProducerWarningInterval(blockedProducerWarningInterval);
    }

    /**
     *
     * @return the interval at which warning about blocked producers will be
     *         triggered.
     */
    @Override
    public long getBlockedProducerWarningInterval() {
        return destination.getBlockedProducerWarningInterval();
    }

    @Override
    public int getMaxPageSize() {
        return destination.getMaxPageSize();
    }

    @Override
    public void setMaxPageSize(int pageSize) {
        destination.setMaxPageSize(pageSize);
    }

    @Override
    public boolean isUseCache() {
        return destination.isUseCache();
    }

    @Override
    public void setUseCache(boolean value) {
        destination.setUseCache(value);
    }

    @Override
    public ObjectName[] getSubscriptions() throws IOException, MalformedObjectNameException {
        List<Subscription> subscriptions = destination.getConsumers();
        ObjectName[] answer = new ObjectName[subscriptions.size()];
        ObjectName brokerObjectName = broker.getBrokerService().getBrokerObjectName();
        int index = 0;
        for (Subscription subscription : subscriptions) {
            String connectionClientId = subscription.getContext().getClientId();
            answer[index++] = BrokerMBeanSupport.createSubscriptionName(brokerObjectName, connectionClientId, subscription.getConsumerInfo());
        }
        return answer;
    }

    @Override
    public ObjectName getSlowConsumerStrategy() throws IOException, MalformedObjectNameException {
        ObjectName result = null;
        SlowConsumerStrategy strategy = destination.getSlowConsumerStrategy();
        if (strategy != null && strategy instanceof AbortSlowConsumerStrategy) {
            result = broker.registerSlowConsumerStrategy((AbortSlowConsumerStrategy)strategy);
        }
        return result;
    }

    @Override
    public String getOptions() {
        Map<String, String> options = destination.getActiveMQDestination().getOptions();
        String optionsString = "";
        try {
            if (options != null) {
                optionsString = URISupport.createQueryString(options);
            }
        } catch (URISyntaxException ignored) {}
        return optionsString;
    }

    @Override
    public boolean isDLQ() {
        return destination.getActiveMQDestination().isDLQ();
    }

    @Override
    public void setDLQ(boolean val) {
         destination.getActiveMQDestination().setDLQ(val);
    }

    @Override
    public long getBlockedSends() {
        return destination.getDestinationStatistics().getBlockedSends().getCount();
    }

    @Override
    public double getAverageBlockedTime() {
        return destination.getDestinationStatistics().getBlockedTime().getAverageTime();
    }

    @Override
    public long getTotalBlockedTime() {
        return destination.getDestinationStatistics().getBlockedTime().getTotalTime();
    }

}
