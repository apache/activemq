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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.jms.Connection;
import javax.jms.InvalidSelectorException;
import javax.jms.MessageProducer;
import javax.jms.Session;
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
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQMessage;
import org.apache.activemq.command.ActiveMQTextMessage;
import org.apache.activemq.command.Message;
import org.apache.activemq.filter.BooleanExpression;
import org.apache.activemq.filter.MessageEvaluationContext;
import org.apache.activemq.selector.SelectorParser;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class DestinationView implements DestinationViewMBean {
    private static final Log LOG = LogFactory.getLog(DestinationViewMBean.class);
    protected final Destination destination;
    protected final ManagedRegionBroker broker;

    public DestinationView(ManagedRegionBroker broker, Destination destination) {
        this.broker = broker;
        this.destination = destination;
    }

    public void gc() {
        destination.gc();
    }

    public String getName() {
        return destination.getName();
    }

    public void resetStatistics() {
        destination.getDestinationStatistics().reset();
    }

    public long getEnqueueCount() {
        return destination.getDestinationStatistics().getEnqueues().getCount();
    }

    public long getDequeueCount() {
        return destination.getDestinationStatistics().getDequeues().getCount();
    }

    public long getDispatchCount() {
        return destination.getDestinationStatistics().getDispatched().getCount();
    }

    public long getConsumerCount() {
        return destination.getDestinationStatistics().getConsumers().getCount();
    }

    public long getQueueSize() {
        return destination.getDestinationStatistics().getMessages().getCount();
    }

    public long getMessagesCached() {
        return destination.getDestinationStatistics().getMessagesCached().getCount();
    }

    public int getMemoryPercentageUsed() {
        return destination.getUsageManager().getPercentUsage();
    }

    public long getMemoryLimit() {
        return destination.getUsageManager().getLimit();
    }

    public void setMemoryLimit(long limit) {
        destination.getUsageManager().setLimit(limit);
    }

    public double getAverageEnqueueTime() {
        return destination.getDestinationStatistics().getProcessTime().getAverageTime();
    }

    public long getMaxEnqueueTime() {
        return destination.getDestinationStatistics().getProcessTime().getMaxTime();
    }

    public long getMinEnqueueTime() {
        return destination.getDestinationStatistics().getProcessTime().getMinTime();
    }

    public CompositeData[] browse() throws OpenDataException {
        try {
            return browse(null);
        } catch (InvalidSelectorException e) {
            // should not happen.
            throw new RuntimeException(e);
        }
    }

    public CompositeData[] browse(String selector) throws OpenDataException, InvalidSelectorException {
        Message[] messages = destination.browse();
        ArrayList<CompositeData> c = new ArrayList<CompositeData>();

        MessageEvaluationContext ctx = new MessageEvaluationContext();
        ctx.setDestination(destination.getActiveMQDestination());
        BooleanExpression selectorExpression = selector == null ? null : new SelectorParser().parse(selector);

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
    public List<Object> browseMessages() throws InvalidSelectorException {
        return browseMessages(null);
    }

    /**
     * Browses the current destination with the given selector returning a list
     * of messages
     */
    public List<Object> browseMessages(String selector) throws InvalidSelectorException {
        Message[] messages = destination.browse();
        ArrayList<Object> answer = new ArrayList<Object>();

        MessageEvaluationContext ctx = new MessageEvaluationContext();
        ctx.setDestination(destination.getActiveMQDestination());
        BooleanExpression selectorExpression = selector == null ? null : new SelectorParser().parse(selector);

        for (int i = 0; i < messages.length; i++) {
            try {
                Message message = messages[i];
                if (selectorExpression == null) {
                    answer.add(OpenTypeSupport.convert(message));
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

    public TabularData browseAsTable() throws OpenDataException {
        try {
            return browseAsTable(null);
        } catch (InvalidSelectorException e) {
            throw new RuntimeException(e);
        }
    }

    public TabularData browseAsTable(String selector) throws OpenDataException, InvalidSelectorException {
        OpenTypeFactory factory = OpenTypeSupport.getFactory(ActiveMQMessage.class);
        Message[] messages = destination.browse();
        CompositeType ct = factory.getCompositeType();
        TabularType tt = new TabularType("MessageList", "MessageList", ct, new String[] {"JMSMessageID"});
        TabularDataSupport rc = new TabularDataSupport(tt);

        MessageEvaluationContext ctx = new MessageEvaluationContext();
        ctx.setDestination(destination.getActiveMQDestination());
        BooleanExpression selectorExpression = selector == null ? null : new SelectorParser().parse(selector);

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

    public String sendTextMessage(String body) throws Exception {
        return sendTextMessage(Collections.EMPTY_MAP, body);
    }

    public String sendTextMessage(Map headers, String body) throws Exception {

        String brokerUrl = "vm://" + broker.getBrokerName();
        ActiveMQDestination dest = destination.getActiveMQDestination();

        ActiveMQConnectionFactory cf = new ActiveMQConnectionFactory(brokerUrl);
        Connection connection = null;
        try {

            connection = cf.createConnection();
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageProducer producer = session.createProducer(dest);
            ActiveMQTextMessage msg = (ActiveMQTextMessage)session.createTextMessage(body);

            for (Iterator iter = headers.entrySet().iterator(); iter.hasNext();) {
                Map.Entry entry = (Map.Entry)iter.next();
                msg.setObjectProperty((String)entry.getKey(), entry.getValue());
            }

            producer.setDeliveryMode(msg.getJMSDeliveryMode());
            producer.setPriority(msg.getPriority());
            long ttl = msg.getExpiration() - System.currentTimeMillis();
            producer.setTimeToLive(ttl > 0 ? ttl : 0);
            producer.send(msg);

            return msg.getJMSMessageID();

        } finally {
            connection.close();
        }

    }
}
