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
package org.apache.activemq.broker.view;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import javax.management.ObjectName;

import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.broker.ProducerBrokerExchange;
import org.apache.activemq.broker.jmx.BrokerViewMBean;
import org.apache.activemq.broker.jmx.SubscriptionViewMBean;
import org.apache.activemq.broker.region.Subscription;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ConsumerInfo;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.ProducerId;
import org.apache.activemq.command.ProducerInfo;
import org.apache.activemq.filter.DestinationMapNode;

/**
 *
 */
public class ConnectionDotFileInterceptor extends DotFileInterceptorSupport {

    protected static final String ID_SEPARATOR = "_";

    private final boolean redrawOnRemove;
    private boolean clearProducerCacheAfterRender;
    private final String domain = "org.apache.activemq";
    private BrokerViewMBean brokerView;

    // until we have some MBeans for producers, lets do it all ourselves
    private final Map<ProducerId, ProducerInfo> producers = new HashMap<ProducerId, ProducerInfo>();
    private final Map<ProducerId, Set<ActiveMQDestination>> producerDestinations = new HashMap<ProducerId, Set<ActiveMQDestination>>();
    private final Object lock = new Object();

    public ConnectionDotFileInterceptor(Broker next, String file, boolean redrawOnRemove) throws IOException {
        super(next, file);
        this.redrawOnRemove = redrawOnRemove;

    }

    @Override
    public Subscription addConsumer(ConnectionContext context, ConsumerInfo info) throws Exception {
        Subscription answer = super.addConsumer(context, info);
        generateFile();
        return answer;
    }

    @Override
    public void addProducer(ConnectionContext context, ProducerInfo info) throws Exception {
        super.addProducer(context, info);
        ProducerId producerId = info.getProducerId();
        synchronized (lock) {
            producers.put(producerId, info);
        }
        generateFile();
    }

    @Override
    public void removeConsumer(ConnectionContext context, ConsumerInfo info) throws Exception {
        super.removeConsumer(context, info);
        if (redrawOnRemove) {
            generateFile();
        }
    }

    @Override
    public void removeProducer(ConnectionContext context, ProducerInfo info) throws Exception {
        super.removeProducer(context, info);
        ProducerId producerId = info.getProducerId();
        if (redrawOnRemove) {
            synchronized (lock) {
                producerDestinations.remove(producerId);
                producers.remove(producerId);
            }
            generateFile();
        }
    }

    @Override
    public void send(ProducerBrokerExchange producerExchange, Message messageSend) throws Exception {
        super.send(producerExchange, messageSend);
        ProducerId producerId = messageSend.getProducerId();
        ActiveMQDestination destination = messageSend.getDestination();
        synchronized (lock) {
            Set<ActiveMQDestination> destinations = producerDestinations.get(producerId);
            if (destinations == null) {
                destinations = new HashSet<ActiveMQDestination>();
            }
            producerDestinations.put(producerId, destinations);
            destinations.add(destination);
        }
    }

    @Override
    protected void generateFile(PrintWriter writer) throws Exception {

        writer.println("digraph \"ActiveMQ Connections\" {");
        writer.println();
        writer.println("label=\"ActiveMQ Broker: " + getBrokerView().getBrokerId() + "\"];");
        writer.println();
        writer.println("node [style = \"rounded,filled\", fillcolor = yellow, fontname=\"Helvetica-Oblique\"];");
        writer.println();

        Map<String, String> clients = new HashMap<String, String>();
        Map<String, String> queues = new HashMap<String, String>();
        Map<String, String> topics = new HashMap<String, String>();

        printSubscribers(writer, clients, queues, "queue_", getBrokerView().getQueueSubscribers());
        writer.println();

        printSubscribers(writer, clients, topics, "topic_", getBrokerView().getTopicSubscribers());
        writer.println();

        printProducers(writer, clients, queues, topics);
        writer.println();

        writeLabels(writer, "green", "Client: ", clients);
        writer.println();

        writeLabels(writer, "red", "Queue: ", queues);
        writeLabels(writer, "blue", "Topic: ", topics);
        writer.println("}");

        if (clearProducerCacheAfterRender) {
            producerDestinations.clear();
        }
    }

    protected void printProducers(PrintWriter writer, Map<String, String> clients, Map<String, String> queues, Map<String, String> topics) {
        synchronized (lock) {
            for (Iterator iter = producerDestinations.entrySet().iterator(); iter.hasNext();) {
                Map.Entry entry = (Map.Entry)iter.next();
                ProducerId producerId = (ProducerId)entry.getKey();
                Set destinationSet = (Set)entry.getValue();
                printProducers(writer, clients, queues, topics, producerId, destinationSet);
            }
        }
    }

    protected void printProducers(PrintWriter writer, Map<String, String> clients, Map<String, String> queues, Map<String, String> topics, ProducerId producerId, Set destinationSet) {
        for (Iterator iter = destinationSet.iterator(); iter.hasNext();) {
            ActiveMQDestination destination = (ActiveMQDestination)iter.next();

            // TODO use clientId one day
            String clientId = producerId.getConnectionId();
            String safeClientId = asID(clientId);
            clients.put(safeClientId, clientId);

            String physicalName = destination.getPhysicalName();
            String safeDestinationId = asID(physicalName);
            if (destination.isTopic()) {
                safeDestinationId = "topic_" + safeDestinationId;
                topics.put(safeDestinationId, physicalName);
            } else {
                safeDestinationId = "queue_" + safeDestinationId;
                queues.put(safeDestinationId, physicalName);
            }

            String safeProducerId = asID(producerId.toString());

            // lets write out the links

            writer.print(safeClientId);
            writer.print(" -> ");
            writer.print(safeProducerId);
            writer.println(";");

            writer.print(safeProducerId);
            writer.print(" -> ");
            writer.print(safeDestinationId);
            writer.println(";");

            // now lets write out the label
            writer.print(safeProducerId);
            writer.print(" [label = \"");
            String label = "Producer: " + producerId.getSessionId() + "-" + producerId.getValue();
            writer.print(label);
            writer.println("\"];");

        }
    }

    protected void printSubscribers(PrintWriter writer, Map<String, String> clients, Map<String, String> destinations, String type, ObjectName[] subscribers) {
        for (int i = 0; i < subscribers.length; i++) {
            ObjectName name = subscribers[i];
            SubscriptionViewMBean subscriber = (SubscriptionViewMBean)getBrokerService().getManagementContext().newProxyInstance(name, SubscriptionViewMBean.class, true);

            String clientId = subscriber.getClientId();
            String safeClientId = asID(clientId);
            clients.put(safeClientId, clientId);

            String destination = subscriber.getDestinationName();
            String safeDestinationId = type + asID(destination);
            destinations.put(safeDestinationId, destination);

            String selector = subscriber.getSelector();

            // lets write out the links
            String subscriberId = safeClientId + "_" + subscriber.getSessionId() + "_" + subscriber.getSubscriptionId();

            writer.print(subscriberId);
            writer.print(" -> ");
            writer.print(safeClientId);
            writer.println(";");

            writer.print(safeDestinationId);
            writer.print(" -> ");
            writer.print(subscriberId);
            writer.println(";");

            // now lets write out the label
            writer.print(subscriberId);
            writer.print(" [label = \"");
            String label = "Subscription: " + subscriber.getSessionId() + "-" + subscriber.getSubscriptionId();
            if (selector != null && selector.length() > 0) {
                label = label + "\\nSelector: " + selector;
            }
            writer.print(label);
            writer.println("\"];");
        }
    }

    protected void writeLabels(PrintWriter writer, String color, String prefix, Map<String, String> map) {
        for (Iterator iter = map.entrySet().iterator(); iter.hasNext();) {
            Map.Entry entry = (Map.Entry)iter.next();
            String id = (String)entry.getKey();
            String label = (String)entry.getValue();

            writer.print(id);
            writer.print(" [ fillcolor = ");
            writer.print(color);
            writer.print(", label = \"");
            writer.print(prefix);
            writer.print(label);
            writer.println("\"];");
        }
    }

    /**
     * Lets strip out any non supported characters
     */
    protected String asID(String name) {
        StringBuffer buffer = new StringBuffer();
        int size = name.length();
        for (int i = 0; i < size; i++) {
            char ch = name.charAt(i);
            if (Character.isLetterOrDigit(ch) || ch == '_') {
                buffer.append(ch);
            } else {
                buffer.append('_');
            }
        }
        return buffer.toString();
    }

    protected void printNodes(PrintWriter writer, DestinationMapNode node, String prefix) {
        String path = getPath(node);
        writer.print("  ");
        writer.print(prefix);
        writer.print(ID_SEPARATOR);
        writer.print(path);
        String label = path;
        if (prefix.equals("topic")) {
            label = "Topics";
        } else if (prefix.equals("queue")) {
            label = "Queues";
        }
        writer.print("[ label = \"");
        writer.print(label);
        writer.println("\" ];");

        Collection children = node.getChildren();
        for (Iterator iter = children.iterator(); iter.hasNext();) {
            DestinationMapNode child = (DestinationMapNode)iter.next();
            printNodes(writer, child, prefix + ID_SEPARATOR + path);
        }
    }

    protected void printNodeLinks(PrintWriter writer, DestinationMapNode node, String prefix) {
        String path = getPath(node);
        Collection children = node.getChildren();
        for (Iterator iter = children.iterator(); iter.hasNext();) {
            DestinationMapNode child = (DestinationMapNode)iter.next();

            writer.print("  ");
            writer.print(prefix);
            writer.print(ID_SEPARATOR);
            writer.print(path);
            writer.print(" -> ");
            writer.print(prefix);
            writer.print(ID_SEPARATOR);
            writer.print(path);
            writer.print(ID_SEPARATOR);
            writer.print(getPath(child));
            writer.println(";");

            printNodeLinks(writer, child, prefix + ID_SEPARATOR + path);
        }
    }

    protected String getPath(DestinationMapNode node) {
        String path = node.getPath();
        if (path.equals("*")) {
            return "root";
        }
        return path;
    }

    BrokerViewMBean getBrokerView() throws Exception {
        if (this.brokerView == null) {
            ObjectName brokerName = getBrokerService().getBrokerObjectName();
            this.brokerView = (BrokerViewMBean) getBrokerService().getManagementContext().newProxyInstance(brokerName,
                    BrokerViewMBean.class, true);
        }
        return this.brokerView;
    }
}
