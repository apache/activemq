/*
 * Copyright 2005-2006 The Apache Software Foundation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.Connection;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.broker.Connector;
import org.apache.activemq.broker.jmx.BrokerViewMBean;
import org.apache.activemq.broker.jmx.ManagementContext;
import org.apache.activemq.broker.jmx.SubscriptionViewMBean;
import org.apache.activemq.broker.region.Subscription;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ConsumerInfo;
import org.apache.activemq.command.ProducerInfo;
import org.apache.activemq.filter.DestinationMap;
import org.apache.activemq.filter.DestinationMapNode;

import javax.management.MBeanServer;
import javax.management.MBeanServerFactory;
import javax.management.MBeanServerInvocationHandler;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import java.io.PrintWriter;
import java.util.*;

/**
 * 
 * @version $Revision: $
 */
public class ConnectionDotFileInterceptor extends DotFileInterceptorSupport {

    protected static final String ID_SEPARATOR = "_";

    private final boolean redrawOnRemove;
    private String domain = "org.apache.activemq";
    private BrokerViewMBean brokerView;
    private MBeanServer mbeanServer;

    public ConnectionDotFileInterceptor(Broker next, String file, boolean redrawOnRemove)
            throws MalformedObjectNameException {
        super(next, file);
        this.redrawOnRemove = redrawOnRemove;

        mbeanServer = new ManagementContext().getMBeanServer();
        ObjectName brokerName = new ObjectName(domain + ":Type=Broker,BrokerName=localhost");
        brokerView = (BrokerViewMBean) MBeanServerInvocationHandler.newProxyInstance(mbeanServer, brokerName,
                BrokerViewMBean.class, true);
    }

    public Subscription addConsumer(ConnectionContext context, ConsumerInfo info) throws Exception {
        Subscription answer = super.addConsumer(context, info);
        generateFile();
        return answer;
    }

    public void addProducer(ConnectionContext context, ProducerInfo info) throws Exception {
        super.addProducer(context, info);
        generateFile();
    }

    public void removeConsumer(ConnectionContext context, ConsumerInfo info) throws Exception {
        super.removeConsumer(context, info);
        if (redrawOnRemove) {
            generateFile();
        }
    }

    public void removeProducer(ConnectionContext context, ProducerInfo info) throws Exception {
        super.removeProducer(context, info);
        if (redrawOnRemove) {
            generateFile();
        }
    }

    protected void generateFile(PrintWriter writer) throws Exception {

        writer.println("digraph \"ActiveMQ Connections\" {");
        writer.println();
        writer.println("node [style = \"rounded,filled\", fillcolor = yellow, fontname=\"Helvetica-Oblique\"];");
        writer.println();

        writer.println("broker [fillcolor = deepskyblue, label=\"ActiveMQ Broker\\n" + brokerView.getBrokerId() + "\"];");
        writer.println();

        Map clients = new HashMap();
        Map destinations = new HashMap();
        printSubscribers(writer, clients, destinations, "queue_", brokerView.getQueueSubscribers());
        writer.println();
        
        printSubscribers(writer, clients, destinations, "topic_", brokerView.getTopicSubscribers());
        writer.println();

        // lets print the broker links
        for (Iterator iter = clients.keySet().iterator(); iter.hasNext();) {
            String clientId = (String) iter.next();
            writer.print(clientId);
            writer.println(" -> broker");
        }
        writer.println();
        
        writeLabels(writer, "green", "Client: ", clients);
        writer.println();

        writeLabels(writer, "red", "Queue: ", destinations);
        writer.println("}");
    }

    protected void writeLabels(PrintWriter writer, String color, String prefix, Map map) {
        for (Iterator iter = map.entrySet().iterator(); iter.hasNext();) {
            Map.Entry entry = (Map.Entry) iter.next();
            String id = (String) entry.getKey();
            String label = (String) entry.getValue();

            writer.print(id);
            writer.print(" [ fillcolor = ");
            writer.print(color);
            writer.print(", label = \"");
            writer.print(prefix);
            writer.print(label);
            writer.println("\"];");
        }
    }

    protected void printSubscribers(PrintWriter writer, Map clients, Map destinations, String type,
            ObjectName[] subscribers) {
        for (int i = 0; i < subscribers.length; i++) {
            ObjectName name = subscribers[i];
            SubscriptionViewMBean subscriber = (SubscriptionViewMBean) MBeanServerInvocationHandler.newProxyInstance(
                    mbeanServer, name, SubscriptionViewMBean.class, true);

            String clientId = subscriber.getClientId();
            String destination = subscriber.getDestinationName();
            String selector = subscriber.getSelector();

            String safeClientId = asID(clientId);
            clients.put(safeClientId, clientId);

            String safeDestinationId = type + asID(destination);
            destinations.put(safeDestinationId, destination);

            // lets write out the links

            String subscriberId = safeClientId + "_" + subscriber.getSessionId() + "_" + subscriber.getSubcriptionId();

            writer.print(safeClientId);
            writer.print(" -> ");
            writer.print(subscriberId);
            writer.println(";");

            writer.print(safeDestinationId);
            writer.print(" -> ");
            writer.print(subscriberId);
            writer.println(";");

            // now lets write out the label
            writer.print(subscriberId);
            writer.print(" [label = \"");
            String label = "Subscription: " + subscriber.getSessionId() + "-" + subscriber.getSubcriptionId();
            if (selector != null && selector.length() > 0) {
                label = label + "\\nSelector: " + selector;
            }
            writer.print(label);
            writer.println("\"];");
        }
    }

    /**
     * Lets strip out any non supported characters
     */
    protected String asID(String name) {
        StringBuffer buffer = new StringBuffer();
        for (int i = 0, size = name.length(); i < size; i++) {
            char ch = name.charAt(i);
            if (Character.isLetterOrDigit(ch) || ch == '_') {
                buffer.append(ch);
            }
            else {
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
        }
        else if (prefix.equals("queue")) {
            label = "Queues";
        }
        writer.print("[ label = \"");
        writer.print(label);
        writer.println("\" ];");

        Collection children = node.getChildren();
        for (Iterator iter = children.iterator(); iter.hasNext();) {
            DestinationMapNode child = (DestinationMapNode) iter.next();
            printNodes(writer, child, prefix + ID_SEPARATOR + path);
        }
    }

    protected void printNodeLinks(PrintWriter writer, DestinationMapNode node, String prefix) {
        String path = getPath(node);
        Collection children = node.getChildren();
        for (Iterator iter = children.iterator(); iter.hasNext();) {
            DestinationMapNode child = (DestinationMapNode) iter.next();

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
}
