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

import java.io.PrintWriter;

import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.broker.region.Destination;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.filter.DestinationMap;
import org.apache.activemq.filter.DestinationMapNode;
import org.apache.activemq.filter.DestinationNode;

/**
 * 
 */
public class DestinationDotFileInterceptor extends DotFileInterceptorSupport {

    protected static final String ID_SEPARATOR = "_";

    public DestinationDotFileInterceptor(Broker next, String file) {
        super(next, file);
    }

    @Override
    public Destination addDestination(ConnectionContext context, ActiveMQDestination destination,boolean create) throws Exception {
        Destination answer = super.addDestination(context, destination,create);
        generateFile();
        return answer;
    }

    @Override
    public void removeDestination(ConnectionContext context, ActiveMQDestination destination, long timeout) throws Exception {
        super.removeDestination(context, destination, timeout);
        generateFile();
    }

    @Override
    protected void generateFile(PrintWriter writer) throws Exception {
        ActiveMQDestination[] destinations = getDestinations();

        // let's split into a tree
        DestinationMap map = new DestinationMap();

        for (ActiveMQDestination destination : destinations) {
            map.put(destination, destination);
        }

        // now let's navigate the tree
        writer.println("digraph \"ActiveMQ Destinations\" {");
        writer.println();
        writer.println("node [style = \"rounded,filled\", fontname=\"Helvetica-Oblique\"];");
        writer.println();
        writer.println("topic_root [fillcolor = deepskyblue, label = \"Topics\" ];");
        writer.println("queue_root [fillcolor = deepskyblue, label = \"Queues\" ];");
        writer.println();

        writer.println("subgraph queues {");
        writer.println("  node [fillcolor=red];     ");
        writer.println("  label = \"Queues\"");
        writer.println();
        printNodeLinks(writer, map.getQueueRootNode(), "queue");
        writer.println("}");
        writer.println();

        writer.println("subgraph temp queues {");
        writer.println("  node [fillcolor=red];     ");
        writer.println("  label = \"TempQueues\"");
        writer.println();
        printNodeLinks(writer, map.getTempQueueRootNode(), "tempqueue");
        writer.println("}");
        writer.println();

        writer.println("subgraph topics {");
        writer.println("  node [fillcolor=green];     ");
        writer.println("  label = \"Topics\"");
        writer.println();
        printNodeLinks(writer, map.getTopicRootNode(), "topic");
        writer.println("}");
        writer.println();

        writer.println("subgraph temp topics {");
        writer.println("  node [fillcolor=green];     ");
        writer.println("  label = \"TempTopics\"");
        writer.println();
        printNodeLinks(writer, map.getTempTopicRootNode(), "temptopic");
        writer.println("}");
        writer.println();

        printNodes(writer, map.getQueueRootNode(), "queue");
        writer.println();

        printNodes(writer, map.getTempQueueRootNode(), "tempqueue");
        writer.println();

        printNodes(writer, map.getTopicRootNode(), "topic");
        writer.println();

        printNodes(writer, map.getTempTopicRootNode(), "temptopic");
        writer.println();

        writer.println("}");
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

        for (DestinationNode destinationNode : node.getChildren()) {
            DestinationMapNode child = (DestinationMapNode) destinationNode;
            printNodes(writer, child, prefix + ID_SEPARATOR + path);
        }
    }

    protected void printNodeLinks(PrintWriter writer, DestinationMapNode node, String prefix) {
        String path = getPath(node);
        for (DestinationNode destinationNode : node.getChildren()) {
            DestinationMapNode child = (DestinationMapNode) destinationNode;

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
