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

package org.apache.activemq.broker.util;

import org.apache.activemq.broker.BrokerPluginSupport;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.broker.ConsumerBrokerExchange;
import org.apache.activemq.broker.ProducerBrokerExchange;
import org.apache.activemq.broker.region.Destination;
import org.apache.activemq.broker.region.Subscription;
import org.apache.activemq.command.*;
import org.apache.activemq.filter.DestinationPath;

import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

/**
 * @org.apache.xbean.XBean element="destinationPathSeparatorPlugin"
 */

public class DestinationPathSeparatorBroker extends BrokerPluginSupport {

    String pathSeparator = "/";

    protected ActiveMQDestination convertDestination(ActiveMQDestination destination) {
        if (destination != null && destination.getPhysicalName().contains(pathSeparator)) {
            List<String> l = new ArrayList<String>();
            StringTokenizer iter = new StringTokenizer(destination.getPhysicalName(), pathSeparator);
            while (iter.hasMoreTokens()) {
                String name = iter.nextToken().trim();
                if (name.length() == 0) {
                    continue;
                }
                l.add(name);
            }

            String newName = DestinationPath.toString(l.toArray(new String[l.size()]));
            return ActiveMQDestination.createDestination(newName, destination.getDestinationType());
        } else {
            return destination;
        }
    }


    @Override
    public void acknowledge(ConsumerBrokerExchange consumerExchange, MessageAck ack) throws Exception {
        ack.setDestination(convertDestination(ack.getDestination()));
        super.acknowledge(consumerExchange, ack);    
    }

    @Override
    public Subscription addConsumer(ConnectionContext context, ConsumerInfo info) throws Exception {
        info.setDestination(convertDestination(info.getDestination()));
        return super.addConsumer(context, info);    
    }

    @Override
    public void addProducer(ConnectionContext context, ProducerInfo info) throws Exception {
        info.setDestination(convertDestination(info.getDestination()));
        super.addProducer(context, info);    
    }

    @Override
    public void removeConsumer(ConnectionContext context, ConsumerInfo info) throws Exception {
        info.setDestination(convertDestination(info.getDestination()));
        super.removeConsumer(context, info);    
    }

    @Override
    public void removeProducer(ConnectionContext context, ProducerInfo info) throws Exception {
        info.setDestination(convertDestination(info.getDestination()));
        super.removeProducer(context, info);    
    }

    @Override
    public void send(ProducerBrokerExchange producerExchange, Message messageSend) throws Exception {
        messageSend.setDestination(convertDestination(messageSend.getDestination()));
        super.send(producerExchange, messageSend);    
    }

    @Override
    public Destination addDestination(ConnectionContext context, ActiveMQDestination destination, boolean createIfTemporary) throws Exception {
        return super.addDestination(context, convertDestination(destination), createIfTemporary);
    }

    @Override
    public void removeDestination(ConnectionContext context, ActiveMQDestination destination, long timeout) throws Exception {
        super.removeDestination(context, convertDestination(destination), timeout);
    }

    @Override
    public void addDestinationInfo(ConnectionContext context, DestinationInfo info) throws Exception {
        info.setDestination(convertDestination(info.getDestination()));
        super.addDestinationInfo(context, info);    
    }

    @Override
    public void removeDestinationInfo(ConnectionContext context, DestinationInfo info) throws Exception {
        info.setDestination(convertDestination(info.getDestination()));
        super.removeDestinationInfo(context, info);    
    }

    @Override
    public void processConsumerControl(ConsumerBrokerExchange consumerExchange, ConsumerControl control) {
        control.setDestination(convertDestination(control.getDestination()));
        super.processConsumerControl(consumerExchange, control);
    }

    @Override
    public Response messagePull(ConnectionContext context, MessagePull pull) throws Exception {
        pull.setDestination(convertDestination(pull.getDestination()));
        return super.messagePull(context, pull);
    }

    public void setPathSeparator(String pathSeparator) {
        this.pathSeparator = pathSeparator;
    }
}
