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
package org.apache.activemq.broker.region.virtual;

import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.BrokerServiceAware;
import org.apache.activemq.broker.ProducerBrokerExchange;
import org.apache.activemq.broker.region.Destination;
import org.apache.activemq.broker.region.DestinationFilter;
import org.apache.activemq.broker.region.DestinationInterceptor;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.command.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Creates <a href="http://activemq.org/site/mirrored-queues.html">Mirrored
 * Queue</a> using a prefix and postfix to define the topic name on which to mirror the queue to.
 *
 * 
 * @org.apache.xbean.XBean
 */
public class MirroredQueue implements DestinationInterceptor, BrokerServiceAware {
    private static final transient Logger LOG = LoggerFactory.getLogger(MirroredQueue.class);
    private String prefix = "VirtualTopic.Mirror.";
    private String postfix = "";
    private boolean copyMessage = true;
    private BrokerService brokerService;

    public Destination intercept(final Destination destination) {
        if (destination.getActiveMQDestination().isQueue()) {
            if (!destination.getActiveMQDestination().isTemporary() || brokerService.isUseTempMirroredQueues()) {
                try {
                    final Destination mirrorDestination = getMirrorDestination(destination);
                    if (mirrorDestination != null) {
                        return new DestinationFilter(destination) {
                            public void send(ProducerBrokerExchange context, Message message) throws Exception {
                                message.setDestination(mirrorDestination.getActiveMQDestination());
                                mirrorDestination.send(context, message);
    
                                if (isCopyMessage()) {
                                    message = message.copy();
                                }
                                message.setDestination(destination.getActiveMQDestination());
                                super.send(context, message);
                            }
                        };
                    }
                }
                catch (Exception e) {
                    LOG.error("Failed to lookup the mirror destination for: " + destination + ". Reason: " + e, e);
                }
            }
        }
        return destination;
    }
    

    public void remove(Destination destination) {
        if (brokerService == null) {
            throw new IllegalArgumentException("No brokerService injected!");
        }
        ActiveMQDestination topic = getMirrorTopic(destination.getActiveMQDestination());
        if (topic != null) {
            try {
                brokerService.removeDestination(topic);
            } catch (Exception e) {
                LOG.error("Failed to remove mirror destination for " + destination + ". Reason: " + e,e);
            }
        }
        
    }

    // Properties
    // -------------------------------------------------------------------------

    public String getPostfix() {
        return postfix;
    }

    /**
     * Sets any postix used to identify the queue consumers
     */
    public void setPostfix(String postfix) {
        this.postfix = postfix;
    }

    public String getPrefix() {
        return prefix;
    }

    /**
     * Sets the prefix wildcard used to identify the queue consumers for a given
     * topic
     */
    public void setPrefix(String prefix) {
        this.prefix = prefix;
    }

    public boolean isCopyMessage() {
        return copyMessage;
    }

    /**
     * Sets whether a copy of the message will be sent to each destination.
     * Defaults to true so that the forward destination is set as the
     * destination of the message
     */
    public void setCopyMessage(boolean copyMessage) {
        this.copyMessage = copyMessage;
    }

    public void setBrokerService(BrokerService brokerService) {
        this.brokerService = brokerService;
    }

    // Implementation methods
    //-------------------------------------------------------------------------
    protected Destination getMirrorDestination(Destination destination) throws Exception {
        if (brokerService == null) {
            throw new IllegalArgumentException("No brokerService injected!");
        }
        ActiveMQDestination topic = getMirrorTopic(destination.getActiveMQDestination());
        return brokerService.getDestination(topic);
    }

    protected ActiveMQDestination getMirrorTopic(ActiveMQDestination original) {
        return new ActiveMQTopic(prefix + original.getPhysicalName() + postfix);
    }

}
