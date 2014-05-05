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
package org.apache.activemq.transport.mqtt;

import java.util.HashSet;
import java.util.Set;

import org.apache.activemq.Service;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.filter.DestinationMapNode;
import org.apache.activemq.security.AuthorizationBroker;
import org.apache.activemq.security.AuthorizationMap;
import org.apache.activemq.security.SecurityContext;
import org.apache.activemq.util.ServiceStopper;
import org.apache.activemq.util.ServiceSupport;
import org.fusesource.mqtt.codec.PUBLISH;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MQTTRetainedMessages extends ServiceSupport {

    private static final Logger LOG = LoggerFactory.getLogger(MQTTRetainedMessages.class);
    private static final Object LOCK = new Object();

    private final AuthorizationMap authorizationMap;

    DestinationMapNode retainedMessages = new DestinationMapNode(null);

    private MQTTRetainedMessages(BrokerService brokerService) throws Exception {
        AuthorizationBroker authorizationBroker = (AuthorizationBroker) brokerService.getBroker().getAdaptor(AuthorizationBroker.class);
        if (authorizationBroker != null) {
            authorizationMap = authorizationBroker.getAuthorizationMap();
        } else {
            authorizationMap = null;
        }
    }

    @Override
    protected void doStop(ServiceStopper stopper) throws Exception {
        synchronized (this) {
            retainedMessages = new DestinationMapNode(null);
        }
    }

    @Override
    protected void doStart() throws Exception {
    }

   public void addMessage(ActiveMQTopic dest, PUBLISH publish){
       synchronized (this) {
           retainedMessages.set(dest.getDestinationPaths(), 0, publish);
       }
   }

   public Set<PUBLISH> getMessages(ActiveMQTopic topic, SecurityContext context) {
       final Set<PUBLISH> answer = new HashSet<PUBLISH>();
       synchronized (this) {
           // get matching RetainedMessages that match topic path
           final Set nodes = new HashSet();
           retainedMessages.appendMatchingValues(nodes, topic.getDestinationPaths(), 0);

           // if AuthorizationBroker is in use, authorize messages for topic using SecurityContext
           if (authorizationMap != null) {
               if (context == null) {
                   throw new SecurityException("User is not authenticated.");
               }

               for (Object node : nodes) {
                   final PUBLISH publish = (PUBLISH) node;

                   final ActiveMQTopic publishTopic = new ActiveMQTopic(publish.topicName().toString());
                   final Set<?> allowedACLs = authorizationMap.getReadACLs(publishTopic);
                   if (!context.isBrokerContext() && allowedACLs != null && !context.isInOneOf(allowedACLs)) {
                       // not authorized, skip this message
                       continue;
                   }
                   answer.add(publish);
               }
           } else {
               // no authorization, return all matched messages
               answer.addAll(nodes);
               nodes.clear();
           }
       }

       return answer;
   }

    public static MQTTRetainedMessages getMQTTRetainedMessages(BrokerService broker) throws Exception {
        MQTTRetainedMessages result = null;
        if (broker != null){
            synchronized (LOCK) {
                Service[] services = broker.getServices();
                if (services != null) {
                    for (Service service : services) {
                        if (service instanceof MQTTRetainedMessages) {
                            return (MQTTRetainedMessages) service;
                        }
                    }
                }
                result = new MQTTRetainedMessages(broker);
                broker.addService(result);
                if (broker != null && broker.isStarted()) {
                    try {
                        result.start();
                    } catch (Exception e) {
                        LOG.warn("Couldn't start MQTTRetainedMessages");
                    }
                }
            }
        }

        return result;
    }

}
