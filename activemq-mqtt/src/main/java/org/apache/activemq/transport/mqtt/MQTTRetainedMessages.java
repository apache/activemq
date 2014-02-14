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

import org.apache.activemq.Service;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.filter.DestinationMapNode;
import org.apache.activemq.util.ServiceStopper;
import org.apache.activemq.util.ServiceSupport;
import org.fusesource.hawtbuf.Buffer;
import org.fusesource.hawtbuf.UTF8Buffer;
import org.fusesource.mqtt.codec.PUBLISH;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;

public class MQTTRetainedMessages extends ServiceSupport {
    private static final Logger LOG = LoggerFactory.getLogger(MQTTRetainedMessages.class);
    private static final Object LOCK = new Object();

    DestinationMapNode retainedMessages = new DestinationMapNode(null);

    private MQTTRetainedMessages(){
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

   public Set<PUBLISH> getMessages(ActiveMQTopic topic){
       Set answer = new HashSet();
       synchronized (this) {
           retainedMessages.appendMatchingValues(answer, topic.getDestinationPaths(), 0);
       }
       return (Set<PUBLISH>)answer;
   }

    public static MQTTRetainedMessages getMQTTRetainedMessages(BrokerService broker){
        MQTTRetainedMessages result = null;
        if (broker != null){
            synchronized (LOCK){
               Service[] services = broker.getServices();
               if (services != null){
                   for (Service service:services){
                       if (service instanceof MQTTRetainedMessages){
                           return (MQTTRetainedMessages) service;
                       }
                   }
               }
               result = new MQTTRetainedMessages();
                broker.addService(result);
                if (broker != null && broker.isStarted()){
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
