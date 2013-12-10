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
import org.apache.activemq.util.LRUCache;
import org.apache.activemq.util.ServiceStopper;
import org.apache.activemq.util.ServiceSupport;
import org.fusesource.hawtbuf.Buffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MQTTRetainedMessages extends ServiceSupport {
    private static final Logger LOG = LoggerFactory.getLogger(MQTTRetainedMessages.class);
    private static final Object LOCK = new Object();
    private LRUCache<String,Buffer> cache = new LRUCache<String, Buffer>(10000);

    private MQTTRetainedMessages(){
    }

    @Override
    protected void doStop(ServiceStopper stopper) throws Exception {
       cache.clear();
    }

    @Override
    protected void doStart() throws Exception {
    }

   public void addMessage(String destination,Buffer payload){
       cache.put(destination,payload);
   }

   public Buffer getMessage(String destination){
       return cache.get(destination);
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
