/**
* <a href="http://activemq.org">ActiveMQ: The Open Source Message Fabric</a>
*
* Copyright 2005 (C) LogicBlaze, Inc. http://www.logicblaze.com
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*
**/

package org.activemq.state;

import java.util.Collection;
import java.util.Set;

import org.activemq.command.ConsumerId;
import org.activemq.command.ConsumerInfo;
import org.activemq.command.ProducerId;
import org.activemq.command.ProducerInfo;
import org.activemq.command.SessionInfo;

import edu.emory.mathcs.backport.java.util.concurrent.ConcurrentHashMap;

public class SessionState {        
    final SessionInfo info;
    
    public final ConcurrentHashMap producers = new ConcurrentHashMap();
    public final ConcurrentHashMap consumers = new ConcurrentHashMap();
    
    public SessionState(SessionInfo info) {
        this.info = info;
    }        
    public String toString() {
        return info.toString();
    }
    
    public void addProducer(ProducerInfo info) {
        producers.put(info.getProducerId(), new ProducerState(info));            
    }        
    public ProducerState removeProducer(ProducerId id) {
        return (ProducerState) producers.remove(id);
    }
    
    public void addConsumer(ConsumerInfo info) {
        consumers.put(info.getConsumerId(), new ConsumerState(info));            
    }        
    public ConsumerState removeConsumer(ConsumerId id) {
        return (ConsumerState) consumers.remove(id);
    }
    
    public SessionInfo getInfo() {
        return info;
    }
    
    public Set getConsumerIds() {
        return consumers.keySet();
    }                
    public Set getProducerIds() {
        return producers.keySet();
    }
    
    public Collection getProducerStates() {
        return producers.values();
    }
    
    public Collection getConsumerStates() {
        return consumers.values();
    }        
}