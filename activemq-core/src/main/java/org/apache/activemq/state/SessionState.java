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

package org.apache.activemq.state;

import java.util.Collection;
import java.util.Set;

import org.apache.activemq.command.ConsumerId;
import org.apache.activemq.command.ConsumerInfo;
import org.apache.activemq.command.ProducerId;
import org.apache.activemq.command.ProducerInfo;
import org.apache.activemq.command.SessionInfo;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

public class SessionState {        
    final SessionInfo info;
    
    public final ConcurrentHashMap producers = new ConcurrentHashMap();
    public final ConcurrentHashMap consumers = new ConcurrentHashMap();
    private final AtomicBoolean shutdown = new AtomicBoolean(false);
    
    public SessionState(SessionInfo info) {
        this.info = info;
    }        
    public String toString() {
        return info.toString();
    }
    
    public void addProducer(ProducerInfo info) {
    	checkShutdown();
        producers.put(info.getProducerId(), new ProducerState(info));            
    }        
    public ProducerState removeProducer(ProducerId id) {
        return (ProducerState) producers.remove(id);
    }
    
    public void addConsumer(ConsumerInfo info) {
    	checkShutdown();
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
	public ProducerState getProducerState(ProducerId producerId) {
		return (ProducerState) producers.get(producerId);
	}
    
    public Collection getConsumerStates() {
        return consumers.values();
    }
    
    public ConsumerState getConsumerState(ConsumerId consumerId) {
        return (ConsumerState)consumers.get(consumerId);
    }
    
    private void checkShutdown() {
		if( shutdown.get() )
			throw new IllegalStateException("Disposed");
	}
    
    public void shutdown() {
    	shutdown.set(false);
    }

}
