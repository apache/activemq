/**
 *
 * Copyright 2005-2006 The Apache Software Foundation
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
 */
package org.apache.activemq.advisory;

import java.util.Iterator;

import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.BrokerFilter;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.broker.region.Destination;
import org.apache.activemq.broker.region.Subscription;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQMessage;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.command.Command;
import org.apache.activemq.command.ConnectionInfo;
import org.apache.activemq.command.ConsumerId;
import org.apache.activemq.command.ConsumerInfo;
import org.apache.activemq.command.DestinationInfo;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.command.ProducerId;
import org.apache.activemq.command.ProducerInfo;
import org.apache.activemq.util.IdGenerator;
import org.apache.activemq.util.LongSequenceGenerator;

import edu.emory.mathcs.backport.java.util.concurrent.ConcurrentHashMap;

/**
 * This broker filter handles tracking the state of the broker for purposes of publishing advisory messages
 * to advisory consumers.
 * 
 * @version $Revision$
 */
public class AdvisoryBroker extends BrokerFilter {
    
    //private static final Log log = LogFactory.getLog(AdvisoryBroker.class);
    
    protected final ConcurrentHashMap connections = new ConcurrentHashMap();
    protected final ConcurrentHashMap consumers = new ConcurrentHashMap();
    protected final ConcurrentHashMap producers = new ConcurrentHashMap();
    protected final ConcurrentHashMap destinations = new ConcurrentHashMap();

    static final private IdGenerator idGenerator = new IdGenerator();
    protected final ProducerId advisoryProducerId = new ProducerId();    
    final private LongSequenceGenerator messageIdGenerator = new LongSequenceGenerator();
    
    public AdvisoryBroker(Broker next) {
        super(next);
        advisoryProducerId.setConnectionId(idGenerator.generateId());
    }

    public void addConnection(ConnectionContext context, ConnectionInfo info) throws Exception {
        next.addConnection(context, info);

        ActiveMQTopic topic = AdvisorySupport.getConnectionAdvisoryTopic();
        fireAdvisory(context, topic, info);        
        connections.put(info.getConnectionId(), info);
    }

    public Subscription addConsumer(ConnectionContext context, ConsumerInfo info) throws Exception {
        Subscription answer = next.addConsumer(context, info);

        // Don't advise advisory topics.
        if( !AdvisorySupport.isAdvisoryTopic(info.getDestination()) ) { 
            ActiveMQTopic topic = AdvisorySupport.getConsumerAdvisoryTopic(info.getDestination());
            consumers.put(info.getConsumerId(), info);
            fireConsumerAdvisory(context, topic, info);
        } else {
            
            // We need to replay all the previously collected state objects 
            // for this newly added consumer.            
            if( AdvisorySupport.isConnectionAdvisoryTopic(info.getDestination()) ) {
                // Replay the connections.
                for (Iterator iter = connections.values().iterator(); iter.hasNext();) {
                    ConnectionInfo value = (ConnectionInfo) iter.next();
                    ActiveMQTopic topic = AdvisorySupport.getConnectionAdvisoryTopic();
                    fireAdvisory(context, topic, value, info.getConsumerId());
                }
            }
            
            // We need to replay all the previously collected destination objects 
            // for this newly added consumer.            
            if( AdvisorySupport.isDestinationAdvisoryTopic(info.getDestination()) ) {
                // Replay the destinations.
                for (Iterator iter = destinations.values().iterator(); iter.hasNext();) {
                    DestinationInfo value = (DestinationInfo) iter.next();
                    ActiveMQTopic topic = AdvisorySupport.getDestinationAdvisoryTopic(value.getDestination());
                    fireAdvisory(context, topic, value, info.getConsumerId());
                }
            }

            // Replay the producers.
            if( AdvisorySupport.isProducerAdvisoryTopic(info.getDestination()) ) {
                for (Iterator iter = producers.values().iterator(); iter.hasNext();) {
                    ProducerInfo value = (ProducerInfo) iter.next();
                    ActiveMQTopic topic = AdvisorySupport.getProducerAdvisoryTopic(value.getDestination());
                    fireProducerAdvisory(context, topic, value, info.getConsumerId());
                }
            }
            
            // Replay the consumers.
            if( AdvisorySupport.isConsumerAdvisoryTopic(info.getDestination()) ) {
                for (Iterator iter = consumers.values().iterator(); iter.hasNext();) {
                    ConsumerInfo value = (ConsumerInfo) iter.next();
                    ActiveMQTopic topic = AdvisorySupport.getConsumerAdvisoryTopic(value.getDestination());
                    fireConsumerAdvisory(context, topic, value, info.getConsumerId());
                }
            }
        }
        return answer;
    }

    public void addProducer(ConnectionContext context, ProducerInfo info) throws Exception {
        next.addProducer(context, info);

        // Don't advise advisory topics.        
        if( info.getDestination()!=null && !AdvisorySupport.isAdvisoryTopic(info.getDestination()) ) { 
            ActiveMQTopic topic = AdvisorySupport.getProducerAdvisoryTopic(info.getDestination());
            fireAdvisory(context, topic, info);
            producers.put(info.getProducerId(), info);
        }
    }
    
    public Destination addDestination(ConnectionContext context, ActiveMQDestination destination) throws Exception {
        Destination answer = next.addDestination(context, destination);  
      
        ActiveMQTopic topic = AdvisorySupport.getDestinationAdvisoryTopic(destination);
        DestinationInfo info = new DestinationInfo(context.getConnectionId(), DestinationInfo.ADD_OPERATION_TYPE, destination);
        fireAdvisory(context, topic, info);        
        destinations.put(destination, info);
        return answer;
    }
    
    public void removeDestination(ConnectionContext context, ActiveMQDestination destination, long timeout) throws Exception {
        next.removeDestination(context, destination, timeout);
        ActiveMQTopic topic = AdvisorySupport.getDestinationAdvisoryTopic(destination);
        DestinationInfo info = (DestinationInfo) destinations.remove(destination);
        if( info !=null ) {
            info.setOperationType(DestinationInfo.REMOVE_OPERATION_TYPE);
            fireAdvisory(context, topic, info);
        }
        next.removeDestination(context, AdvisorySupport.getConsumerAdvisoryTopic(info.getDestination()), timeout); 
    }
    
    public void addDestinationInfo(ConnectionContext context,DestinationInfo info) throws Exception{
        ActiveMQDestination destination =  info.getDestination();
        next.addDestinationInfo(context, info);  
        
        ActiveMQTopic topic = AdvisorySupport.getDestinationAdvisoryTopic(destination);
        fireAdvisory(context, topic, info);        
        destinations.put(destination, info);    
    }

    public void removeDestinationInfo(ConnectionContext context,DestinationInfo info) throws Exception{
        next.removeDestinationInfo(context, info);
        ActiveMQTopic topic = AdvisorySupport.getDestinationAdvisoryTopic(info.getDestination());
        fireAdvisory(context, topic, info);
    }

    public void removeConnection(ConnectionContext context, ConnectionInfo info, Throwable error) throws Exception {
        next.removeConnection(context, info, error);

        ActiveMQTopic topic = AdvisorySupport.getConnectionAdvisoryTopic();
        fireAdvisory(context, topic, info.createRemoveCommand());
        connections.remove(info.getConnectionId());
    }

    public void removeConsumer(ConnectionContext context, ConsumerInfo info) throws Exception {
        next.removeConsumer(context, info);

        // Don't advise advisory topics.
        if( !AdvisorySupport.isAdvisoryTopic(info.getDestination()) ) { 
            ActiveMQTopic topic = AdvisorySupport.getConsumerAdvisoryTopic(info.getDestination());
            consumers.remove(info.getConsumerId());
            fireConsumerAdvisory(context, topic, info.createRemoveCommand());
        }
    }

    public void removeProducer(ConnectionContext context, ProducerInfo info) throws Exception {
        next.removeProducer(context, info);

        // Don't advise advisory topics.
        if( info.getDestination()!=null && !AdvisorySupport.isAdvisoryTopic(info.getDestination()) ) { 
            ActiveMQTopic topic = AdvisorySupport.getProducerAdvisoryTopic(info.getDestination());
            producers.remove(info.getProducerId());
            fireProducerAdvisory(context, topic, info.createRemoveCommand());
        }
    }
    
    protected void fireAdvisory(ConnectionContext context, ActiveMQTopic topic, Command command) throws Exception {
        fireAdvisory(context, topic, command, null);
    }
    
    protected void fireAdvisory(ConnectionContext context, ActiveMQTopic topic, Command command, ConsumerId targetConsumerId) throws Exception {
        ActiveMQMessage advisoryMessage = new ActiveMQMessage();
        fireAdvisory(context, topic, command, targetConsumerId, advisoryMessage);
    }
    
    protected void fireConsumerAdvisory(ConnectionContext context, ActiveMQTopic topic, Command command) throws Exception {
        fireConsumerAdvisory(context, topic, command, null);
    }
    protected void fireConsumerAdvisory(ConnectionContext context, ActiveMQTopic topic, Command command, ConsumerId targetConsumerId) throws Exception {
        ActiveMQMessage advisoryMessage = new ActiveMQMessage();
        advisoryMessage.setIntProperty("consumerCount", consumers.size());
        fireAdvisory(context, topic, command, targetConsumerId, advisoryMessage);
    }
    
    protected void fireProducerAdvisory(ConnectionContext context, ActiveMQTopic topic, Command command) throws Exception {
        fireProducerAdvisory(context, topic, command, null);
    }
    protected void fireProducerAdvisory(ConnectionContext context, ActiveMQTopic topic, Command command, ConsumerId targetConsumerId) throws Exception {
        ActiveMQMessage advisoryMessage = new ActiveMQMessage();
        advisoryMessage.setIntProperty("producerCount", producers.size());
        fireAdvisory(context, topic, command, targetConsumerId, advisoryMessage);
    }

    protected void fireAdvisory(ConnectionContext context, ActiveMQTopic topic, Command command, ConsumerId targetConsumerId, ActiveMQMessage advisoryMessage) throws Exception {
        advisoryMessage.setDataStructure(command);
        advisoryMessage.setPersistent(false);
        advisoryMessage.setType(AdvisorySupport.ADIVSORY_MESSAGE_TYPE);
        advisoryMessage.setMessageId(new MessageId(advisoryProducerId, messageIdGenerator.getNextSequenceId()));
        advisoryMessage.setTargetConsumerId(targetConsumerId);

        advisoryMessage.setDestination(topic);
        advisoryMessage.setResponseRequired(false);
        advisoryMessage.setProducerId(advisoryProducerId);
        
        boolean originalFlowControl = context.isProducerFlowControl();
        try {
            context.setProducerFlowControl(false);
            next.send(context, advisoryMessage);
        } finally {
            context.setProducerFlowControl(originalFlowControl);
        }
    }

}