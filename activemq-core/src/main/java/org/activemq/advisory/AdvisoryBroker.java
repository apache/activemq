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
package org.activemq.advisory;

import java.util.Iterator;

import org.activemq.broker.Broker;
import org.activemq.broker.BrokerFilter;
import org.activemq.broker.ConnectionContext;
import org.activemq.broker.region.Destination;
import org.activemq.command.ActiveMQDestination;
import org.activemq.command.ActiveMQMessage;
import org.activemq.command.ActiveMQTopic;
import org.activemq.command.Command;
import org.activemq.command.ConnectionInfo;
import org.activemq.command.ConsumerId;
import org.activemq.command.ConsumerInfo;
import org.activemq.command.DestinationInfo;
import org.activemq.command.MessageId;
import org.activemq.command.ProducerId;
import org.activemq.command.ProducerInfo;
import org.activemq.util.IdGenerator;
import org.activemq.util.LongSequenceGenerator;

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

    public void addConnection(ConnectionContext context, ConnectionInfo info) throws Throwable {
        next.addConnection(context, info);

        ActiveMQTopic topic = AdvisorySupport.getConnectionAdvisoryTopic();
        fireAdvisory(context, topic, info);        
        connections.put(info.getConnectionId(), info);
    }

    public void addConsumer(ConnectionContext context, ConsumerInfo info) throws Throwable {
        next.addConsumer(context, info);
        
        // Don't advise advisory topics.
        if( !AdvisorySupport.isAdvisoryTopic(info.getDestination()) ) { 
            ActiveMQTopic topic = AdvisorySupport.getConsumerAdvisoryTopic(info.getDestination());
            fireAdvisory(context, topic, info);
            consumers.put(info.getConsumerId(), info);
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
                    fireAdvisory(context, topic, value, info.getConsumerId());
                }
            }
            
            // Replay the consumers.
            if( AdvisorySupport.isConsumerAdvisoryTopic(info.getDestination()) ) {
                for (Iterator iter = consumers.values().iterator(); iter.hasNext();) {
                    ConsumerInfo value = (ConsumerInfo) iter.next();
                    ActiveMQTopic topic = AdvisorySupport.getConsumerAdvisoryTopic(value.getDestination());
                    fireAdvisory(context, topic, value, info.getConsumerId());
                }
            }
        }
    }

    public void addProducer(ConnectionContext context, ProducerInfo info) throws Throwable {
        next.addProducer(context, info);

        // Don't advise advisory topics.        
        if( info.getDestination()!=null && !AdvisorySupport.isAdvisoryTopic(info.getDestination()) ) { 
            ActiveMQTopic topic = AdvisorySupport.getProducerAdvisoryTopic(info.getDestination());
            fireAdvisory(context, topic, info);
            producers.put(info.getProducerId(), info);
        }
    }
    
    public Destination addDestination(ConnectionContext context, ActiveMQDestination destination) throws Throwable {
        Destination answer = next.addDestination(context, destination);        
        ActiveMQTopic topic = AdvisorySupport.getDestinationAdvisoryTopic(destination);
        DestinationInfo info = new DestinationInfo(context.getConnectionId(), DestinationInfo.ADD_OPERATION_TYPE, destination);
        fireAdvisory(context, topic, info);        
        destinations.put(destination, info);
        return answer;
    }
    
    public void removeDestination(ConnectionContext context, ActiveMQDestination destination, long timeout) throws Throwable {
        next.removeDestination(context, destination, timeout);
        ActiveMQTopic topic = AdvisorySupport.getDestinationAdvisoryTopic(destination);
        DestinationInfo info = (DestinationInfo) destinations.remove(destination);
        if( info !=null ) {
            info.setOperationType(DestinationInfo.REMOVE_OPERATION_TYPE);
            fireAdvisory(context, topic, info);
        }
    }

    public void removeConnection(ConnectionContext context, ConnectionInfo info, Throwable error) throws Throwable {
        next.removeConnection(context, info, error);

        ActiveMQTopic topic = AdvisorySupport.getConnectionAdvisoryTopic();
        fireAdvisory(context, topic, info.createRemoveCommand());
        connections.remove(info.getConnectionId());
    }

    public void removeConsumer(ConnectionContext context, ConsumerInfo info) throws Throwable {
        next.removeConsumer(context, info);

        // Don't advise advisory topics.
        if( !AdvisorySupport.isAdvisoryTopic(info.getDestination()) ) { 
            ActiveMQTopic topic = AdvisorySupport.getConsumerAdvisoryTopic(info.getDestination());
            fireAdvisory(context, topic, info.createRemoveCommand());
            consumers.remove(info.getConsumerId());
        }
    }

    public void removeProducer(ConnectionContext context, ProducerInfo info) throws Throwable {
        next.removeProducer(context, info);

        // Don't advise advisory topics.
        if( info.getDestination()!=null && !AdvisorySupport.isAdvisoryTopic(info.getDestination()) ) { 
            ActiveMQTopic topic = AdvisorySupport.getProducerAdvisoryTopic(info.getDestination());
            fireAdvisory(context, topic, info.createRemoveCommand());
            producers.remove(info.getProducerId());
        }
    }
    
    private void fireAdvisory(ConnectionContext context, ActiveMQTopic topic, Command command) throws Throwable {
        fireAdvisory(context, topic, command, null);
    }
    
    private void fireAdvisory(ConnectionContext context, ActiveMQTopic topic, Command command, ConsumerId targetConsumerId) throws Throwable {
        
        ActiveMQMessage advisoryMessage = new ActiveMQMessage();
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