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
package org.activemq.broker.region;

import edu.emory.mathcs.backport.java.util.concurrent.CopyOnWriteArrayList;
import edu.emory.mathcs.backport.java.util.concurrent.CopyOnWriteArraySet;

import org.activemq.broker.Broker;
import org.activemq.broker.Connection;
import org.activemq.broker.ConnectionContext;
import org.activemq.broker.region.policy.PolicyMap;
import org.activemq.command.ActiveMQDestination;
import org.activemq.command.BrokerId;
import org.activemq.command.ConnectionInfo;
import org.activemq.command.ConsumerInfo;
import org.activemq.command.Message;
import org.activemq.command.MessageAck;
import org.activemq.command.ProducerInfo;
import org.activemq.command.RemoveSubscriptionInfo;
import org.activemq.command.SessionInfo;
import org.activemq.command.TransactionId;
import org.activemq.memory.UsageManager;
import org.activemq.store.PersistenceAdapter;
import org.activemq.store.memory.MemoryPersistenceAdapter;
import org.activemq.thread.TaskRunnerFactory;
import org.activemq.util.IdGenerator;
import org.activemq.util.LongSequenceGenerator;

import javax.jms.JMSException;

import java.io.IOException;
import java.util.ArrayList;

/**
 * Routes Broker operations to the correct messaging regions for processing.
 * 
 * @version $Revision$
 */
public class RegionBroker implements Broker {

    private static final IdGenerator brokerIdGenerator = new IdGenerator();

    private final Region queueRegion;
    private final Region topicRegion;
    private final Region tempQueueRegion;
    private final Region tempTopicRegion;
    
    protected final DestinationStatistics destinationStatistics = new DestinationStatistics();
    
    private final CopyOnWriteArrayList connections = new CopyOnWriteArrayList();
    private final CopyOnWriteArraySet destinations = new CopyOnWriteArraySet();

    private final LongSequenceGenerator sequenceGenerator = new LongSequenceGenerator();    
    private BrokerId brokerId;
    private String brokerName;

    public RegionBroker(TaskRunnerFactory taskRunnerFactory, UsageManager memoryManager, PersistenceAdapter adapter) throws IOException {
        this(taskRunnerFactory, memoryManager, createDefaultPersistenceAdapter(memoryManager), null);
    }
    
    public RegionBroker(TaskRunnerFactory taskRunnerFactory, UsageManager memoryManager, PersistenceAdapter adapter, PolicyMap policyMap) throws IOException {
        this.sequenceGenerator.setLastSequenceId( adapter.getLastMessageBrokerSequenceId() );
        
        queueRegion = createQueueRegion(memoryManager, taskRunnerFactory, adapter, policyMap);
        topicRegion = createTopicRegion(memoryManager, taskRunnerFactory, adapter, policyMap);
        
        tempQueueRegion = createTempQueueRegion(memoryManager, taskRunnerFactory);
        tempTopicRegion = createTempTopicRegion(memoryManager, taskRunnerFactory);        
    }

    protected Region createTempTopicRegion(UsageManager memoryManager, TaskRunnerFactory taskRunnerFactory) {
        return new TempTopicRegion(destinationStatistics, memoryManager, taskRunnerFactory);
    }

    protected Region createTempQueueRegion(UsageManager memoryManager, TaskRunnerFactory taskRunnerFactory) {
        return new TempQueueRegion(destinationStatistics, memoryManager, taskRunnerFactory);
    }

    protected Region createTopicRegion(UsageManager memoryManager, TaskRunnerFactory taskRunnerFactory, PersistenceAdapter adapter, PolicyMap policyMap) {
        return new TopicRegion(destinationStatistics, memoryManager, taskRunnerFactory, adapter, policyMap);
    }

    protected Region createQueueRegion(UsageManager memoryManager, TaskRunnerFactory taskRunnerFactory, PersistenceAdapter adapter, PolicyMap policyMap) {
        return new QueueRegion(destinationStatistics, memoryManager, taskRunnerFactory, adapter, policyMap);
    }
    
    private static PersistenceAdapter createDefaultPersistenceAdapter(UsageManager memoryManager) throws IOException {
        return new MemoryPersistenceAdapter();
    }
    
    
    public void start() throws Exception {
    }

    public void stop() throws Exception {
    }

    public void addConnection(ConnectionContext context, ConnectionInfo info) throws Throwable {
        connections.add(context.getConnection());
    }

    public void removeConnection(ConnectionContext context, ConnectionInfo info, Throwable error) throws Throwable {
        connections.remove(context.getConnection());
    }

    public Connection[] getClients() throws Throwable {
        ArrayList l = new ArrayList(connections);
        Connection rc[] = new Connection[l.size()];
        l.toArray(rc);
        return rc;
    }

    public Destination addDestination(ConnectionContext context, ActiveMQDestination destination) throws Throwable {
        if( destinations.contains(destination) )
            throw new JMSException("Destination already exists: "+destination);
        
        Destination answer = null;
        switch(destination.getDestinationType()) {
        case ActiveMQDestination.QUEUE_TYPE:
            answer  = queueRegion.addDestination(context, destination);
            break;
        case ActiveMQDestination.TOPIC_TYPE:
            answer = topicRegion.addDestination(context, destination);
            break;
        case ActiveMQDestination.TEMP_QUEUE_TYPE:
            answer = tempQueueRegion.addDestination(context, destination);
            break;
        case ActiveMQDestination.TEMP_TOPIC_TYPE:
            answer = tempTopicRegion.addDestination(context, destination);
            break;
        default:
            throwUnknownDestinationType(destination);
        }

        destinations.add(destination);
        return answer;
    }

    public void removeDestination(ConnectionContext context, ActiveMQDestination destination, long timeout) throws Throwable {
        if( !destinations.contains(destination) )
            throw new JMSException("Destination does not exist: "+destination);
        
        switch(destination.getDestinationType()) {
        case ActiveMQDestination.QUEUE_TYPE:
            queueRegion.removeDestination(context, destination, timeout);
            break;
        case ActiveMQDestination.TOPIC_TYPE:
            topicRegion.removeDestination(context, destination, timeout);
            break;
        case ActiveMQDestination.TEMP_QUEUE_TYPE:
            tempQueueRegion.removeDestination(context, destination, timeout);
            break;
        case ActiveMQDestination.TEMP_TOPIC_TYPE:
            tempTopicRegion.removeDestination(context, destination, timeout);
            break;
        default:
            throwUnknownDestinationType(destination);
        }
        
        destinations.remove(destination);
    }

    public ActiveMQDestination[] getDestinations() throws Throwable {
        ArrayList l = new ArrayList(destinations);
        ActiveMQDestination rc[] = new ActiveMQDestination[l.size()];
        l.toArray(rc);
        return rc;
    }


    public void addSession(ConnectionContext context, SessionInfo info) throws Throwable {
    }

    public void removeSession(ConnectionContext context, SessionInfo info) throws Throwable {
    }

    public void addProducer(ConnectionContext context, ProducerInfo info) throws Throwable {
    }

    public void removeProducer(ConnectionContext context, ProducerInfo info) throws Throwable {
    }

    public void addConsumer(ConnectionContext context, ConsumerInfo info) throws Throwable {
        ActiveMQDestination destination = info.getDestination();
        switch(destination.getDestinationType()) {
        case ActiveMQDestination.QUEUE_TYPE:
            queueRegion.addConsumer(context, info);
            break;
        case ActiveMQDestination.TOPIC_TYPE:
            topicRegion.addConsumer(context, info);
            break;
        case ActiveMQDestination.TEMP_QUEUE_TYPE:
            tempQueueRegion.addConsumer(context, info);
            break;
        case ActiveMQDestination.TEMP_TOPIC_TYPE:
            tempTopicRegion.addConsumer(context, info);
            break;
        default:
            throwUnknownDestinationType(destination);
        }
    }

    public void removeConsumer(ConnectionContext context, ConsumerInfo info) throws Throwable {
        ActiveMQDestination destination = info.getDestination();
        switch(destination.getDestinationType()) {
        case ActiveMQDestination.QUEUE_TYPE:
            queueRegion.removeConsumer(context, info);
            break;
        case ActiveMQDestination.TOPIC_TYPE:
            topicRegion.removeConsumer(context, info);
            break;
        case ActiveMQDestination.TEMP_QUEUE_TYPE:
            tempQueueRegion.removeConsumer(context, info);
            break;
        case ActiveMQDestination.TEMP_TOPIC_TYPE:
            tempTopicRegion.removeConsumer(context, info);
            break;
        default:
            throwUnknownDestinationType(destination);
        }
    }

    public void removeSubscription(ConnectionContext context, RemoveSubscriptionInfo info) throws Throwable {
        topicRegion.removeSubscription(context, info);
    }

    public void send(ConnectionContext context,  Message message) throws Throwable {
        
        message.getMessageId().setBrokerSequenceId(sequenceGenerator.getNextSequenceId());
        ActiveMQDestination destination = message.getDestination();
        switch(destination.getDestinationType()) {
        case ActiveMQDestination.QUEUE_TYPE:
            queueRegion.send(context, message);
            break;
        case ActiveMQDestination.TOPIC_TYPE:
            topicRegion.send(context, message);
            break;
        case ActiveMQDestination.TEMP_QUEUE_TYPE:
            tempQueueRegion.send(context, message);
            break;
        case ActiveMQDestination.TEMP_TOPIC_TYPE:
            tempTopicRegion.send(context, message);
            break;
        default:
            throwUnknownDestinationType(destination);
        }
    }

    public void acknowledge(ConnectionContext context, MessageAck ack) throws Throwable {
        ActiveMQDestination destination = ack.getDestination();
        switch(destination.getDestinationType()) {
        case ActiveMQDestination.QUEUE_TYPE:
            queueRegion.acknowledge(context, ack);
            break;
        case ActiveMQDestination.TOPIC_TYPE:
            topicRegion.acknowledge(context, ack);
            break;
        case ActiveMQDestination.TEMP_QUEUE_TYPE:
            tempQueueRegion.acknowledge(context, ack);
            break;
        case ActiveMQDestination.TEMP_TOPIC_TYPE:
            tempTopicRegion.acknowledge(context, ack);
            break;
        default:
            throwUnknownDestinationType(destination);
        }
    }

    public TransactionId[] getPreparedTransactions(ConnectionContext context) throws Throwable {
        throw new IllegalAccessException("Transaction operation not implemented by this broker.");
    }

    public void beginTransaction(ConnectionContext context, TransactionId xid) throws Throwable {
        throw new IllegalAccessException("Transaction operation not implemented by this broker.");
    }

    public int prepareTransaction(ConnectionContext context, TransactionId xid) throws Throwable {
        throw new IllegalAccessException("Transaction operation not implemented by this broker.");
    }

    public void rollbackTransaction(ConnectionContext context, TransactionId xid) throws Throwable {
        throw new IllegalAccessException("Transaction operation not implemented by this broker.");
    }

    public void commitTransaction(ConnectionContext context, TransactionId xid, boolean onePhase) throws Throwable {
        throw new IllegalAccessException("Transaction operation not implemented by this broker.");
    }
    
    public void forgetTransaction(ConnectionContext context, TransactionId transactionId) throws Throwable {
        throw new IllegalAccessException("Transaction operation not implemented by this broker.");
    }


    public void gc() {
        queueRegion.gc();
        topicRegion.gc();
    }

    public BrokerId getBrokerId() {
        if( brokerId==null ) {
            // TODO: this should persist the broker id so that subsequent startup
            // uses the same broker id.
            brokerId=new BrokerId(brokerIdGenerator.generateId());
        }
        return brokerId;
    }
    
    public void setBrokerId(BrokerId brokerId) {
        this.brokerId = brokerId;
    }

    public String getBrokerName() {
        if( brokerName==null ) {
            try {
                brokerName = java.net.InetAddress.getLocalHost().getHostName().toLowerCase();
            } catch (Exception e) {
                brokerName="localhost";
            }
        }
        return brokerName;
    }
    
    public void setBrokerName(String brokerName) {
        this.brokerName = brokerName;
    }
	
    public DestinationStatistics getDestinationStatistics() {
        return destinationStatistics;
    }

    protected void throwUnknownDestinationType(ActiveMQDestination destination) throws JMSException {
        throw new JMSException("Unknown destination type: " + destination.getDestinationType());
    }

}
