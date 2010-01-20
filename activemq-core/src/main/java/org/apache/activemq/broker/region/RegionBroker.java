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
package org.apache.activemq.broker.region;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import javax.jms.InvalidClientIDException;
import javax.jms.JMSException;
import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.Connection;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.broker.ConsumerBrokerExchange;
import org.apache.activemq.broker.EmptyBroker;
import org.apache.activemq.broker.ProducerBrokerExchange;
import org.apache.activemq.broker.region.policy.DeadLetterStrategy;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.BrokerId;
import org.apache.activemq.command.BrokerInfo;
import org.apache.activemq.command.ConnectionId;
import org.apache.activemq.command.ConnectionInfo;
import org.apache.activemq.command.ConsumerInfo;
import org.apache.activemq.command.DestinationInfo;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.MessageAck;
import org.apache.activemq.command.MessageDispatch;
import org.apache.activemq.command.MessageDispatchNotification;
import org.apache.activemq.command.MessagePull;
import org.apache.activemq.command.ProducerInfo;
import org.apache.activemq.command.RemoveSubscriptionInfo;
import org.apache.activemq.command.Response;
import org.apache.activemq.command.TransactionId;
import org.apache.activemq.state.ConnectionState;
import org.apache.activemq.store.kahadb.plist.PListStore;
import org.apache.activemq.thread.TaskRunnerFactory;
import org.apache.activemq.usage.SystemUsage;
import org.apache.activemq.util.BrokerSupport;
import org.apache.activemq.util.IdGenerator;
import org.apache.activemq.util.LongSequenceGenerator;
import org.apache.activemq.util.ServiceStopper;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Routes Broker operations to the correct messaging regions for processing.
 * 
 * @version $Revision$
 */
public class RegionBroker extends EmptyBroker {
    public static final String ORIGINAL_EXPIRATION = "originalExpiration";
    private static final Log LOG = LogFactory.getLog(RegionBroker.class);
    private static final IdGenerator BROKER_ID_GENERATOR = new IdGenerator();

    protected final DestinationStatistics destinationStatistics = new DestinationStatistics();
    protected DestinationFactory destinationFactory;
    protected final Map<ConnectionId, ConnectionState> connectionStates = Collections.synchronizedMap(new HashMap<ConnectionId, ConnectionState>());

    private final Region queueRegion;
    private final Region topicRegion;
    private final Region tempQueueRegion;
    private final Region tempTopicRegion;
    protected final BrokerService brokerService;
    private boolean started;
    private boolean keepDurableSubsActive;

    private final CopyOnWriteArrayList<Connection> connections = new CopyOnWriteArrayList<Connection>();
    private final Map<ActiveMQDestination, Destination> destinations = new ConcurrentHashMap<ActiveMQDestination, Destination>();
    private final CopyOnWriteArrayList<BrokerInfo> brokerInfos = new CopyOnWriteArrayList<BrokerInfo>();

    private final LongSequenceGenerator sequenceGenerator = new LongSequenceGenerator();
    private BrokerId brokerId;
    private String brokerName;
    private final Map<String, ConnectionContext> clientIdSet = new HashMap<String, ConnectionContext>();
    private final DestinationInterceptor destinationInterceptor;
    private ConnectionContext adminConnectionContext;

    public RegionBroker(BrokerService brokerService, TaskRunnerFactory taskRunnerFactory, SystemUsage memoryManager, DestinationFactory destinationFactory,
                        DestinationInterceptor destinationInterceptor) throws IOException {
        this.brokerService = brokerService;
        if (destinationFactory == null) {
            throw new IllegalArgumentException("null destinationFactory");
        }
        this.sequenceGenerator.setLastSequenceId(destinationFactory.getLastMessageBrokerSequenceId());
        this.destinationFactory = destinationFactory;
        queueRegion = createQueueRegion(memoryManager, taskRunnerFactory, destinationFactory);
        topicRegion = createTopicRegion(memoryManager, taskRunnerFactory, destinationFactory);
        this.destinationInterceptor = destinationInterceptor;
        tempQueueRegion = createTempQueueRegion(memoryManager, taskRunnerFactory, destinationFactory);
        tempTopicRegion = createTempTopicRegion(memoryManager, taskRunnerFactory, destinationFactory);
    }

    @Override
    public Map<ActiveMQDestination, Destination> getDestinationMap() {
        Map<ActiveMQDestination, Destination> answer = getQueueRegion().getDestinationMap();
        answer.putAll(getTopicRegion().getDestinationMap());
        return answer;
    }

    @Override
    public Set <Destination> getDestinations(ActiveMQDestination destination) {
        switch (destination.getDestinationType()) {
        case ActiveMQDestination.QUEUE_TYPE:
            return queueRegion.getDestinations(destination);
        case ActiveMQDestination.TOPIC_TYPE:
            return topicRegion.getDestinations(destination);
        case ActiveMQDestination.TEMP_QUEUE_TYPE:
            return tempQueueRegion.getDestinations(destination);
        case ActiveMQDestination.TEMP_TOPIC_TYPE:
            return tempTopicRegion.getDestinations(destination);
        default:
            return Collections.emptySet();
        }
    }

    @Override
    public Broker getAdaptor(Class type) {
        if (type.isInstance(this)) {
            return this;
        }
        return null;
    }

    public Region getQueueRegion() {
        return queueRegion;
    }

    public Region getTempQueueRegion() {
        return tempQueueRegion;
    }

    public Region getTempTopicRegion() {
        return tempTopicRegion;
    }

    public Region getTopicRegion() {
        return topicRegion;
    }

    protected Region createTempTopicRegion(SystemUsage memoryManager, TaskRunnerFactory taskRunnerFactory, DestinationFactory destinationFactory) {
        return new TempTopicRegion(this, destinationStatistics, memoryManager, taskRunnerFactory, destinationFactory);
    }

    protected Region createTempQueueRegion(SystemUsage memoryManager, TaskRunnerFactory taskRunnerFactory, DestinationFactory destinationFactory) {
        return new TempQueueRegion(this, brokerService, destinationStatistics, memoryManager, taskRunnerFactory, destinationFactory);
    }

    protected Region createTopicRegion(SystemUsage memoryManager, TaskRunnerFactory taskRunnerFactory, DestinationFactory destinationFactory) {
        return new TopicRegion(this, destinationStatistics, memoryManager, taskRunnerFactory, destinationFactory);
    }

    protected Region createQueueRegion(SystemUsage memoryManager, TaskRunnerFactory taskRunnerFactory, DestinationFactory destinationFactory) {
        return new QueueRegion(this, destinationStatistics, memoryManager, taskRunnerFactory, destinationFactory);
    }

    @Override
    public void start() throws Exception {
        ((TopicRegion)topicRegion).setKeepDurableSubsActive(keepDurableSubsActive);
        started = true;
        queueRegion.start();
        topicRegion.start();
        tempQueueRegion.start();
        tempTopicRegion.start();
    }

    @Override
    public void stop() throws Exception {
        started = false;
        ServiceStopper ss = new ServiceStopper();
        doStop(ss);
        ss.throwFirstException();
        // clear the state
        clientIdSet.clear();
        connections.clear();
        destinations.clear();
        brokerInfos.clear();
    }

    public PolicyMap getDestinationPolicy() {
        return brokerService != null ? brokerService.getDestinationPolicy() : null;
    }

    @Override
    public void addConnection(ConnectionContext context, ConnectionInfo info) throws Exception {
        String clientId = info.getClientId();
        if (clientId == null) {
            throw new InvalidClientIDException("No clientID specified for connection request");
        }
        synchronized (clientIdSet) {
            ConnectionContext oldContext = clientIdSet.get(clientId);
            if (oldContext != null) {
            	if (context.isFaultTolerant() || context.isNetworkConnection()){
            		//remove the old connection
            		try{
            			removeConnection(oldContext, info, new Exception("remove stale client"));
            		}catch(Exception e){
            			LOG.warn("Failed to remove stale connection ",e);
            		}
            	}else{
                throw new InvalidClientIDException("Broker: " + getBrokerName() + " - Client: " + clientId + " already connected from "
                                                   + oldContext.getConnection().getRemoteAddress());
            	}
            } else {
                clientIdSet.put(clientId, context);
            }
        }

        connections.add(context.getConnection());
    }

    @Override
    public void removeConnection(ConnectionContext context, ConnectionInfo info, Throwable error) throws Exception {
        String clientId = info.getClientId();
        if (clientId == null) {
            throw new InvalidClientIDException("No clientID specified for connection disconnect request");
        }
        synchronized (clientIdSet) {
            ConnectionContext oldValue = clientIdSet.get(clientId);
            // we may be removing the duplicate connection, not the first
            // connection to be created
            // so lets check that their connection IDs are the same
            if (oldValue == context) {
                if (isEqual(oldValue.getConnectionId(), info.getConnectionId())) {
                    clientIdSet.remove(clientId);
                }
            }
        }
        connections.remove(context.getConnection());
    }

    protected boolean isEqual(ConnectionId connectionId, ConnectionId connectionId2) {
        return connectionId == connectionId2 || (connectionId != null && connectionId.equals(connectionId2));
    }

    @Override
    public Connection[] getClients() throws Exception {
        ArrayList<Connection> l = new ArrayList<Connection>(connections);
        Connection rc[] = new Connection[l.size()];
        l.toArray(rc);
        return rc;
    }

    @Override
    public Destination addDestination(ConnectionContext context, ActiveMQDestination destination) throws Exception {

        Destination answer;

        answer = destinations.get(destination);
        if (answer != null) {
            return answer;
        }

        switch (destination.getDestinationType()) {
        case ActiveMQDestination.QUEUE_TYPE:
            answer = queueRegion.addDestination(context, destination);
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
            throw createUnknownDestinationTypeException(destination);
        }

        destinations.put(destination, answer);
        return answer;

    }

    @Override
    public void removeDestination(ConnectionContext context, ActiveMQDestination destination, long timeout) throws Exception {

        if (destinations.containsKey(destination)) {
            switch (destination.getDestinationType()) {
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
                throw createUnknownDestinationTypeException(destination);
            }
            destinations.remove(destination);
        }

    }

    @Override
    public void addDestinationInfo(ConnectionContext context, DestinationInfo info) throws Exception {
        addDestination(context, info.getDestination());

    }

    @Override
    public void removeDestinationInfo(ConnectionContext context, DestinationInfo info) throws Exception {
        removeDestination(context, info.getDestination(), info.getTimeout());

    }

    @Override
    public ActiveMQDestination[] getDestinations() throws Exception {
        ArrayList<ActiveMQDestination> l;

        l = new ArrayList<ActiveMQDestination>(getDestinationMap().keySet());

        ActiveMQDestination rc[] = new ActiveMQDestination[l.size()];
        l.toArray(rc);
        return rc;
    }

    @Override
    public void addProducer(ConnectionContext context, ProducerInfo info)
            throws Exception {
        ActiveMQDestination destination = info.getDestination();
        if (destination != null) {

            // This seems to cause the destination to be added but without advisories firing...
            context.getBroker().addDestination(context, destination);
            switch (destination.getDestinationType()) {
            case ActiveMQDestination.QUEUE_TYPE:
                queueRegion.addProducer(context, info);
                break;
            case ActiveMQDestination.TOPIC_TYPE:
                topicRegion.addProducer(context, info);
                break;
            case ActiveMQDestination.TEMP_QUEUE_TYPE:
                tempQueueRegion.addProducer(context, info);
                break;
            case ActiveMQDestination.TEMP_TOPIC_TYPE:
                tempTopicRegion.addProducer(context, info);
                break;
            }
        }
    }

    @Override
    public void removeProducer(ConnectionContext context, ProducerInfo info) throws Exception {
        ActiveMQDestination destination = info.getDestination();
        if (destination != null) {
            switch (destination.getDestinationType()) {
            case ActiveMQDestination.QUEUE_TYPE:
                queueRegion.removeProducer(context, info);
                break;
            case ActiveMQDestination.TOPIC_TYPE:
                topicRegion.removeProducer(context, info);
                break;
            case ActiveMQDestination.TEMP_QUEUE_TYPE:
                tempQueueRegion.removeProducer(context, info);
                break;
            case ActiveMQDestination.TEMP_TOPIC_TYPE:
                tempTopicRegion.removeProducer(context, info);
                break;
            }
        }
    }

    @Override
    public Subscription addConsumer(ConnectionContext context, ConsumerInfo info) throws Exception {
        ActiveMQDestination destination = info.getDestination();
        switch (destination.getDestinationType()) {
        case ActiveMQDestination.QUEUE_TYPE:
            return queueRegion.addConsumer(context, info);

        case ActiveMQDestination.TOPIC_TYPE:
            return topicRegion.addConsumer(context, info);

        case ActiveMQDestination.TEMP_QUEUE_TYPE:
            return tempQueueRegion.addConsumer(context, info);

        case ActiveMQDestination.TEMP_TOPIC_TYPE:
            return tempTopicRegion.addConsumer(context, info);

        default:
            throw createUnknownDestinationTypeException(destination);
        }
    }

    @Override
    public void removeConsumer(ConnectionContext context, ConsumerInfo info) throws Exception {
        ActiveMQDestination destination = info.getDestination();
        switch (destination.getDestinationType()) {
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
            throw createUnknownDestinationTypeException(destination);
        }
    }

    @Override
    public void removeSubscription(ConnectionContext context, RemoveSubscriptionInfo info) throws Exception {
        topicRegion.removeSubscription(context, info);
    }

    @Override
    public void send(ProducerBrokerExchange producerExchange, Message message) throws Exception {
        message.setBrokerInTime(System.currentTimeMillis());
        if (producerExchange.isMutable() || producerExchange.getRegion() == null) {
            ActiveMQDestination destination = message.getDestination();
            // ensure the destination is registered with the RegionBroker
            producerExchange.getConnectionContext().getBroker().addDestination(producerExchange.getConnectionContext(), destination);
            Region region;
            switch (destination.getDestinationType()) {
            case ActiveMQDestination.QUEUE_TYPE:
                region = queueRegion;
                break;
            case ActiveMQDestination.TOPIC_TYPE:
                region = topicRegion;
                break;
            case ActiveMQDestination.TEMP_QUEUE_TYPE:
                region = tempQueueRegion;
                break;
            case ActiveMQDestination.TEMP_TOPIC_TYPE:
                region = tempTopicRegion;
                break;
            default:
                throw createUnknownDestinationTypeException(destination);
            }
            producerExchange.setRegion(region);
        }
        producerExchange.getRegion().send(producerExchange, message);
    }

    @Override
    public void acknowledge(ConsumerBrokerExchange consumerExchange, MessageAck ack) throws Exception {
        if (consumerExchange.isWildcard() || consumerExchange.getRegion() == null) {
            ActiveMQDestination destination = ack.getDestination();
            Region region;
            switch (destination.getDestinationType()) {
            case ActiveMQDestination.QUEUE_TYPE:
                region = queueRegion;
                break;
            case ActiveMQDestination.TOPIC_TYPE:
                region = topicRegion;
                break;
            case ActiveMQDestination.TEMP_QUEUE_TYPE:
                region = tempQueueRegion;
                break;
            case ActiveMQDestination.TEMP_TOPIC_TYPE:
                region = tempTopicRegion;
                break;
            default:
                throw createUnknownDestinationTypeException(destination);
            }
            consumerExchange.setRegion(region);
        }
        consumerExchange.getRegion().acknowledge(consumerExchange, ack);
    }

    @Override
    public Response messagePull(ConnectionContext context, MessagePull pull) throws Exception {
        ActiveMQDestination destination = pull.getDestination();
        switch (destination.getDestinationType()) {
        case ActiveMQDestination.QUEUE_TYPE:
            return queueRegion.messagePull(context, pull);

        case ActiveMQDestination.TOPIC_TYPE:
            return topicRegion.messagePull(context, pull);

        case ActiveMQDestination.TEMP_QUEUE_TYPE:
            return tempQueueRegion.messagePull(context, pull);

        case ActiveMQDestination.TEMP_TOPIC_TYPE:
            return tempTopicRegion.messagePull(context, pull);
        default:
            throw createUnknownDestinationTypeException(destination);
        }
    }

    @Override
    public TransactionId[] getPreparedTransactions(ConnectionContext context) throws Exception {
        throw new IllegalAccessException("Transaction operation not implemented by this broker.");
    }

    @Override
    public void beginTransaction(ConnectionContext context, TransactionId xid) throws Exception {
        throw new IllegalAccessException("Transaction operation not implemented by this broker.");
    }

    @Override
    public int prepareTransaction(ConnectionContext context, TransactionId xid) throws Exception {
        throw new IllegalAccessException("Transaction operation not implemented by this broker.");
    }

    @Override
    public void rollbackTransaction(ConnectionContext context, TransactionId xid) throws Exception {
        throw new IllegalAccessException("Transaction operation not implemented by this broker.");
    }

    @Override
    public void commitTransaction(ConnectionContext context, TransactionId xid, boolean onePhase) throws Exception {
        throw new IllegalAccessException("Transaction operation not implemented by this broker.");
    }

    @Override
    public void forgetTransaction(ConnectionContext context, TransactionId transactionId) throws Exception {
        throw new IllegalAccessException("Transaction operation not implemented by this broker.");
    }

    @Override
    public void gc() {
        queueRegion.gc();
        topicRegion.gc();
    }

    @Override
    public BrokerId getBrokerId() {
        if (brokerId == null) {
            brokerId = new BrokerId(BROKER_ID_GENERATOR.generateId());
        }
        return brokerId;
    }

    public void setBrokerId(BrokerId brokerId) {
        this.brokerId = brokerId;
    }

    @Override
    public String getBrokerName() {
        if (brokerName == null) {
            try {
                brokerName = java.net.InetAddress.getLocalHost().getHostName().toLowerCase();
            } catch (Exception e) {
                brokerName = "localhost";
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

    protected JMSException createUnknownDestinationTypeException(ActiveMQDestination destination) {
        return new JMSException("Unknown destination type: " + destination.getDestinationType());
    }

    @Override
    public synchronized void addBroker(Connection connection, BrokerInfo info) {
        brokerInfos.add(info);
    }

    @Override
    public synchronized void removeBroker(Connection connection, BrokerInfo info) {
        if (info != null) {
            brokerInfos.remove(info);
        }
    }

    @Override
    public synchronized BrokerInfo[] getPeerBrokerInfos() {
        BrokerInfo[] result = new BrokerInfo[brokerInfos.size()];
        result = brokerInfos.toArray(result);
        return result;
    }

    @Override
    public void preProcessDispatch(MessageDispatch messageDispatch) {
        Message message = messageDispatch.getMessage();
        if (message != null) {
            long endTime = System.currentTimeMillis();
            message.setBrokerOutTime(endTime);
            if (getBrokerService().isEnableStatistics()) {
                long totalTime = endTime - message.getBrokerInTime();
                message.getRegionDestination().getDestinationStatistics().getProcessTime().addTime(totalTime);
            }
        }
    }

    @Override
    public void postProcessDispatch(MessageDispatch messageDispatch) {
    }

    @Override
    public void processDispatchNotification(MessageDispatchNotification messageDispatchNotification) throws Exception {
        ActiveMQDestination destination = messageDispatchNotification.getDestination();
        switch (destination.getDestinationType()) {
        case ActiveMQDestination.QUEUE_TYPE:
            queueRegion.processDispatchNotification(messageDispatchNotification);
            break;
        case ActiveMQDestination.TOPIC_TYPE:
            topicRegion.processDispatchNotification(messageDispatchNotification);
            break;
        case ActiveMQDestination.TEMP_QUEUE_TYPE:
            tempQueueRegion.processDispatchNotification(messageDispatchNotification);
            break;
        case ActiveMQDestination.TEMP_TOPIC_TYPE:
            tempTopicRegion.processDispatchNotification(messageDispatchNotification);
            break;
        default:
            throw createUnknownDestinationTypeException(destination);
        }
    }

    public boolean isSlaveBroker() {
        return brokerService.isSlave();
    }

    @Override
    public boolean isStopped() {
        return !started;
    }

    @Override
    public Set<ActiveMQDestination> getDurableDestinations() {
        return destinationFactory.getDestinations();
    }

    protected void doStop(ServiceStopper ss) {
        ss.stop(queueRegion);
        ss.stop(topicRegion);
        ss.stop(tempQueueRegion);
        ss.stop(tempTopicRegion);
    }

    public boolean isKeepDurableSubsActive() {
        return keepDurableSubsActive;
    }

    public void setKeepDurableSubsActive(boolean keepDurableSubsActive) {
        this.keepDurableSubsActive = keepDurableSubsActive;
    }

    public DestinationInterceptor getDestinationInterceptor() {
        return destinationInterceptor;
    }

    @Override
    public ConnectionContext getAdminConnectionContext() {
        return adminConnectionContext;
    }

    @Override
    public void setAdminConnectionContext(ConnectionContext adminConnectionContext) {
        this.adminConnectionContext = adminConnectionContext;
    }

    public Map<ConnectionId, ConnectionState> getConnectionStates() {
        return connectionStates;
    }

    @Override
    public PListStore getTempDataStore() {
        return brokerService.getTempDataStore();
    }

    @Override
    public URI getVmConnectorURI() {
        return brokerService.getVmConnectorURI();
    }

    @Override
    public void brokerServiceStarted() {
    }

    @Override
    public BrokerService getBrokerService() {
        return brokerService;
    }

    @Override
    public boolean isExpired(MessageReference messageReference) {
        boolean expired = false;
        if (messageReference.isExpired()) {
            try {
                // prevent duplicate expiry processing
                Message message = messageReference.getMessage();
                synchronized (message) {
                    expired = stampAsExpired(message);
                }
            } catch (IOException e) {
                LOG.warn("unexpected exception on message expiry determination for: " + messageReference, e);
            }
        }
        return expired;
    }
   
    private boolean stampAsExpired(Message message) throws IOException {
        boolean stamped=false;
        if (message.getProperty(ORIGINAL_EXPIRATION) == null) {
            long expiration=message.getExpiration();     
            message.setProperty(ORIGINAL_EXPIRATION,new Long(expiration));
            stamped = true;
        }
        return stamped;
    }

    
    @Override
    public void messageExpired(ConnectionContext context, MessageReference node) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Message expired " + node);
        }
        getRoot().sendToDeadLetterQueue(context, node);
    }
    
    @Override
    public void sendToDeadLetterQueue(ConnectionContext context,
	        MessageReference node){
		try{
			if(node!=null){
				Message message=node.getMessage();
				if(message!=null && node.getRegionDestination()!=null){
					DeadLetterStrategy deadLetterStrategy=node
					        .getRegionDestination().getDeadLetterStrategy();
					if(deadLetterStrategy!=null){
						if(deadLetterStrategy.isSendToDeadLetterQueue(message)){
						    // message may be inflight to other subscriptions so do not modify
						    message = message.copy();
						    stampAsExpired(message);
						    message.setExpiration(0);
						    if(!message.isPersistent()){
							    message.setPersistent(true);
							    message.setProperty("originalDeliveryMode",
								        "NON_PERSISTENT");
							}
							// The original destination and transaction id do
							// not get filled when the message is first sent,
							// it is only populated if the message is routed to
							// another destination like the DLQ
							ActiveMQDestination deadLetterDestination=deadLetterStrategy
							        .getDeadLetterQueueFor(message
							                .getDestination());
							if (context.getBroker()==null) {
								context.setBroker(getRoot());
							}
							BrokerSupport.resendNoCopy(context,message,
							        deadLetterDestination);
						}
					} else {
					    if (LOG.isDebugEnabled()) {
					        LOG.debug("Expired message with no DLQ strategy in place");
					    }
					}
				}
			}
		}catch(Exception e){
			LOG.warn("Caught an exception sending to DLQ: "+node,e);
		}
	}

    @Override
    public Broker getRoot() {
        try {
            return getBrokerService().getBroker();
        } catch (Exception e) {
            LOG.fatal("Trying to get Root Broker " + e);
            throw new RuntimeException("The broker from the BrokerService should not throw an exception");
        }
    }
    
    /**
     * @return the broker sequence id
     */
    @Override
    public long getBrokerSequenceId() {
        synchronized(sequenceGenerator) {
            return sequenceGenerator.getNextSequenceId();
        }
    }
}
