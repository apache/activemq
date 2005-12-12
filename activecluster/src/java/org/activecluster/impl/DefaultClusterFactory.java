/**
 * 
 * Copyright 2005 LogicBlaze, Inc. (http://www.logicblaze.com)
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
package org.activecluster.impl;

import java.util.Timer;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.DeliveryMode;
import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.Topic;
import org.activecluster.Cluster;
import org.activecluster.ClusterException;
import org.activecluster.ClusterFactory;
import org.activemq.util.IdGenerator;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * A Factory of DefaultCluster instances
 *
 * @version $Revision: 1.4 $
 */
public class DefaultClusterFactory implements ClusterFactory {

    private final static Log log = LogFactory.getLog(DefaultClusterFactory.class);

    private ConnectionFactory connectionFactory;
    private boolean transacted;
    private int acknowledgeMode;
    private String dataTopicPrefix;
    private long inactiveTime;
    private boolean useQueueForInbox = false;
    private int deliveryMode = DeliveryMode.NON_PERSISTENT;
    private IdGenerator idGenerator = new IdGenerator();

    public DefaultClusterFactory(ConnectionFactory connectionFactory, boolean transacted, int acknowledgeMode, String dataTopicPrefix, long inactiveTime) {
        this.connectionFactory = connectionFactory;
        this.transacted = transacted;
        this.acknowledgeMode = acknowledgeMode;
        this.dataTopicPrefix = dataTopicPrefix;
        this.inactiveTime = inactiveTime;
    }

    public DefaultClusterFactory(ConnectionFactory connectionFactory) {
        this(connectionFactory, false, Session.AUTO_ACKNOWLEDGE, "ACTIVECLUSTER.DATA.", 6000L);
    }

    public Cluster createCluster(String groupDestination) throws JMSException {
        return createCluster(idGenerator.generateId(), groupDestination);
    }

    public Cluster createCluster(String name,String groupDestination) throws  JMSException {
        Connection connection = getConnectionFactory().createConnection();
        Session session = createSession(connection);
        return createCluster(connection, session, name,groupDestination);
    }

    // Properties
    //-------------------------------------------------------------------------
    public String getDataTopicPrefix() {
        return dataTopicPrefix;
    }

    public void setDataTopicPrefix(String dataTopicPrefix) {
        this.dataTopicPrefix = dataTopicPrefix;
    }

    public int getAcknowledgeMode() {
        return acknowledgeMode;
    }

    public void setAcknowledgeMode(int acknowledgeMode) {
        this.acknowledgeMode = acknowledgeMode;
    }

    public long getInactiveTime() {
        return inactiveTime;
    }

    public void setInactiveTime(long inactiveTime) {
        this.inactiveTime = inactiveTime;
    }

    public boolean isTransacted() {
        return transacted;
    }

    public void setTransacted(boolean transacted) {
        this.transacted = transacted;
    }

    public boolean isUseQueueForInbox() {
        return useQueueForInbox;
    }

    public void setUseQueueForInbox(boolean useQueueForInbox) {
        this.useQueueForInbox = useQueueForInbox;
    }

    public ConnectionFactory getConnectionFactory() {
        return connectionFactory;
    }

    public void setConnectionFactory(ConnectionFactory connectionFactory) {
        this.connectionFactory = connectionFactory;
    }

    public int getDeliveryMode() {
        return deliveryMode;
    }

    /**
     * Sets the delivery mode of the group based producer
     */
    public void setDeliveryMode(int deliveryMode) {
        this.deliveryMode = deliveryMode;
    }

    // Implementation methods
    //-------------------------------------------------------------------------
    protected Cluster createCluster(Connection connection, Session session, String name,String groupDestination) throws JMSException {
        String dataDestination = dataTopicPrefix + groupDestination;

        log.info("Creating cluster group producer on topic: " + groupDestination);

        MessageProducer producer = createProducer(session, null);
        producer.setDeliveryMode(deliveryMode);

        log.info("Creating cluster data producer on data destination: " + dataDestination);

        Topic dataTopic = session.createTopic(dataDestination);
        MessageProducer keepAliveProducer = session.createProducer(dataTopic);
        keepAliveProducer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
        StateService serviceStub = new StateServiceStub(session, keepAliveProducer);

        String localInboxDestination = dataDestination + "." + name;
        
        ReplicatedLocalNode localNode = new ReplicatedLocalNode(name,localInboxDestination, serviceStub);
        Timer timer = new Timer();
        DefaultCluster answer = new DefaultCluster(localNode, dataDestination, groupDestination, connection, session, producer, timer, inactiveTime);
        return answer;
    }

    /*
     protected Cluster createInternalCluster(Session session, Topic dataDestination) {
         MessageProducer producer = createProducer(session);
         return new DefaultCluster(new NonReplicatedLocalNode(), dataDestination, connection, session, producer);
     }
     */
    
    protected MessageProducer createProducer(Session session, Topic groupDestination) throws JMSException {
        return session.createProducer(groupDestination);
    }

    protected Session createSession(Connection connection) throws JMSException {
        return connection.createSession(transacted, acknowledgeMode);
    }
}