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
package org.apache.activemq.command;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.advisory.AdvisorySupport;
import org.apache.activemq.broker.region.MessageReference;
import org.apache.activemq.usage.MemoryUsage;
import org.apache.activemq.util.ByteArrayInputStream;
import org.apache.activemq.util.ByteArrayOutputStream;
import org.apache.activemq.util.ByteSequence;
import org.apache.activemq.util.MarshallingSupport;
import org.apache.activemq.wireformat.WireFormat;

/**
 * Represents an ActiveMQ message
 * 
 * @openwire:marshaller
 * @version $Revision$
 */
public abstract class Message extends BaseCommand implements MarshallAware, MessageReference {

    public static final int AVERAGE_MESSAGE_SIZE_OVERHEAD = 8 * 1024;

    protected MessageId messageId;
    protected ActiveMQDestination originalDestination;
    protected TransactionId originalTransactionId;

    protected ProducerId producerId;
    protected ActiveMQDestination destination;
    protected TransactionId transactionId;

    protected long expiration;
    protected long timestamp;
    protected long arrival;
    protected long brokerInTime;
    protected long brokerOutTime;
    protected String correlationId;
    protected ActiveMQDestination replyTo;
    protected boolean persistent;
    protected String type;
    protected byte priority;
    protected String groupID;
    protected int groupSequence;
    protected ConsumerId targetConsumerId;
    protected boolean compressed;
    protected String userID;

    protected ByteSequence content;
    protected ByteSequence marshalledProperties;
    protected DataStructure dataStructure;
    protected int redeliveryCounter;

    protected int size;
    protected Map<String, Object> properties;
    protected boolean readOnlyProperties;
    protected boolean readOnlyBody;
    protected transient boolean recievedByDFBridge;
    protected boolean droppable;

    private transient short referenceCount;
    private transient ActiveMQConnection connection;
    private transient org.apache.activemq.broker.region.Destination regionDestination;
    private transient MemoryUsage memoryUsage;

    private BrokerId[] brokerPath;
    private BrokerId[] cluster;

    public abstract Message copy();

    protected void copy(Message copy) {
        super.copy(copy);
        copy.producerId = producerId;
        copy.transactionId = transactionId;
        copy.destination = destination;
        copy.messageId = messageId != null ? messageId.copy() : null;
        copy.originalDestination = originalDestination;
        copy.originalTransactionId = originalTransactionId;
        copy.expiration = expiration;
        copy.timestamp = timestamp;
        copy.correlationId = correlationId;
        copy.replyTo = replyTo;
        copy.persistent = persistent;
        copy.redeliveryCounter = redeliveryCounter;
        copy.type = type;
        copy.priority = priority;
        copy.size = size;
        copy.groupID = groupID;
        copy.userID = userID;
        copy.groupSequence = groupSequence;

        if (properties != null) {
            copy.properties = new HashMap<String, Object>(properties);
        } else {
            copy.properties = properties;
        }

        copy.content = content;
        copy.marshalledProperties = marshalledProperties;
        copy.dataStructure = dataStructure;
        copy.readOnlyProperties = readOnlyProperties;
        copy.readOnlyBody = readOnlyBody;
        copy.compressed = compressed;
        copy.recievedByDFBridge = recievedByDFBridge;

        copy.arrival = arrival;
        copy.connection = connection;
        copy.regionDestination = regionDestination;
        copy.brokerInTime = brokerInTime;
        copy.brokerOutTime = brokerOutTime;
        copy.memoryUsage=this.memoryUsage;
        // copying the broker path breaks networks - if a consumer re-uses a
        // consumed
        // message and forwards it on
        // copy.brokerPath = brokerPath;

        // lets not copy the following fields
        // copy.targetConsumerId = targetConsumerId;
        // copy.referenceCount = referenceCount;
    }

    public Object getProperty(String name) throws IOException {
        if (properties == null) {
            if (marshalledProperties == null) {
                return null;
            }
            properties = unmarsallProperties(marshalledProperties);
        }
        return properties.get(name);
    }

    @SuppressWarnings("unchecked")
    public Map<String, Object> getProperties() throws IOException {
        if (properties == null) {
            if (marshalledProperties == null) {
                return Collections.EMPTY_MAP;
            }
            properties = unmarsallProperties(marshalledProperties);
        }
        return Collections.unmodifiableMap(properties);
    }

    public void clearProperties() {
        marshalledProperties = null;
        properties = null;
    }

    public void setProperty(String name, Object value) throws IOException {
        lazyCreateProperties();
        properties.put(name, value);
    }

    protected void lazyCreateProperties() throws IOException {
        if (properties == null) {
            if (marshalledProperties == null) {
                properties = new HashMap<String, Object>();
            } else {
                properties = unmarsallProperties(marshalledProperties);
                marshalledProperties = null;
            }
        }
    }

    private Map<String, Object> unmarsallProperties(ByteSequence marshalledProperties) throws IOException {
        return MarshallingSupport.unmarshalPrimitiveMap(new DataInputStream(new ByteArrayInputStream(marshalledProperties)));
    }

    public void beforeMarshall(WireFormat wireFormat) throws IOException {
        // Need to marshal the properties.
        if (marshalledProperties == null && properties != null) {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            DataOutputStream os = new DataOutputStream(baos);
            MarshallingSupport.marshalPrimitiveMap(properties, os);
            os.close();
            marshalledProperties = baos.toByteSequence();
        }
    }

    public void afterMarshall(WireFormat wireFormat) throws IOException {
    }

    public void beforeUnmarshall(WireFormat wireFormat) throws IOException {
    }

    public void afterUnmarshall(WireFormat wireFormat) throws IOException {
    }

    // /////////////////////////////////////////////////////////////////
    //
    // Simple Field accessors
    //
    // /////////////////////////////////////////////////////////////////

    /**
     * @openwire:property version=1 cache=true
     */
    public ProducerId getProducerId() {
        return producerId;
    }

    public void setProducerId(ProducerId producerId) {
        this.producerId = producerId;
    }

    /**
     * @openwire:property version=1 cache=true
     */
    public ActiveMQDestination getDestination() {
        return destination;
    }

    public void setDestination(ActiveMQDestination destination) {
        this.destination = destination;
    }

    /**
     * @openwire:property version=1 cache=true
     */
    public TransactionId getTransactionId() {
        return transactionId;
    }

    public void setTransactionId(TransactionId transactionId) {
        this.transactionId = transactionId;
    }

    public boolean isInTransaction() {
        return transactionId != null;
    }

    /**
     * @openwire:property version=1 cache=true
     */
    public ActiveMQDestination getOriginalDestination() {
        return originalDestination;
    }

    public void setOriginalDestination(ActiveMQDestination destination) {
        this.originalDestination = destination;
    }

    /**
     * @openwire:property version=1
     */
    public MessageId getMessageId() {
        return messageId;
    }

    public void setMessageId(MessageId messageId) {
        this.messageId = messageId;
    }

    /**
     * @openwire:property version=1 cache=true
     */
    public TransactionId getOriginalTransactionId() {
        return originalTransactionId;
    }

    public void setOriginalTransactionId(TransactionId transactionId) {
        this.originalTransactionId = transactionId;
    }

    /**
     * @openwire:property version=1
     */
    public String getGroupID() {
        return groupID;
    }

    public void setGroupID(String groupID) {
        this.groupID = groupID;
    }

    /**
     * @openwire:property version=1
     */
    public int getGroupSequence() {
        return groupSequence;
    }

    public void setGroupSequence(int groupSequence) {
        this.groupSequence = groupSequence;
    }

    /**
     * @openwire:property version=1
     */
    public String getCorrelationId() {
        return correlationId;
    }

    public void setCorrelationId(String correlationId) {
        this.correlationId = correlationId;
    }

    /**
     * @openwire:property version=1
     */
    public boolean isPersistent() {
        return persistent;
    }

    public void setPersistent(boolean deliveryMode) {
        this.persistent = deliveryMode;
    }

    /**
     * @openwire:property version=1
     */
    public long getExpiration() {
        return expiration;
    }

    public void setExpiration(long expiration) {
        this.expiration = expiration;
    }

    /**
     * @openwire:property version=1
     */
    public byte getPriority() {
        return priority;
    }

    public void setPriority(byte priority) {
        this.priority = priority;
    }

    /**
     * @openwire:property version=1
     */
    public ActiveMQDestination getReplyTo() {
        return replyTo;
    }

    public void setReplyTo(ActiveMQDestination replyTo) {
        this.replyTo = replyTo;
    }

    /**
     * @openwire:property version=1
     */
    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    /**
     * @openwire:property version=1
     */
    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    /**
     * @openwire:property version=1
     */
    public ByteSequence getContent() {
        return content;
    }

    public void setContent(ByteSequence content) {
        this.content = content;
    }

    /**
     * @openwire:property version=1
     */
    public ByteSequence getMarshalledProperties() {
        return marshalledProperties;
    }

    public void setMarshalledProperties(ByteSequence marshalledProperties) {
        this.marshalledProperties = marshalledProperties;
    }

    /**
     * @openwire:property version=1
     */
    public DataStructure getDataStructure() {
        return dataStructure;
    }

    public void setDataStructure(DataStructure data) {
        this.dataStructure = data;
    }

    /**
     * Can be used to route the message to a specific consumer. Should be null
     * to allow the broker use normal JMS routing semantics. If the target
     * consumer id is an active consumer on the broker, the message is dropped.
     * Used by the AdvisoryBroker to replay advisory messages to a specific
     * consumer.
     * 
     * @openwire:property version=1 cache=true
     */
    public ConsumerId getTargetConsumerId() {
        return targetConsumerId;
    }

    public void setTargetConsumerId(ConsumerId targetConsumerId) {
        this.targetConsumerId = targetConsumerId;
    }

    public boolean isExpired() {
        long expireTime = getExpiration();
        if (expireTime > 0 && System.currentTimeMillis() > expireTime) {
            return true;
        }
        return false;
    }

    public boolean isAdvisory() {
        return type != null && type.equals(AdvisorySupport.ADIVSORY_MESSAGE_TYPE);
    }

    /**
     * @openwire:property version=1
     */
    public boolean isCompressed() {
        return compressed;
    }

    public void setCompressed(boolean compressed) {
        this.compressed = compressed;
    }

    public boolean isRedelivered() {
        return redeliveryCounter > 0;
    }

    public void setRedelivered(boolean redelivered) {
        if (redelivered) {
            if (!isRedelivered()) {
                setRedeliveryCounter(1);
            }
        } else {
            if (isRedelivered()) {
                setRedeliveryCounter(0);
            }
        }
    }

    public void incrementRedeliveryCounter() {
        redeliveryCounter++;
    }

    /**
     * @openwire:property version=1
     */
    public int getRedeliveryCounter() {
        return redeliveryCounter;
    }

    public void setRedeliveryCounter(int deliveryCounter) {
        this.redeliveryCounter = deliveryCounter;
    }

    /**
     * The route of brokers the command has moved through.
     * 
     * @openwire:property version=1 cache=true
     */
    public BrokerId[] getBrokerPath() {
        return brokerPath;
    }

    public void setBrokerPath(BrokerId[] brokerPath) {
        this.brokerPath = brokerPath;
    }

    public boolean isReadOnlyProperties() {
        return readOnlyProperties;
    }

    public void setReadOnlyProperties(boolean readOnlyProperties) {
        this.readOnlyProperties = readOnlyProperties;
    }

    public boolean isReadOnlyBody() {
        return readOnlyBody;
    }

    public void setReadOnlyBody(boolean readOnlyBody) {
        this.readOnlyBody = readOnlyBody;
    }

    public ActiveMQConnection getConnection() {
        return this.connection;
    }

    public void setConnection(ActiveMQConnection connection) {
        this.connection = connection;
    }

    /**
     * Used to schedule the arrival time of a message to a broker. The broker
     * will not dispatch a message to a consumer until it's arrival time has
     * elapsed.
     * 
     * @openwire:property version=1
     */
    public long getArrival() {
        return arrival;
    }

    public void setArrival(long arrival) {
        this.arrival = arrival;
    }

    /**
     * Only set by the broker and defines the userID of the producer connection
     * who sent this message. This is an optional field, it needs to be enabled
     * on the broker to have this field populated.
     * 
     * @openwire:property version=1
     */
    public String getUserID() {
        return userID;
    }

    public void setUserID(String jmsxUserID) {
        this.userID = jmsxUserID;
    }

    public int getReferenceCount() {
        return referenceCount;
    }

    public Message getMessageHardRef() {
        return this;
    }

    public Message getMessage() throws IOException {
        return this;
    }

    public org.apache.activemq.broker.region.Destination getRegionDestination() {
        return regionDestination;
    }

    public void setRegionDestination(org.apache.activemq.broker.region.Destination destination) {
        this.regionDestination = destination;
        if(this.memoryUsage==null) {
            this.memoryUsage=regionDestination.getMemoryUsage();
        }
    }
    
    public MemoryUsage getMemoryUsage() {
        return this.memoryUsage;
    }
    
    public void setMemoryUsage(MemoryUsage usage) {
        this.memoryUsage=usage;
    }

    public boolean isMarshallAware() {
        return true;
    }

    public int incrementReferenceCount() {
        int rc;
        int size;
        synchronized (this) {
            rc = ++referenceCount;
            size = getSize();
        }

        if (rc == 1 && getMemoryUsage() != null) {
            getMemoryUsage().increaseUsage(size);
        }

        //System.out.println(" + "+getMemoryUsage().getName()+" :::: "+getMessageId()+"rc="+rc);
        return rc;
    }

    public int decrementReferenceCount() {
        int rc;
        int size;
        synchronized (this) {
            rc = --referenceCount;
            size = getSize();
        }

        if (rc == 0 && getMemoryUsage() != null) {
            getMemoryUsage().decreaseUsage(size);
        }
        //System.out.println(" - "+getMemoryUsage().getName()+" :::: "+getMessageId()+"rc="+rc);

        return rc;
    }

    public int getSize() {
        if (size <= AVERAGE_MESSAGE_SIZE_OVERHEAD) {
            size = AVERAGE_MESSAGE_SIZE_OVERHEAD;
            if (marshalledProperties != null) {
                size += marshalledProperties.getLength();
            }
            if (content != null) {
                size += content.getLength();
            }
        }
        return size;
    }

    /**
     * @openwire:property version=1
     * @return Returns the recievedByDFBridge.
     */
    public boolean isRecievedByDFBridge() {
        return recievedByDFBridge;
    }

    /**
     * @param recievedByDFBridge The recievedByDFBridge to set.
     */
    public void setRecievedByDFBridge(boolean recievedByDFBridge) {
        this.recievedByDFBridge = recievedByDFBridge;
    }

    public void onMessageRolledBack() {
        incrementRedeliveryCounter();
    }

    /**
     * @openwire:property version=2 cache=true
     */
    public boolean isDroppable() {
        return droppable;
    }

    public void setDroppable(boolean droppable) {
        this.droppable = droppable;
    }

    /**
     * If a message is stored in multiple nodes on a cluster, all the cluster
     * members will be listed here. Otherwise, it will be null.
     * 
     * @openwire:property version=3 cache=true
     */
    public BrokerId[] getCluster() {
        return cluster;
    }

    public void setCluster(BrokerId[] cluster) {
        this.cluster = cluster;
    }

    public boolean isMessage() {
        return true;
    }

    /**
     * @openwire:property version=3
     */
    public long getBrokerInTime() {
        return this.brokerInTime;
    }

    public void setBrokerInTime(long brokerInTime) {
        this.brokerInTime = brokerInTime;
    }

    /**
     * @openwire:property version=3
     */
    public long getBrokerOutTime() {
        return this.brokerOutTime;
    }

    public void setBrokerOutTime(long brokerOutTime) {
        this.brokerOutTime = brokerOutTime;
    }
    
    public boolean isDropped() {
        return false;
    }
}
