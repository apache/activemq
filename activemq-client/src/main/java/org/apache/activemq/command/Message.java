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

import java.beans.Transient;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.zip.DeflaterOutputStream;

import javax.jms.JMSException;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.advisory.AdvisorySupport;
import org.apache.activemq.broker.region.MessageReference;
import org.apache.activemq.usage.MemoryUsage;
import org.apache.activemq.util.ByteArrayInputStream;
import org.apache.activemq.util.ByteArrayOutputStream;
import org.apache.activemq.util.ByteSequence;
import org.apache.activemq.util.MarshallingSupport;
import org.apache.activemq.wireformat.WireFormat;
import org.fusesource.hawtbuf.UTF8Buffer;

/**
 * Represents an ActiveMQ message
 *
 * @openwire:marshaller
 *
 */
public abstract class Message extends BaseCommand implements MarshallAware, MessageReference {
    public static final String ORIGINAL_EXPIRATION = "originalExpiration";

    /**
     * The default minimum amount of memory a message is assumed to use
     */
    public static final int DEFAULT_MINIMUM_MESSAGE_SIZE = 1024;

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
    protected volatile ByteSequence marshalledProperties;
    protected DataStructure dataStructure;
    protected int redeliveryCounter;

    protected int size;
    protected Map<String, Object> properties;
    protected boolean readOnlyProperties;
    protected boolean readOnlyBody;
    protected transient boolean recievedByDFBridge;
    protected boolean droppable;
    protected boolean jmsXGroupFirstForConsumer;

    private transient short referenceCount;
    private transient ActiveMQConnection connection;
    transient MessageDestination regionDestination;
    transient MemoryUsage memoryUsage;
    transient AtomicBoolean processAsExpired = new AtomicBoolean(false);

    private BrokerId[] brokerPath;
    private BrokerId[] cluster;

    public static interface MessageDestination {
        int getMinimumMessageSize();
        MemoryUsage getMemoryUsage();
    }

    public abstract Message copy();
    public abstract void clearBody() throws JMSException;
    public abstract void storeContent();
    public abstract void storeContentAndClear();

    /**
     * @deprecated - This method name is misnamed
     * @throws JMSException
     */
    public void clearMarshalledState() throws JMSException {
        clearUnMarshalledState();
    }

    // useful to reduce the memory footprint of a persisted message
    public void clearUnMarshalledState() throws JMSException {
        properties = null;
    }

    public boolean isMarshalled() {
        return isContentMarshalled() && isPropertiesMarshalled();
    }

    protected boolean isPropertiesMarshalled() {
        return marshalledProperties != null || properties == null;
    }

    protected boolean isContentMarshalled() {
        return content != null;
    }

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

            // The new message hasn't expired, so remove this feild.
            copy.properties.remove(ORIGINAL_EXPIRATION);
        } else {
            copy.properties = properties;
        }

        copy.content = copyByteSequence(content);
        copy.marshalledProperties = copyByteSequence(marshalledProperties);
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
        copy.brokerPath = brokerPath;
        copy.jmsXGroupFirstForConsumer = jmsXGroupFirstForConsumer;

        // lets not copy the following fields
        // copy.targetConsumerId = targetConsumerId;
        // copy.referenceCount = referenceCount;
    }

    private ByteSequence copyByteSequence(ByteSequence content) {
        if (content != null) {
            return new ByteSequence(content.getData(), content.getOffset(), content.getLength());
        }
        return null;
    }

    public Object getProperty(String name) throws IOException {
        if (properties == null) {
            if (marshalledProperties == null) {
                return null;
            }
            properties = unmarsallProperties(marshalledProperties);
        }
        Object result = properties.get(name);
        if (result instanceof UTF8Buffer) {
            result = result.toString();
        }

        return result;
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

    public void removeProperty(String name) throws IOException {
        lazyCreateProperties();
        properties.remove(name);
    }

    protected void lazyCreateProperties() throws IOException {
        if (properties == null) {
            if (marshalledProperties == null) {
                properties = new HashMap<String, Object>();
            } else {
                properties = unmarsallProperties(marshalledProperties);
                marshalledProperties = null;
            }
        } else {
            marshalledProperties = null;
        }
    }

    private Map<String, Object> unmarsallProperties(ByteSequence marshalledProperties) throws IOException {
        return MarshallingSupport.unmarshalPrimitiveMap(new DataInputStream(new ByteArrayInputStream(marshalledProperties)));
    }

    @Override
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

    @Override
	public void afterMarshall(WireFormat wireFormat) throws IOException {
    }

    @Override
	public void beforeUnmarshall(WireFormat wireFormat) throws IOException {
    }

    @Override
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
    @Override
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
    @Override
	public String getGroupID() {
        return groupID;
    }

    public void setGroupID(String groupID) {
        this.groupID = groupID;
    }

    /**
     * @openwire:property version=1
     */
    @Override
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
    @Override
	public boolean isPersistent() {
        return persistent;
    }

    public void setPersistent(boolean deliveryMode) {
        this.persistent = deliveryMode;
    }

    /**
     * @openwire:property version=1
     */
    @Override
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
        if (priority < 0) {
            this.priority = 0;
        } else if (priority > 9) {
            this.priority = 9;
        } else {
            this.priority = priority;
        }
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
    @Override
	public ConsumerId getTargetConsumerId() {
        return targetConsumerId;
    }

    public void setTargetConsumerId(ConsumerId targetConsumerId) {
        this.targetConsumerId = targetConsumerId;
    }

    @Override
	public boolean isExpired() {
        long expireTime = getExpiration();
        return expireTime > 0 && System.currentTimeMillis() > expireTime;
    }

    @Override
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

    @Override
	public void incrementRedeliveryCounter() {
        redeliveryCounter++;
    }

    /**
     * @openwire:property version=1
     */
    @Override
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

    @Override
	public int getReferenceCount() {
        return referenceCount;
    }

    @Override
	public Message getMessageHardRef() {
        return this;
    }

    @Override
	public Message getMessage() {
        return this;
    }

    public void setRegionDestination(MessageDestination destination) {
        this.regionDestination = destination;
        if(this.memoryUsage==null) {
            this.memoryUsage=destination.getMemoryUsage();
        }
    }

    @Override
    @Transient
	public MessageDestination getRegionDestination() {
        return regionDestination;
    }

    public MemoryUsage getMemoryUsage() {
        return this.memoryUsage;
    }

    public void setMemoryUsage(MemoryUsage usage) {
        this.memoryUsage=usage;
    }

    @Override
    public boolean isMarshallAware() {
        return true;
    }

    @Override
	public int incrementReferenceCount() {
        int rc;
        int size;
        synchronized (this) {
            rc = ++referenceCount;
            size = getSize();
        }

        if (rc == 1 && getMemoryUsage() != null) {
            getMemoryUsage().increaseUsage(size);
            //System.err.println("INCREASE USAGE " + System.identityHashCode(getMemoryUsage()) + " PERCENT = " + getMemoryUsage().getPercentUsage());

        }

        //System.out.println(" + "+getMemoryUsage().getName()+" :::: "+getMessageId()+"rc="+rc);
        return rc;
    }

    @Override
	public int decrementReferenceCount() {
        int rc;
        int size;
        synchronized (this) {
            rc = --referenceCount;
            size = getSize();
        }

        if (rc == 0 && getMemoryUsage() != null) {
            getMemoryUsage().decreaseUsage(size);
            //Thread.dumpStack();
            //System.err.println("DECREADED USAGE " + System.identityHashCode(getMemoryUsage()) + " PERCENT = " + getMemoryUsage().getPercentUsage());
        }

        //System.out.println(" - "+getMemoryUsage().getName()+" :::: "+getMessageId()+"rc="+rc);

        return rc;
    }

    @Override
	public int getSize() {
        int minimumMessageSize = getMinimumMessageSize();
        if (size < minimumMessageSize || size == 0) {
            size = minimumMessageSize;
            if (marshalledProperties != null) {
                size += marshalledProperties.getLength();
            }
            if (content != null) {
                size += content.getLength();
            }
        }
        return size;
    }

    protected int getMinimumMessageSize() {
        int result = DEFAULT_MINIMUM_MESSAGE_SIZE;
        //let destination override
        MessageDestination dest = regionDestination;
        if (dest != null) {
            result=dest.getMinimumMessageSize();
        }
        return result;
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

    @Override
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

    @Override
	public boolean isDropped() {
        return false;
    }

    /**
     * @openwire:property version=10
     */
    public boolean isJMSXGroupFirstForConsumer() {
        return jmsXGroupFirstForConsumer;
    }

    public void setJMSXGroupFirstForConsumer(boolean val) {
        jmsXGroupFirstForConsumer = val;
    }

    public void compress() throws IOException {
        if (!isCompressed()) {
            storeContent();
            if (!isCompressed() && getContent() != null) {
                doCompress();
            }
        }
    }

    protected void doCompress() throws IOException {
        compressed = true;
        ByteSequence bytes = getContent();
        ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
        OutputStream os = new DeflaterOutputStream(bytesOut);
        os.write(bytes.data, bytes.offset, bytes.length);
        os.close();
        setContent(bytesOut.toByteSequence());
    }

    @Override
    public String toString() {
        return toString(null);
    }

    @Override
    public String toString(Map<String, Object>overrideFields) {
        try {
            getProperties();
        } catch (IOException e) {
        }
        return super.toString(overrideFields);
    }

    @Override
    public boolean canProcessAsExpired() {
        return processAsExpired.compareAndSet(false, true);
    }
}
