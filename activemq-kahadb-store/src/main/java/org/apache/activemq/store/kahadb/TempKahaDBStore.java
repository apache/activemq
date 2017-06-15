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
package org.apache.activemq.store.kahadb;

import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.BrokerServiceAware;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.broker.scheduler.JobSchedulerStore;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTempQueue;
import org.apache.activemq.command.ActiveMQTempTopic;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.MessageAck;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.command.ProducerId;
import org.apache.activemq.command.SubscriptionInfo;
import org.apache.activemq.command.TransactionId;
import org.apache.activemq.command.XATransactionId;
import org.apache.activemq.openwire.OpenWireFormat;
import org.apache.activemq.protobuf.Buffer;
import org.apache.activemq.store.AbstractMessageStore;
import org.apache.activemq.store.MessageRecoveryListener;
import org.apache.activemq.store.MessageStore;
import org.apache.activemq.store.MessageStoreSubscriptionStatistics;
import org.apache.activemq.store.PersistenceAdapter;
import org.apache.activemq.store.TopicMessageStore;
import org.apache.activemq.store.TransactionRecoveryListener;
import org.apache.activemq.store.TransactionStore;
import org.apache.activemq.store.kahadb.data.KahaAddMessageCommand;
import org.apache.activemq.store.kahadb.data.KahaDestination;
import org.apache.activemq.store.kahadb.data.KahaDestination.DestinationType;
import org.apache.activemq.store.kahadb.data.KahaLocation;
import org.apache.activemq.store.kahadb.data.KahaRemoveDestinationCommand;
import org.apache.activemq.store.kahadb.data.KahaRemoveMessageCommand;
import org.apache.activemq.store.kahadb.data.KahaSubscriptionCommand;
import org.apache.activemq.store.kahadb.disk.journal.Location;
import org.apache.activemq.store.kahadb.disk.page.Transaction;
import org.apache.activemq.usage.MemoryUsage;
import org.apache.activemq.usage.SystemUsage;
import org.apache.activemq.util.ByteSequence;
import org.apache.activemq.wireformat.WireFormat;

public class TempKahaDBStore extends TempMessageDatabase implements PersistenceAdapter, BrokerServiceAware {

    private final WireFormat wireFormat = new OpenWireFormat();
    private BrokerService brokerService;

    @Override
    public void setBrokerName(String brokerName) {
    }
    @Override
    public void setUsageManager(SystemUsage usageManager) {
    }

    @Override
    public TransactionStore createTransactionStore() throws IOException {
        return new TransactionStore(){

            @Override
            public void commit(TransactionId txid, boolean wasPrepared, Runnable preCommit,Runnable postCommit) throws IOException {
                if (preCommit != null) {
                    preCommit.run();
                }
                processCommit(txid);
                if (postCommit != null) {
                    postCommit.run();
                }
            }
            @Override
            public void prepare(TransactionId txid) throws IOException {
                processPrepare(txid);
            }
            @Override
            public void rollback(TransactionId txid) throws IOException {
                processRollback(txid);
            }
            @Override
            public void recover(TransactionRecoveryListener listener) throws IOException {
                for (Map.Entry<TransactionId, ArrayList<Operation>> entry : preparedTransactions.entrySet()) {
                    XATransactionId xid = (XATransactionId)entry.getKey();
                    ArrayList<Message> messageList = new ArrayList<Message>();
                    ArrayList<MessageAck> ackList = new ArrayList<MessageAck>();

                    for (Operation op : entry.getValue()) {
                        if( op.getClass() == AddOpperation.class ) {
                            AddOpperation addOp = (AddOpperation)op;
                            Message msg = (Message)wireFormat.unmarshal( new DataInputStream(addOp.getCommand().getMessage().newInput()) );
                            messageList.add(msg);
                        } else {
                            RemoveOpperation rmOp = (RemoveOpperation)op;
                            MessageAck ack = (MessageAck)wireFormat.unmarshal( new DataInputStream(rmOp.getCommand().getAck().newInput()) );
                            ackList.add(ack);
                        }
                    }

                    Message[] addedMessages = new Message[messageList.size()];
                    MessageAck[] acks = new MessageAck[ackList.size()];
                    messageList.toArray(addedMessages);
                    ackList.toArray(acks);
                    listener.recover(xid, addedMessages, acks);
                }
            }
            @Override
            public void start() throws Exception {
            }
            @Override
            public void stop() throws Exception {
            }
        };
    }

    public class KahaDBMessageStore extends AbstractMessageStore {
        protected KahaDestination dest;

        public KahaDBMessageStore(ActiveMQDestination destination) {
            super(destination);
            this.dest = convert( destination );
        }

        @Override
        public ActiveMQDestination getDestination() {
            return destination;
        }

        @Override
        public void addMessage(ConnectionContext context, Message message) throws IOException {
            KahaAddMessageCommand command = new KahaAddMessageCommand();
            command.setDestination(dest);
            command.setMessageId(message.getMessageId().toProducerKey());
            processAdd(command, message.getTransactionId(), wireFormat.marshal(message));
        }

        @Override
        public void removeMessage(ConnectionContext context, MessageAck ack) throws IOException {
            KahaRemoveMessageCommand command = new KahaRemoveMessageCommand();
            command.setDestination(dest);
            command.setMessageId(ack.getLastMessageId().toProducerKey());
            processRemove(command, ack.getTransactionId());
        }

        @Override
        public void removeAllMessages(ConnectionContext context) throws IOException {
            KahaRemoveDestinationCommand command = new KahaRemoveDestinationCommand();
            command.setDestination(dest);
            process(command);
        }

        @Override
        public Message getMessage(MessageId identity) throws IOException {
            final String key = identity.toProducerKey();

            // Hopefully one day the page file supports concurrent read operations... but for now we must
            // externally synchronize...
            ByteSequence data;
            synchronized(indexMutex) {
                data = pageFile.tx().execute(new Transaction.CallableClosure<ByteSequence, IOException>(){
                    @Override
                    public ByteSequence execute(Transaction tx) throws IOException {
                        StoredDestination sd = getStoredDestination(dest, tx);
                        Long sequence = sd.messageIdIndex.get(tx, key);
                        if( sequence ==null ) {
                            return null;
                        }
                        return sd.orderIndex.get(tx, sequence).data;
                    }
                });
            }
            if( data == null ) {
                return null;
            }

            Message msg = (Message)wireFormat.unmarshal( data );
            return msg;
        }

        @Override
        public void recover(final MessageRecoveryListener listener) throws Exception {
            synchronized(indexMutex) {
                pageFile.tx().execute(new Transaction.Closure<Exception>(){
                    @Override
                    public void execute(Transaction tx) throws Exception {
                        StoredDestination sd = getStoredDestination(dest, tx);
                        for (Iterator<Entry<Long, MessageRecord>> iterator = sd.orderIndex.iterator(tx); iterator.hasNext();) {
                            Entry<Long, MessageRecord> entry = iterator.next();
                            listener.recoverMessage( (Message) wireFormat.unmarshal(entry.getValue().data) );
                        }
                    }
                });
            }
        }

        long cursorPos=0;

        @Override
        public void recoverNextMessages(final int maxReturned, final MessageRecoveryListener listener) throws Exception {
            synchronized(indexMutex) {
                pageFile.tx().execute(new Transaction.Closure<Exception>(){
                    @Override
                    public void execute(Transaction tx) throws Exception {
                        StoredDestination sd = getStoredDestination(dest, tx);
                        Entry<Long, MessageRecord> entry=null;
                        int counter = 0;
                        for (Iterator<Entry<Long, MessageRecord>> iterator = sd.orderIndex.iterator(tx, cursorPos); iterator.hasNext();) {
                            entry = iterator.next();
                            listener.recoverMessage( (Message) wireFormat.unmarshal(entry.getValue().data ) );
                            counter++;
                            if( counter >= maxReturned ) {
                                break;
                            }
                        }
                        if( entry!=null ) {
                            cursorPos = entry.getKey()+1;
                        }
                    }
                });
            }
        }

        @Override
        public void resetBatching() {
            cursorPos=0;
        }


        @Override
        public void setBatch(MessageId identity) throws IOException {
            final String key = identity.toProducerKey();

            // Hopefully one day the page file supports concurrent read operations... but for now we must
            // externally synchronize...
            Long location;
            synchronized(indexMutex) {
                location = pageFile.tx().execute(new Transaction.CallableClosure<Long, IOException>(){
                    @Override
                    public Long execute(Transaction tx) throws IOException {
                        StoredDestination sd = getStoredDestination(dest, tx);
                        return sd.messageIdIndex.get(tx, key);
                    }
                });
            }
            if( location!=null ) {
                cursorPos=location+1;
            }

        }

        @Override
        public void setMemoryUsage(MemoryUsage memoryUsage) {
        }
        @Override
        public void start() throws Exception {
        }
        @Override
        public void stop() throws Exception {
        }

        @Override
        public void recoverMessageStoreStatistics() throws IOException {
            int count = 0;
            synchronized(indexMutex) {
                count = pageFile.tx().execute(new Transaction.CallableClosure<Integer, IOException>(){
                    @Override
                    public Integer execute(Transaction tx) throws IOException {
                        // Iterate through all index entries to get a count of messages in the destination.
                        StoredDestination sd = getStoredDestination(dest, tx);
                        int rc=0;
                        for (Iterator<Entry<String, Long>> iterator = sd.messageIdIndex.iterator(tx); iterator.hasNext();) {
                            iterator.next();
                            rc++;
                        }
                        return rc;
                    }
                });
            }
            getMessageStoreStatistics().getMessageCount().setCount(count);
        }

    }

    class KahaDBTopicMessageStore extends KahaDBMessageStore implements TopicMessageStore {
        public KahaDBTopicMessageStore(ActiveMQTopic destination) {
            super(destination);
        }

        @Override
        public void acknowledge(ConnectionContext context, String clientId, String subscriptionName,
                                MessageId messageId, MessageAck ack) throws IOException {
            KahaRemoveMessageCommand command = new KahaRemoveMessageCommand();
            command.setDestination(dest);
            command.setSubscriptionKey(subscriptionKey(clientId, subscriptionName));
            command.setMessageId(messageId.toProducerKey());
            // We are not passed a transaction info.. so we can't participate in a transaction.
            // Looks like a design issue with the TopicMessageStore interface.  Also we can't recover the original ack
            // to pass back to the XA recover method.
            // command.setTransactionInfo();
            processRemove(command, null);
        }

        @Override
        public void addSubscription(SubscriptionInfo subscriptionInfo, boolean retroactive) throws IOException {
            String subscriptionKey = subscriptionKey(subscriptionInfo.getClientId(), subscriptionInfo.getSubscriptionName());
            KahaSubscriptionCommand command = new KahaSubscriptionCommand();
            command.setDestination(dest);
            command.setSubscriptionKey(subscriptionKey);
            command.setRetroactive(retroactive);
            org.apache.activemq.util.ByteSequence packet = wireFormat.marshal(subscriptionInfo);
            command.setSubscriptionInfo(new Buffer(packet.getData(), packet.getOffset(), packet.getLength()));
            process(command);
        }

        @Override
        public void deleteSubscription(String clientId, String subscriptionName) throws IOException {
            KahaSubscriptionCommand command = new KahaSubscriptionCommand();
            command.setDestination(dest);
            command.setSubscriptionKey(subscriptionKey(clientId, subscriptionName));
            process(command);
        }

        @Override
        public SubscriptionInfo[] getAllSubscriptions() throws IOException {

            final ArrayList<SubscriptionInfo> subscriptions = new ArrayList<SubscriptionInfo>();
            synchronized(indexMutex) {
                pageFile.tx().execute(new Transaction.Closure<IOException>(){
                    @Override
                    public void execute(Transaction tx) throws IOException {
                        StoredDestination sd = getStoredDestination(dest, tx);
                        for (Iterator<Entry<String, KahaSubscriptionCommand>> iterator = sd.subscriptions.iterator(tx); iterator.hasNext();) {
                            Entry<String, KahaSubscriptionCommand> entry = iterator.next();
                            SubscriptionInfo info = (SubscriptionInfo)wireFormat.unmarshal( new DataInputStream(entry.getValue().getSubscriptionInfo().newInput()) );
                            subscriptions.add(info);

                        }
                    }
                });
            }

            SubscriptionInfo[]rc=new SubscriptionInfo[subscriptions.size()];
            subscriptions.toArray(rc);
            return rc;
        }

        @Override
        public SubscriptionInfo lookupSubscription(String clientId, String subscriptionName) throws IOException {
            final String subscriptionKey = subscriptionKey(clientId, subscriptionName);
            synchronized(indexMutex) {
                return pageFile.tx().execute(new Transaction.CallableClosure<SubscriptionInfo, IOException>(){
                    @Override
                    public SubscriptionInfo execute(Transaction tx) throws IOException {
                        StoredDestination sd = getStoredDestination(dest, tx);
                        KahaSubscriptionCommand command = sd.subscriptions.get(tx, subscriptionKey);
                        if( command ==null ) {
                            return null;
                        }
                        return (SubscriptionInfo)wireFormat.unmarshal( new DataInputStream(command.getSubscriptionInfo().newInput()) );
                    }
                });
            }
        }

        @Override
        public int getMessageCount(String clientId, String subscriptionName) throws IOException {
            final String subscriptionKey = subscriptionKey(clientId, subscriptionName);
            synchronized(indexMutex) {
                return pageFile.tx().execute(new Transaction.CallableClosure<Integer, IOException>(){
                    @Override
                    public Integer execute(Transaction tx) throws IOException {
                        StoredDestination sd = getStoredDestination(dest, tx);
                        Long cursorPos = sd.subscriptionAcks.get(tx, subscriptionKey);
                        if ( cursorPos==null ) {
                            // The subscription might not exist.
                            return 0;
                        }
                        cursorPos += 1;

                        int counter = 0;
                        for (Iterator<Entry<Long, MessageRecord>> iterator = sd.orderIndex.iterator(tx, cursorPos); iterator.hasNext();) {
                            iterator.next();
                            counter++;
                        }
                        return counter;
                    }
                });
            }
        }

        @Override
        public long getMessageSize(String clientId, String subscriptionName) throws IOException {
            return 0;
        }

        private final MessageStoreSubscriptionStatistics stats = new MessageStoreSubscriptionStatistics(false);

        @Override
        public MessageStoreSubscriptionStatistics getMessageStoreSubStatistics() {
            return stats;
        }

        @Override
        public void recoverSubscription(String clientId, String subscriptionName, final MessageRecoveryListener listener) throws Exception {
            final String subscriptionKey = subscriptionKey(clientId, subscriptionName);
            synchronized(indexMutex) {
                pageFile.tx().execute(new Transaction.Closure<Exception>(){
                    @Override
                    public void execute(Transaction tx) throws Exception {
                        StoredDestination sd = getStoredDestination(dest, tx);
                        Long cursorPos = sd.subscriptionAcks.get(tx, subscriptionKey);
                        cursorPos += 1;

                        for (Iterator<Entry<Long, MessageRecord>> iterator = sd.orderIndex.iterator(tx, cursorPos); iterator.hasNext();) {
                            Entry<Long, MessageRecord> entry = iterator.next();
                            listener.recoverMessage( (Message) wireFormat.unmarshal(entry.getValue().data ) );
                        }
                    }
                });
            }
        }

        @Override
        public void recoverNextMessages(String clientId, String subscriptionName, final int maxReturned, final MessageRecoveryListener listener) throws Exception {
            final String subscriptionKey = subscriptionKey(clientId, subscriptionName);
            synchronized(indexMutex) {
                pageFile.tx().execute(new Transaction.Closure<Exception>(){
                    @Override
                    public void execute(Transaction tx) throws Exception {
                        StoredDestination sd = getStoredDestination(dest, tx);
                        Long cursorPos = sd.subscriptionCursors.get(subscriptionKey);
                        if( cursorPos == null ) {
                            cursorPos = sd.subscriptionAcks.get(tx, subscriptionKey);
                            cursorPos += 1;
                        }

                        Entry<Long, MessageRecord> entry=null;
                        int counter = 0;
                        for (Iterator<Entry<Long, MessageRecord>> iterator = sd.orderIndex.iterator(tx, cursorPos); iterator.hasNext();) {
                            entry = iterator.next();
                            listener.recoverMessage( (Message) wireFormat.unmarshal(entry.getValue().data ) );
                            counter++;
                            if( counter >= maxReturned ) {
                                break;
                            }
                        }
                        if( entry!=null ) {
                            sd.subscriptionCursors.put(subscriptionKey, entry.getKey() + 1);
                        }
                    }
                });
            }
        }

        @Override
        public void resetBatching(String clientId, String subscriptionName) {
            try {
                final String subscriptionKey = subscriptionKey(clientId, subscriptionName);
                synchronized(indexMutex) {
                    pageFile.tx().execute(new Transaction.Closure<IOException>(){
                        @Override
                        public void execute(Transaction tx) throws IOException {
                            StoredDestination sd = getStoredDestination(dest, tx);
                            sd.subscriptionCursors.remove(subscriptionKey);
                        }
                    });
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    String subscriptionKey(String clientId, String subscriptionName){
        return clientId+":"+subscriptionName;
    }

    @Override
    public MessageStore createQueueMessageStore(ActiveMQQueue destination) throws IOException {
        return new KahaDBMessageStore(destination);
    }

    @Override
    public TopicMessageStore createTopicMessageStore(ActiveMQTopic destination) throws IOException {
        return new KahaDBTopicMessageStore(destination);
    }

    /**
     * Cleanup method to remove any state associated with the given destination.
     * This method does not stop the message store (it might not be cached).
     *
     * @param destination Destination to forget
     */
    @Override
    public void removeQueueMessageStore(ActiveMQQueue destination) {
    }

    /**
     * Cleanup method to remove any state associated with the given destination
     * This method does not stop the message store (it might not be cached).
     *
     * @param destination Destination to forget
     */
    @Override
    public void removeTopicMessageStore(ActiveMQTopic destination) {
    }

    @Override
    public void deleteAllMessages() throws IOException {
    }


    @Override
    public Set<ActiveMQDestination> getDestinations() {
        try {
            final HashSet<ActiveMQDestination> rc = new HashSet<ActiveMQDestination>();
            synchronized(indexMutex) {
                pageFile.tx().execute(new Transaction.Closure<IOException>(){
                    @Override
                    public void execute(Transaction tx) throws IOException {
                        for (Iterator<Entry<String, StoredDestination>> iterator = destinations.iterator(tx); iterator.hasNext();) {
                            Entry<String, StoredDestination> entry = iterator.next();
                            rc.add(convert(entry.getKey()));
                        }
                    }
                });
            }
            return rc;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public long getLastMessageBrokerSequenceId() throws IOException {
        return 0;
    }

    @Override
    public long size() {
        if ( !started.get() ) {
            return 0;
        }
        try {
            return pageFile.getDiskSize();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void beginTransaction(ConnectionContext context) throws IOException {
        throw new IOException("Not yet implemented.");
    }
    @Override
    public void commitTransaction(ConnectionContext context) throws IOException {
        throw new IOException("Not yet implemented.");
    }
    @Override
    public void rollbackTransaction(ConnectionContext context) throws IOException {
        throw new IOException("Not yet implemented.");
    }

    @Override
    public void checkpoint(boolean sync) throws IOException {
    }

    ///////////////////////////////////////////////////////////////////
    // Internal conversion methods.
    ///////////////////////////////////////////////////////////////////



    KahaLocation convert(Location location) {
        KahaLocation rc = new KahaLocation();
        rc.setLogId(location.getDataFileId());
        rc.setOffset(location.getOffset());
        return rc;
    }

    KahaDestination convert(ActiveMQDestination dest) {
        KahaDestination rc = new KahaDestination();
        rc.setName(dest.getPhysicalName());
        switch( dest.getDestinationType() ) {
        case ActiveMQDestination.QUEUE_TYPE:
            rc.setType(DestinationType.QUEUE);
            return rc;
        case ActiveMQDestination.TOPIC_TYPE:
            rc.setType(DestinationType.TOPIC);
            return rc;
        case ActiveMQDestination.TEMP_QUEUE_TYPE:
            rc.setType(DestinationType.TEMP_QUEUE);
            return rc;
        case ActiveMQDestination.TEMP_TOPIC_TYPE:
            rc.setType(DestinationType.TEMP_TOPIC);
            return rc;
        default:
            return null;
        }
    }

    ActiveMQDestination convert(String dest) {
        int p = dest.indexOf(":");
        if( p<0 ) {
            throw new IllegalArgumentException("Not in the valid destination format");
        }
        int type = Integer.parseInt(dest.substring(0, p));
        String name = dest.substring(p+1);

        switch( KahaDestination.DestinationType.valueOf(type) ) {
        case QUEUE:
            return new ActiveMQQueue(name);
        case TOPIC:
            return new ActiveMQTopic(name);
        case TEMP_QUEUE:
            return new ActiveMQTempQueue(name);
        case TEMP_TOPIC:
            return new ActiveMQTempTopic(name);
        default:
            throw new IllegalArgumentException("Not in the valid destination format");
        }
    }

    @Override
    public long getLastProducerSequenceId(ProducerId id) {
        return -1;
    }

    @Override
    public void allowIOResumption() {
        if (pageFile != null) {
            pageFile.allowIOResumption();
        }
    }

    @Override
    public void setBrokerService(BrokerService brokerService) {
        this.brokerService = brokerService;
    }

    @Override
    public void load() throws IOException {
        if( brokerService!=null ) {
            wireFormat.setVersion(brokerService.getStoreOpenWireVersion());
        }
        super.load();
    }
    @Override
    public JobSchedulerStore createJobSchedulerStore() throws IOException, UnsupportedOperationException {
        throw new UnsupportedOperationException();
    }
}
