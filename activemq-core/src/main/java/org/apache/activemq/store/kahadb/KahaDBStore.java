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
import java.util.Set;
import java.util.Map.Entry;

import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTempQueue;
import org.apache.activemq.command.ActiveMQTempTopic;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.command.LocalTransactionId;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.MessageAck;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.command.SubscriptionInfo;
import org.apache.activemq.command.TransactionId;
import org.apache.activemq.command.XATransactionId;
import org.apache.activemq.openwire.OpenWireFormat;
import org.apache.activemq.protobuf.Buffer;
import org.apache.activemq.store.AbstractMessageStore;
import org.apache.activemq.store.MessageRecoveryListener;
import org.apache.activemq.store.MessageStore;
import org.apache.activemq.store.PersistenceAdapter;
import org.apache.activemq.store.TopicMessageStore;
import org.apache.activemq.store.TransactionRecoveryListener;
import org.apache.activemq.store.TransactionStore;
import org.apache.activemq.store.kahadb.data.KahaAddMessageCommand;
import org.apache.activemq.store.kahadb.data.KahaCommitCommand;
import org.apache.activemq.store.kahadb.data.KahaDestination;
import org.apache.activemq.store.kahadb.data.KahaLocalTransactionId;
import org.apache.activemq.store.kahadb.data.KahaLocation;
import org.apache.activemq.store.kahadb.data.KahaPrepareCommand;
import org.apache.activemq.store.kahadb.data.KahaRemoveDestinationCommand;
import org.apache.activemq.store.kahadb.data.KahaRemoveMessageCommand;
import org.apache.activemq.store.kahadb.data.KahaRollbackCommand;
import org.apache.activemq.store.kahadb.data.KahaSubscriptionCommand;
import org.apache.activemq.store.kahadb.data.KahaTransactionInfo;
import org.apache.activemq.store.kahadb.data.KahaXATransactionId;
import org.apache.activemq.store.kahadb.data.KahaDestination.DestinationType;
import org.apache.activemq.usage.MemoryUsage;
import org.apache.activemq.usage.SystemUsage;
import org.apache.activemq.wireformat.WireFormat;
import org.apache.kahadb.journal.Location;
import org.apache.kahadb.page.Transaction;

public class KahaDBStore extends MessageDatabase implements PersistenceAdapter {

    private WireFormat wireFormat = new OpenWireFormat();

    public void setBrokerName(String brokerName) {
    }
    public void setUsageManager(SystemUsage usageManager) {
    }

    public TransactionStore createTransactionStore() throws IOException {
        return new TransactionStore(){
            
            public void commit(TransactionId txid, boolean wasPrepared) throws IOException {
                store(new KahaCommitCommand().setTransactionInfo(createTransactionInfo(txid)), true);
            }
            public void prepare(TransactionId txid) throws IOException {
                store(new KahaPrepareCommand().setTransactionInfo(createTransactionInfo(txid)), true);
            }
            public void rollback(TransactionId txid) throws IOException {
                store(new KahaRollbackCommand().setTransactionInfo(createTransactionInfo(txid)), false);
            }
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
            public void start() throws Exception {
            }
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

        public ActiveMQDestination getDestination() {
            return destination;
        }

        public void addMessage(ConnectionContext context, Message message) throws IOException {
            KahaAddMessageCommand command = new KahaAddMessageCommand();
            command.setDestination(dest);
            command.setMessageId(message.getMessageId().toString());
            command.setTransactionInfo( createTransactionInfo(message.getTransactionId()) );

            org.apache.activemq.util.ByteSequence packet = wireFormat.marshal(message);
            command.setMessage(new Buffer(packet.getData(), packet.getOffset(), packet.getLength()));

            store(command, isEnableJournalDiskSyncs() && message.isResponseRequired());
            
        }
        
        public void removeMessage(ConnectionContext context, MessageAck ack) throws IOException {
            KahaRemoveMessageCommand command = new KahaRemoveMessageCommand();
            command.setDestination(dest);
            command.setMessageId(ack.getLastMessageId().toString());
            command.setTransactionInfo(createTransactionInfo(ack.getTransactionId()) );
            store(command, isEnableJournalDiskSyncs() && ack.isResponseRequired());
        }

        public void removeAllMessages(ConnectionContext context) throws IOException {
            KahaRemoveDestinationCommand command = new KahaRemoveDestinationCommand();
            command.setDestination(dest);
            store(command, true);
        }

        public Message getMessage(MessageId identity) throws IOException {
            final String key = identity.toString();
            
            // Hopefully one day the page file supports concurrent read operations... but for now we must
            // externally synchronize...
            Location location;
            synchronized(indexMutex) {
                location = pageFile.tx().execute(new Transaction.CallableClosure<Location, IOException>(){
                    public Location execute(Transaction tx) throws IOException {
                        StoredDestination sd = getStoredDestination(dest, tx);
                        Long sequence = sd.messageIdIndex.get(tx, key);
                        if( sequence ==null ) {
                            return null;
                        }
                        return sd.orderIndex.get(tx, sequence).location;
                    }
                });
            }
            if( location == null ) {
                return null;
            }
            
            return loadMessage(location);
        }
        
        public int getMessageCount() throws IOException {
            synchronized(indexMutex) {
                return pageFile.tx().execute(new Transaction.CallableClosure<Integer, IOException>(){
                    public Integer execute(Transaction tx) throws IOException {
                        // Iterate through all index entries to get a count of messages in the destination.
                        StoredDestination sd = getStoredDestination(dest, tx);
                        int rc=0;
                        for (Iterator<Entry<Location, Long>> iterator = sd.locationIndex.iterator(tx); iterator.hasNext();) {
                            iterator.next();
                            rc++;
                        }
                        return rc;
                    }
                });
            }
        }

        public void recover(final MessageRecoveryListener listener) throws Exception {
            synchronized(indexMutex) {
                pageFile.tx().execute(new Transaction.Closure<Exception>(){
                    public void execute(Transaction tx) throws Exception {
                        StoredDestination sd = getStoredDestination(dest, tx);
                        for (Iterator<Entry<Long, MessageKeys>> iterator = sd.orderIndex.iterator(tx); iterator.hasNext();) {
                            Entry<Long, MessageKeys> entry = iterator.next();
                            listener.recoverMessage( loadMessage(entry.getValue().location) );
                        }
                    }
                });
            }
        }

        long cursorPos=0;
        
        public void recoverNextMessages(final int maxReturned, final MessageRecoveryListener listener) throws Exception {
            synchronized(indexMutex) {
                pageFile.tx().execute(new Transaction.Closure<Exception>(){
                    public void execute(Transaction tx) throws Exception {
                        StoredDestination sd = getStoredDestination(dest, tx);
                        Entry<Long, MessageKeys> entry=null;
                        int counter = 0;
                        for (Iterator<Entry<Long, MessageKeys>> iterator = sd.orderIndex.iterator(tx, cursorPos); iterator.hasNext();) {
                            entry = iterator.next();
                            listener.recoverMessage( loadMessage(entry.getValue().location ) );
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

        public void resetBatching() {
            cursorPos=0;
        }

        
        @Override
        public void setBatch(MessageId identity) throws IOException {
            final String key = identity.toString();
            
            // Hopefully one day the page file supports concurrent read operations... but for now we must
            // externally synchronize...
            Long location;
            synchronized(indexMutex) {
                location = pageFile.tx().execute(new Transaction.CallableClosure<Long, IOException>(){
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

        public void setMemoryUsage(MemoryUsage memoeyUSage) {
        }
        public void start() throws Exception {
        }
        public void stop() throws Exception {
        }
        
    }
        
    class KahaDBTopicMessageStore extends KahaDBMessageStore implements TopicMessageStore {
        public KahaDBTopicMessageStore(ActiveMQTopic destination) {
            super(destination);
        }
        
        public void acknowledge(ConnectionContext context, String clientId, String subscriptionName, MessageId messageId) throws IOException {
            KahaRemoveMessageCommand command = new KahaRemoveMessageCommand();
            command.setDestination(dest);
            command.setSubscriptionKey(subscriptionKey(clientId, subscriptionName));
            command.setMessageId(messageId.toString());
            // We are not passed a transaction info.. so we can't participate in a transaction.
            // Looks like a design issue with the TopicMessageStore interface.  Also we can't recover the original ack
            // to pass back to the XA recover method.
            // command.setTransactionInfo();
            store(command, false);
        }

        public void addSubsciption(SubscriptionInfo subscriptionInfo, boolean retroactive) throws IOException {
            String subscriptionKey = subscriptionKey(subscriptionInfo.getClientId(), subscriptionInfo.getSubscriptionName());
            KahaSubscriptionCommand command = new KahaSubscriptionCommand();
            command.setDestination(dest);
            command.setSubscriptionKey(subscriptionKey);
            command.setRetroactive(retroactive);
            org.apache.activemq.util.ByteSequence packet = wireFormat.marshal(subscriptionInfo);
            command.setSubscriptionInfo(new Buffer(packet.getData(), packet.getOffset(), packet.getLength()));
            store(command, isEnableJournalDiskSyncs() && true);
        }

        public void deleteSubscription(String clientId, String subscriptionName) throws IOException {
            KahaSubscriptionCommand command = new KahaSubscriptionCommand();
            command.setDestination(dest);
            command.setSubscriptionKey(subscriptionKey(clientId, subscriptionName));
            store(command, isEnableJournalDiskSyncs() && true);
        }

        public SubscriptionInfo[] getAllSubscriptions() throws IOException {
            
            final ArrayList<SubscriptionInfo> subscriptions = new ArrayList<SubscriptionInfo>();
            synchronized(indexMutex) {
                pageFile.tx().execute(new Transaction.Closure<IOException>(){
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

        public SubscriptionInfo lookupSubscription(String clientId, String subscriptionName) throws IOException {
            final String subscriptionKey = subscriptionKey(clientId, subscriptionName);
            synchronized(indexMutex) {
                return pageFile.tx().execute(new Transaction.CallableClosure<SubscriptionInfo, IOException>(){
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
       
        public int getMessageCount(String clientId, String subscriptionName) throws IOException {
            final String subscriptionKey = subscriptionKey(clientId, subscriptionName);
            synchronized(indexMutex) {
                return pageFile.tx().execute(new Transaction.CallableClosure<Integer, IOException>(){
                    public Integer execute(Transaction tx) throws IOException {
                        StoredDestination sd = getStoredDestination(dest, tx);
                        Long cursorPos = sd.subscriptionAcks.get(tx, subscriptionKey);
                        if ( cursorPos==null ) {
                            // The subscription might not exist.
                            return 0;
                        }
                        cursorPos += 1;
                        
                        int counter = 0;
                        for (Iterator<Entry<Long, MessageKeys>> iterator = sd.orderIndex.iterator(tx, cursorPos); iterator.hasNext();) {
                            iterator.next();
                            counter++;
                        }
                        return counter;
                    }
                });
            }        
        }

        public void recoverSubscription(String clientId, String subscriptionName, final MessageRecoveryListener listener) throws Exception {
            final String subscriptionKey = subscriptionKey(clientId, subscriptionName);
            synchronized(indexMutex) {
                pageFile.tx().execute(new Transaction.Closure<Exception>(){
                    public void execute(Transaction tx) throws Exception {
                        StoredDestination sd = getStoredDestination(dest, tx);
                        Long cursorPos = sd.subscriptionAcks.get(tx, subscriptionKey);
                        cursorPos += 1;
                        
                        for (Iterator<Entry<Long, MessageKeys>> iterator = sd.orderIndex.iterator(tx, cursorPos); iterator.hasNext();) {
                            Entry<Long, MessageKeys> entry = iterator.next();
                            listener.recoverMessage( loadMessage(entry.getValue().location ) );
                        }
                    }
                });
            }
        }

        public void recoverNextMessages(String clientId, String subscriptionName, final int maxReturned, final MessageRecoveryListener listener) throws Exception {
            final String subscriptionKey = subscriptionKey(clientId, subscriptionName);
            synchronized(indexMutex) {
                pageFile.tx().execute(new Transaction.Closure<Exception>(){
                    public void execute(Transaction tx) throws Exception {
                        StoredDestination sd = getStoredDestination(dest, tx);
                        Long cursorPos = sd.subscriptionCursors.get(subscriptionKey);
                        if( cursorPos == null ) {
                            cursorPos = sd.subscriptionAcks.get(tx, subscriptionKey);
                            cursorPos += 1;
                        }
                        
                        Entry<Long, MessageKeys> entry=null;
                        int counter = 0;
                        for (Iterator<Entry<Long, MessageKeys>> iterator = sd.orderIndex.iterator(tx, cursorPos); iterator.hasNext();) {
                            entry = iterator.next();
                            listener.recoverMessage( loadMessage(entry.getValue().location ) );
                            counter++;
                            if( counter >= maxReturned ) {
                                break;
                            }
                        }
                        if( entry!=null ) {
                            sd.subscriptionCursors.put(subscriptionKey, cursorPos+1);
                        }
                    }
                });
            }
        }

        public void resetBatching(String clientId, String subscriptionName) {
            try {
                final String subscriptionKey = subscriptionKey(clientId, subscriptionName);
                synchronized(indexMutex) {
                    pageFile.tx().execute(new Transaction.Closure<IOException>(){
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
    
    public MessageStore createQueueMessageStore(ActiveMQQueue destination) throws IOException {
        return new KahaDBMessageStore(destination);
    }

    public TopicMessageStore createTopicMessageStore(ActiveMQTopic destination) throws IOException {
        return new KahaDBTopicMessageStore(destination);
    }

    /**
     * Cleanup method to remove any state associated with the given destination.
     * This method does not stop the message store (it might not be cached).
     *
     * @param destination Destination to forget
     */
    public void removeQueueMessageStore(ActiveMQQueue destination) {
    }

    /**
     * Cleanup method to remove any state associated with the given destination
     * This method does not stop the message store (it might not be cached).
     *
     * @param destination Destination to forget
     */
    public void removeTopicMessageStore(ActiveMQTopic destination) {
    }

    public void deleteAllMessages() throws IOException {
        deleteAllMessages=true;
    }
    
    
    public Set<ActiveMQDestination> getDestinations() {
        try {
            final HashSet<ActiveMQDestination> rc = new HashSet<ActiveMQDestination>();
            synchronized(indexMutex) {
                pageFile.tx().execute(new Transaction.Closure<IOException>(){
                    public void execute(Transaction tx) throws IOException {
                        for (Iterator<Entry<String, StoredDestination>> iterator = metadata.destinations.iterator(tx); iterator.hasNext();) {
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
    
    public long getLastMessageBrokerSequenceId() throws IOException {
        return 0;
    }
    
    public long size() {
        if ( !started.get() ) {
            return 0;
        }
        try {
            return journal.getDiskSize() + pageFile.getDiskSize();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void beginTransaction(ConnectionContext context) throws IOException {
        throw new IOException("Not yet implemented.");
    }
    public void commitTransaction(ConnectionContext context) throws IOException {
        throw new IOException("Not yet implemented.");
    }
    public void rollbackTransaction(ConnectionContext context) throws IOException {
        throw new IOException("Not yet implemented.");
    }
    
    public void checkpoint(boolean sync) throws IOException {
        super.checkpointCleanup(false);
    }
    
    
    ///////////////////////////////////////////////////////////////////
    // Internal helper methods.
    ///////////////////////////////////////////////////////////////////

    /**
     * @param location
     * @return
     * @throws IOException
     */
    Message loadMessage(Location location) throws IOException {
        KahaAddMessageCommand addMessage = (KahaAddMessageCommand)load(location);
        Message msg = (Message)wireFormat.unmarshal( new DataInputStream(addMessage.getMessage().newInput()) );
        return msg;
    }

    ///////////////////////////////////////////////////////////////////
    // Internal conversion methods.
    ///////////////////////////////////////////////////////////////////
    
    KahaTransactionInfo createTransactionInfo(TransactionId txid) {
        if( txid ==null ) {
            return null;
        }
        KahaTransactionInfo rc = new KahaTransactionInfo();
        
        // Link it up to the previous record that was part of the transaction.
        ArrayList<Operation> tx = inflightTransactions.get(txid);
        if( tx!=null ) {
            rc.setPreviousEntry(convert(tx.get(tx.size()-1).location));
        }
        
        if( txid.isLocalTransaction() ) {
            LocalTransactionId t = (LocalTransactionId)txid;
            KahaLocalTransactionId kahaTxId = new KahaLocalTransactionId();
            kahaTxId.setConnectionId(t.getConnectionId().getValue());
            kahaTxId.setTransacitonId(t.getValue());
            rc.setLocalTransacitonId(kahaTxId);
        } else {
            XATransactionId t = (XATransactionId)txid;
            KahaXATransactionId kahaTxId = new KahaXATransactionId();
            kahaTxId.setBranchQualifier(new Buffer(t.getBranchQualifier()));
            kahaTxId.setGlobalTransactionId(new Buffer(t.getGlobalTransactionId()));
            kahaTxId.setFormatId(t.getFormatId());
            rc.setXaTransacitonId(kahaTxId);
        }
        return rc;
    }
    
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
        
}
