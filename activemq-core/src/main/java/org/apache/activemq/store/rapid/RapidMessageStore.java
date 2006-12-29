/**
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.store.rapid;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;

import org.apache.activeio.journal.active.Location;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.JournalQueueAck;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.MessageAck;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.kaha.ListContainer;
import org.apache.activemq.kaha.MapContainer;
import org.apache.activemq.kaha.StoreEntry;
import org.apache.activemq.memory.UsageListener;
import org.apache.activemq.memory.UsageManager;
import org.apache.activemq.store.MessageRecoveryListener;
import org.apache.activemq.store.MessageStore;
import org.apache.activemq.transaction.Synchronization;
import org.apache.activemq.util.LRUCache;
import org.apache.activemq.util.TransactionTemplate;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * A MessageStore that uses a Journal to store it's messages.
 * 
 * @version $Revision: 1.14 $
 */
public class RapidMessageStore implements MessageStore, UsageListener {

    private static final Log log = LogFactory.getLog(RapidMessageStore.class);

    protected final RapidPersistenceAdapter peristenceAdapter;
    protected final RapidTransactionStore transactionStore;
    protected final ListContainer messageContainer;
    protected final ActiveMQDestination destination;
    protected final TransactionTemplate transactionTemplate;
    protected final LRUCache cache;
    protected UsageManager usageManager;
    protected StoreEntry batchEntry = null;
    
    
    
    protected Location lastLocation;
    protected HashSet inFlightTxLocations = new HashSet();
    
    public RapidMessageStore(RapidPersistenceAdapter adapter, ActiveMQDestination destination, ListContainer container, int maximumCacheSize) {
        this.peristenceAdapter = adapter;
        this.transactionStore = adapter.getTransactionStore();
        this.messageContainer = container;
        this.destination = destination;
        this.transactionTemplate = new TransactionTemplate(adapter, new ConnectionContext());
        this.cache=new LRUCache(maximumCacheSize,maximumCacheSize,0.75f,false);
//      populate the cache
        StoreEntry entry=messageContainer.getFirst();
        int count = 0;
        if(entry!=null){
            do{
                RapidMessageReference msg = (RapidMessageReference)messageContainer.get(entry);
                cache.put(msg.getMessageId(),entry);
                entry = messageContainer.getNext(entry);
                count++;
            }while(entry!=null && count < maximumCacheSize);
        }
    }
    

    /**
     * Not synchronized since the Journal has better throughput if you increase
     * the number of concurrent writes that it is doing.
     */
    public synchronized void addMessage(ConnectionContext context, final Message message) throws IOException {
        
        final MessageId id = message.getMessageId();
        
        final boolean debug = log.isDebugEnabled();        
        final Location location = peristenceAdapter.writeCommand(message, message.isResponseRequired());
        final RapidMessageReference md = new RapidMessageReference(message, location);
        
        if( !context.isInTransaction() ) {
            if( debug )
                log.debug("Journalled message add for: "+id+", at: "+location);
            addMessage(md);
        } else {
            message.incrementReferenceCount();
            if( debug )
                log.debug("Journalled transacted message add for: "+id+", at: "+location);
            synchronized( this ) {
                inFlightTxLocations.add(location);
            }
            transactionStore.addMessage(this, message, location);
            context.getTransaction().addSynchronization(new Synchronization(){
                public void afterCommit() throws Exception {                    
                    if( debug )
                        log.debug("Transacted message add commit for: "+id+", at: "+location);
                    message.decrementReferenceCount();
                    synchronized( RapidMessageStore.this ) {
                        inFlightTxLocations.remove(location);
                        addMessage(md);
                    }
                }
                public void afterRollback() throws Exception {                    
                    if( debug )
                        log.debug("Transacted message add rollback for: "+id+", at: "+location);
                    message.decrementReferenceCount();
                    synchronized( RapidMessageStore.this ) {
                        inFlightTxLocations.remove(location);
                    }
                }
            });
        }
    }

    private synchronized void addMessage(final RapidMessageReference messageReference){
        StoreEntry item=messageContainer.placeLast(messageReference);
        cache.put(messageReference.getMessageId(),item);
    }
    
    static protected String toString(Location location) {
        Location l = (Location) location;
        return l.getLogFileId()+":"+l.getLogFileOffset();
    }

    static protected Location toLocation(String t) {
        String[] strings = t.split(":");
        if( strings.length!=2 )
            throw new IllegalArgumentException("Invalid location: "+t);
        return new Location(Integer.parseInt(strings[0]),Integer.parseInt(strings[1]));
    }

    public void replayAddMessage(ConnectionContext context, Message message, Location location) {
        try {
            RapidMessageReference messageReference = new RapidMessageReference(message, location);
            addMessage(messageReference);
        }
        catch (Throwable e) {
            log.warn("Could not replay add for message '" + message.getMessageId() + "'.  Message may have already been added. reason: " + e);
        }
    }

    /**
     */
    public void removeMessage(ConnectionContext context, final MessageAck ack) throws IOException {
        final boolean debug = log.isDebugEnabled();
        JournalQueueAck remove = new JournalQueueAck();
        remove.setDestination(destination);
        remove.setMessageAck(ack);
        
        final Location location = peristenceAdapter.writeCommand(remove, ack.isResponseRequired());
        if( !context.isInTransaction() ) {
            if( debug )
                log.debug("Journalled message remove for: "+ack.getLastMessageId()+", at: "+location);
            removeMessage(ack.getLastMessageId());
        } else {
            if( debug )
                log.debug("Journalled transacted message remove for: "+ack.getLastMessageId()+", at: "+location);
            synchronized( this ) {
                inFlightTxLocations.add(location);
            }
            transactionStore.removeMessage(this, ack, location);
            context.getTransaction().addSynchronization(new Synchronization(){
                public void afterCommit() throws Exception {                    
                    if( debug )
                        log.debug("Transacted message remove commit for: "+ack.getLastMessageId()+", at: "+location);
                    synchronized( RapidMessageStore.this ) {
                        inFlightTxLocations.remove(location);
                        removeMessage(ack.getLastMessageId());
                    }
                }
                public void afterRollback() throws Exception {                    
                    if( debug )
                        log.debug("Transacted message remove rollback for: "+ack.getLastMessageId()+", at: "+location);
                    synchronized( RapidMessageStore.this ) {
                        inFlightTxLocations.remove(location);
                    }
                }
            });

        }
    }
    
        
    public synchronized void removeMessage(MessageId msgId) throws IOException{
        StoreEntry entry=(StoreEntry)cache.remove(msgId);
        if(entry!=null){
            entry = messageContainer.refresh(entry);
            messageContainer.remove(entry);
        }else{
            for (entry = messageContainer.getFirst();entry != null; entry = messageContainer.getNext(entry)) {
                RapidMessageReference msg=(RapidMessageReference)messageContainer.get(entry);
                if(msg.getMessageId().equals(msgId)){
                    messageContainer.remove(entry);
                    break;
                }
            }
        }
        if (messageContainer.isEmpty()) {
            resetBatching();
        }
    }
    
    public void replayRemoveMessage(ConnectionContext context, MessageAck ack) {
        try {
            MessageId id = ack.getLastMessageId();
           removeMessage(id);
        }
        catch (Throwable e) {
            log.warn("Could not replay acknowledge for message '" + ack.getLastMessageId() + "'.  Message may have already been acknowledged. reason: " + e);
        }
    }

   
    public synchronized Message getMessage(MessageId identity) throws IOException{
        RapidMessageReference result=null;
        StoreEntry entry=(StoreEntry)cache.get(identity);
        if(entry!=null){
            entry = messageContainer.refresh(entry);
            result = (RapidMessageReference)messageContainer.get(entry);
        }else{    
            for (entry = messageContainer.getFirst();entry != null; entry = messageContainer.getNext(entry)) {
                RapidMessageReference msg=(RapidMessageReference)messageContainer.get(entry);
                if(msg.getMessageId().equals(identity)){
                    result=msg;
                    cache.put(identity,entry);
                    break;
                }
            }
        }
        if (result == null )
            return null;
        return (Message) peristenceAdapter.readCommand(result.getLocation());
    }

    /**
     * Replays the checkpointStore first as those messages are the oldest ones,
     * then messages are replayed from the transaction log and then the cache is
     * updated.
     * 
     * @param listener
     * @throws Exception 
     */
        
    public synchronized void recover(MessageRecoveryListener listener) throws Exception{
        for(Iterator iter=messageContainer.iterator();iter.hasNext();){
            RapidMessageReference messageReference=(RapidMessageReference) iter.next();
            Message m = (Message) peristenceAdapter.readCommand(messageReference.getLocation());
            listener.recoverMessage(m);
        }
        listener.finished();
    }

    public void start() {
        if( this.usageManager != null )
            this.usageManager.addUsageListener(this);
    }

    public void stop() {
        if( this.usageManager != null )
            this.usageManager.removeUsageListener(this);
    }

    /**
     * @see org.apache.activemq.store.MessageStore#removeAllMessages(ConnectionContext)
     */
    public synchronized void removeAllMessages(ConnectionContext context) throws IOException {
        messageContainer.clear();
        cache.clear();
    }
    
    public ActiveMQDestination getDestination() {
        return destination;
    }

    public void addMessageReference(ConnectionContext context, MessageId messageId, long expirationTime, String messageRef) throws IOException {
        throw new IOException("Does not support message references.");
    }

    public String getMessageReference(MessageId identity) throws IOException {
        throw new IOException("Does not support message references.");
    }


    public void setUsageManager(UsageManager usageManager) {
        this.usageManager = usageManager;
    }

    /**
     * @return
     * @throws IOException
     */
    public Location checkpoint() throws IOException {

        ArrayList cpActiveJournalLocations;

        // swap out the message hash maps..
        synchronized (this) {
            cpActiveJournalLocations=new ArrayList(inFlightTxLocations);
        }
        
        if( cpActiveJournalLocations.size() > 0 ) {
            Collections.sort(cpActiveJournalLocations);
            return (Location) cpActiveJournalLocations.get(0);
        } else {
            return lastLocation;
        }
    }


   
    public int getMessageCount(){
        return messageContainer.size();
    }

    public synchronized void recoverNextMessages(int maxReturned,MessageRecoveryListener listener) throws Exception{
        StoreEntry entry=batchEntry;
        if(entry==null){
            entry=messageContainer.getFirst();
        }else{
            entry=messageContainer.refresh(entry);
            entry=messageContainer.getNext(entry);
        }
        if(entry!=null){
            int count=0;
            do{
                RapidMessageReference messageReference=(RapidMessageReference)messageContainer.get(entry);
                Message msg=(Message)peristenceAdapter.readCommand(messageReference.getLocation());
                if(msg!=null){
                    Message message=(Message)msg;
                    listener.recoverMessage(message);
                    count++;
                }
                batchEntry=entry;
                entry=messageContainer.getNext(entry);
            }while(entry!=null&&count<maxReturned&&listener.hasSpace());
        }
        listener.finished();
    }

    public void resetBatching(){
        batchEntry = null;
    }
    
    /**
     * @return true if the store supports cursors
     */
    public boolean isSupportForCursors() {
        return true;
    }
    
    public synchronized void onMemoryUseChanged(UsageManager memoryManager,int oldPercentUsage,int newPercentUsage){
        if (newPercentUsage == 100) {
            cache.clear();
        }
        
    }

}