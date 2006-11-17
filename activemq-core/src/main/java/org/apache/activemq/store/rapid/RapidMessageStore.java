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

import org.apache.activeio.journal.RecordLocation;
import org.apache.activeio.journal.active.Location;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.JournalQueueAck;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.MessageAck;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.kaha.MapContainer;
import org.apache.activemq.memory.UsageManager;
import org.apache.activemq.store.MessageRecoveryListener;
import org.apache.activemq.store.MessageStore;
import org.apache.activemq.transaction.Synchronization;
import org.apache.activemq.util.TransactionTemplate;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * A MessageStore that uses a Journal to store it's messages.
 * 
 * @version $Revision: 1.14 $
 */
public class RapidMessageStore implements MessageStore {

    private static final Log log = LogFactory.getLog(RapidMessageStore.class);

    protected final RapidPersistenceAdapter peristenceAdapter;
    protected final RapidTransactionStore transactionStore;
    protected final MapContainer messageContainer;
    protected final ActiveMQDestination destination;
    protected final TransactionTemplate transactionTemplate;

//    private LinkedHashMap messages = new LinkedHashMap();
//    private ArrayList messageAcks = new ArrayList();

//    /** A MessageStore that we can use to retrieve messages quickly. */
//    private LinkedHashMap cpAddedMessageIds;
    
    protected RecordLocation lastLocation;
    protected HashSet inFlightTxLocations = new HashSet();
    
    public RapidMessageStore(RapidPersistenceAdapter adapter, ActiveMQDestination destination, MapContainer container) {
        this.peristenceAdapter = adapter;
        this.transactionStore = adapter.getTransactionStore();
        this.messageContainer = container;
        this.destination = destination;
        this.transactionTemplate = new TransactionTemplate(adapter, new ConnectionContext());
    }
    

    /**
     * Not synchronized since the Journal has better throughput if you increase
     * the number of concurrent writes that it is doing.
     */
    public void addMessage(ConnectionContext context, final Message message) throws IOException {
        
        final MessageId id = message.getMessageId();
        
        final boolean debug = log.isDebugEnabled();        
        final RecordLocation location = peristenceAdapter.writeCommand(message, message.isResponseRequired());
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

    private void addMessage(final RapidMessageReference messageReference) {
        synchronized (this) {
            lastLocation = messageReference.getLocation();
            MessageId id = messageReference.getMessageId();
            messageContainer.put(id.toString(), messageReference);
        }
    }
    
    static protected String toString(RecordLocation location) {
        Location l = (Location) location;
        return l.getLogFileId()+":"+l.getLogFileOffset();
    }

    static protected RecordLocation toRecordLocation(String t) {
        String[] strings = t.split(":");
        if( strings.length!=2 )
            throw new IllegalArgumentException("Invalid location: "+t);
        return new Location(Integer.parseInt(strings[0]),Integer.parseInt(strings[1]));
    }

    public void replayAddMessage(ConnectionContext context, Message message, RecordLocation location) {
        try {
            RapidMessageReference messageReference = new RapidMessageReference(message, location);
            messageContainer.put(message.getMessageId().toString(), messageReference);
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
        
        final RecordLocation location = peristenceAdapter.writeCommand(remove, ack.isResponseRequired());
        if( !context.isInTransaction() ) {
            if( debug )
                log.debug("Journalled message remove for: "+ack.getLastMessageId()+", at: "+location);
            removeMessage(ack, location);
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
                        removeMessage(ack, location);
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
    
    private void removeMessage(final MessageAck ack, final RecordLocation location) {
        synchronized (this) {
            lastLocation = location;
            MessageId id = ack.getLastMessageId();
            messageContainer.remove(id.toString());
        }
    }
    
    public void replayRemoveMessage(ConnectionContext context, MessageAck ack) {
        try {
            MessageId id = ack.getLastMessageId();
            messageContainer.remove(id.toString());
        }
        catch (Throwable e) {
            log.warn("Could not replay acknowledge for message '" + ack.getLastMessageId() + "'.  Message may have already been acknowledged. reason: " + e);
        }
    }

    /**
     * 
     */
    public Message getMessage(MessageId id) throws IOException {
        RapidMessageReference messageReference = (RapidMessageReference) messageContainer.get(id.toString());
        if (messageReference == null )
            return null;
        return (Message) peristenceAdapter.readCommand(messageReference.getLocation());
    }

    /**
     * Replays the checkpointStore first as those messages are the oldest ones,
     * then messages are replayed from the transaction log and then the cache is
     * updated.
     * 
     * @param listener
     * @throws Exception 
     */
    public void recover(final MessageRecoveryListener listener) throws Exception {
        
        for(Iterator iter=messageContainer.values().iterator();iter.hasNext();){
            RapidMessageReference messageReference=(RapidMessageReference) iter.next();
            Message m = (Message) peristenceAdapter.readCommand(messageReference.getLocation());
            listener.recoverMessage(m);
        }
        listener.finished();
        
    }

    public void start() throws Exception {
    }

    public void stop() throws Exception {
    }

    /**
     * @see org.apache.activemq.store.MessageStore#removeAllMessages(ConnectionContext)
     */
    public void removeAllMessages(ConnectionContext context) throws IOException {
        messageContainer.clear();
    }
    
    public ActiveMQDestination getDestination() {
        return destination;
    }

    public void addMessageReference(ConnectionContext context, MessageId messageId, long expirationTime, String messageRef) throws IOException {
        throw new IOException("The journal does not support message references.");
    }

    public String getMessageReference(MessageId identity) throws IOException {
        throw new IOException("The journal does not support message references.");
    }


    public void setUsageManager(UsageManager usageManager) {
    }

    /**
     * @return
     * @throws IOException
     */
    public RecordLocation checkpoint() throws IOException {

        ArrayList cpActiveJournalLocations;

        // swap out the message hash maps..
        synchronized (this) {
            cpActiveJournalLocations=new ArrayList(inFlightTxLocations);
        }
        
        if( cpActiveJournalLocations.size() > 0 ) {
            Collections.sort(cpActiveJournalLocations);
            return (RecordLocation) cpActiveJournalLocations.get(0);
        } else {
            return lastLocation;
        }
    }


   
    public int getMessageCount(){
        return 0;
    }

    public void recoverNextMessages(int maxReturned,MessageRecoveryListener listener) throws Exception{
    }

    public void resetBatching(){
    }

}