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
package org.apache.activemq.store.quick;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map.Entry;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.JournalQueueAck;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.MessageAck;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.kaha.impl.async.Location;
import org.apache.activemq.memory.UsageManager;
import org.apache.activemq.store.MessageRecoveryListener;
import org.apache.activemq.store.MessageStore;
import org.apache.activemq.store.PersistenceAdapter;
import org.apache.activemq.store.ReferenceStore;
import org.apache.activemq.store.ReferenceStore.ReferenceData;
import org.apache.activemq.thread.Task;
import org.apache.activemq.thread.TaskRunner;
import org.apache.activemq.transaction.Synchronization;
import org.apache.activemq.util.Callback;
import org.apache.activemq.util.TransactionTemplate;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * A MessageStore that uses a Journal to store it's messages.
 * 
 * @version $Revision: 1.14 $
 */
public class QuickMessageStore implements MessageStore {

    private static final Log log = LogFactory.getLog(QuickMessageStore.class);

    protected final QuickPersistenceAdapter peristenceAdapter;
    protected final QuickTransactionStore transactionStore;
    protected final ReferenceStore referenceStore;
    protected final ActiveMQDestination destination;
    protected final TransactionTemplate transactionTemplate;

    private LinkedHashMap<MessageId, ReferenceData> messages = new LinkedHashMap<MessageId, ReferenceData>();
    private ArrayList<MessageAck> messageAcks = new ArrayList<MessageAck>();

    /** A MessageStore that we can use to retrieve messages quickly. */
    private LinkedHashMap<MessageId, ReferenceData> cpAddedMessageIds;
    
    protected Location lastLocation;
    protected Location lastWrittenLocation;
    
    protected HashSet<Location> inFlightTxLocations = new HashSet<Location>();

	protected final TaskRunner asyncWriteTask;
	protected CountDownLatch flushLatch;
	private final AtomicReference<Location> mark = new AtomicReference<Location>();

    public QuickMessageStore(QuickPersistenceAdapter adapter, ReferenceStore referenceStore, ActiveMQDestination destination) {
        this.peristenceAdapter = adapter;
        this.transactionStore = adapter.getTransactionStore();
        this.referenceStore = referenceStore;
        this.destination = destination;
        this.transactionTemplate = new TransactionTemplate(adapter, new ConnectionContext());
        
        asyncWriteTask = adapter.getTaskRunnerFactory().createTaskRunner(new Task(){
			public boolean iterate() {
				asyncWrite();
				return false;
			}}, "Checkpoint: "+destination);
    }
    
    public void setUsageManager(UsageManager usageManager) {
        referenceStore.setUsageManager(usageManager);
    }


    /**
     * Not synchronized since the Journal has better throughput if you increase
     * the number of concurrent writes that it is doing.
     */
    public void addMessage(ConnectionContext context, final Message message) throws IOException {
        
        final MessageId id = message.getMessageId();
        
        final boolean debug = log.isDebugEnabled();
        
        final Location location = peristenceAdapter.writeCommand(message, message.isResponseRequired());
        if( !context.isInTransaction() ) {
            if( debug )
                log.debug("Journalled message add for: "+id+", at: "+location);
            addMessage(message, location);
        } else {
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
                    synchronized( QuickMessageStore.this ) {
                        inFlightTxLocations.remove(location);
                        addMessage(message, location);
                    }
                }
                public void afterRollback() throws Exception {                    
                    if( debug )
                        log.debug("Transacted message add rollback for: "+id+", at: "+location);
                    synchronized( QuickMessageStore.this ) {
                        inFlightTxLocations.remove(location);
                    }
                }
            });
        }
    }

    private void addMessage(final Message message, final Location location) throws InterruptedIOException {
        ReferenceData data = new ReferenceData();
    	data.setExpiration(message.getExpiration());
    	data.setFileId(location.getDataFileId());
    	data.setOffset(location.getOffset());            
        synchronized (this) {
            lastLocation = location;
            messages.put(message.getMessageId(), data);
        }
        try {
			asyncWriteTask.wakeup();
		} catch (InterruptedException e) {
			throw new InterruptedIOException();
		}
    }
    
    public void replayAddMessage(ConnectionContext context, Message message, Location location) {
    	MessageId id = message.getMessageId();
        try {
            // Only add the message if it has not already been added.
            ReferenceData data = referenceStore.getMessageReference(id);
            if( data==null ) {
            	data = new ReferenceData();
            	data.setExpiration(message.getExpiration());
            	data.setFileId(location.getDataFileId());
            	data.setOffset(location.getOffset());
                referenceStore.addMessageReference(context, id, data);
            }
        }
        catch (Throwable e) {
            log.warn("Could not replay add for message '" + id + "'.  Message may have already been added. reason: " + e,e);
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
                    synchronized( QuickMessageStore.this ) {
                        inFlightTxLocations.remove(location);
                        removeMessage(ack, location);
                    }
                }
                public void afterRollback() throws Exception {                    
                    if( debug )
                        log.debug("Transacted message remove rollback for: "+ack.getLastMessageId()+", at: "+location);
                    synchronized( QuickMessageStore.this ) {
                        inFlightTxLocations.remove(location);
                    }
                }
            });

        }
    }
    
    private void removeMessage(final MessageAck ack, final Location location) throws InterruptedIOException {
    	ReferenceData data;
    	synchronized (this) {
            lastLocation = location;
            MessageId id = ack.getLastMessageId();
            data = messages.remove(id);
            if (data == null) {
                messageAcks.add(ack);
            }
        }
    	
        if (data == null) {
            try {
    			asyncWriteTask.wakeup();
    		} catch (InterruptedException e) {
    			throw new InterruptedIOException();
    		}
        }
    }
    
    public void replayRemoveMessage(ConnectionContext context, MessageAck messageAck) {
        try {
            // Only remove the message if it has not already been removed.
            ReferenceData t = referenceStore.getMessageReference(messageAck.getLastMessageId());
            if( t!=null ) {
                referenceStore.removeMessage(context, messageAck);
            }
        }
        catch (Throwable e) {
            log.warn("Could not replay acknowledge for message '" + messageAck.getLastMessageId() + "'.  Message may have already been acknowledged. reason: " + e);
        }
    }
    
    /**
     * Waits till the lastest data has landed on the referenceStore
     * @throws InterruptedIOException 
     */
    public void flush() throws InterruptedIOException {
    	log.debug("flush");
    	CountDownLatch countDown;
    	synchronized(this) {
    		if( lastWrittenLocation == lastLocation ) {
    			return;
    		}
    		if( flushLatch== null ) {
    			flushLatch = new CountDownLatch(1);
    		}
    		countDown = flushLatch;
    	}
    	try {
        	asyncWriteTask.wakeup();
	    	countDown.await();
		} catch (InterruptedException e) {
			throw new InterruptedIOException();
		}
	}

    /**
     * @return
     * @throws IOException
     */
    private void asyncWrite() {
        try {
        	CountDownLatch countDown;
        	synchronized(this) {
        		countDown = flushLatch;
        		flushLatch = null;
        	}
			
        	mark.set(doAsyncWrite());
			
			if ( countDown != null ) {
				countDown.countDown();
			}
		} catch (IOException e) {
			log.error("Checkpoint failed: "+e, e);
		}
    }
    
    /**
     * @return
     * @throws IOException
     */
    protected Location doAsyncWrite() throws IOException {

    	final ArrayList<MessageAck> cpRemovedMessageLocations;
        final ArrayList<Location> cpActiveJournalLocations;
        final int maxCheckpointMessageAddSize = peristenceAdapter.getMaxCheckpointMessageAddSize();
        final Location lastLocation;
        
        // swap out the message hash maps..
        synchronized (this) {
            cpAddedMessageIds = this.messages;
            cpRemovedMessageLocations = this.messageAcks;
            cpActiveJournalLocations=new ArrayList<Location>(inFlightTxLocations);            
            this.messages = new LinkedHashMap<MessageId, ReferenceData>();
            this.messageAcks = new ArrayList<MessageAck>();      
            lastLocation = this.lastLocation;
        }

        if( log.isDebugEnabled() )
        	log.debug("Doing batch update... adding: "+cpAddedMessageIds.size()+" removing: "+cpRemovedMessageLocations.size()+" ");
        
        transactionTemplate.run(new Callback() {
            public void execute() throws Exception {

                int size = 0;
                
                PersistenceAdapter persitanceAdapter = transactionTemplate.getPersistenceAdapter();
                ConnectionContext context = transactionTemplate.getContext();
                
                // Checkpoint the added messages.
                Iterator<Entry<MessageId, ReferenceData>> iterator = cpAddedMessageIds.entrySet().iterator();
                while (iterator.hasNext()) {
                	Entry<MessageId, ReferenceData> entry = iterator.next();
                    try {
                    	referenceStore.addMessageReference(context, entry.getKey(), entry.getValue() );
                    } catch (Throwable e) {
                        log.warn("Message could not be added to long term store: " + e.getMessage(), e);
                    }
                    
                    size ++;                    
                    
                    // Commit the batch if it's getting too big
                    if( size >= maxCheckpointMessageAddSize ) {
                        persitanceAdapter.commitTransaction(context);
                        persitanceAdapter.beginTransaction(context);
                        size=0;
                    }                    
                }

                persitanceAdapter.commitTransaction(context);
                persitanceAdapter.beginTransaction(context);

                // Checkpoint the removed messages.
                for (MessageAck ack : cpRemovedMessageLocations) {
                    try {
                        referenceStore.removeMessage(transactionTemplate.getContext(), ack);
                    } catch (Throwable e) {
                        log.debug("Message could not be removed from long term store: " + e.getMessage(), e);
                    }
                }
                
            }

        });
        
        log.debug("Batch update done.");

        synchronized (this) {
            cpAddedMessageIds = null;
            lastWrittenLocation = lastLocation;
        }
        
        if( cpActiveJournalLocations.size() > 0 ) {
            Collections.sort(cpActiveJournalLocations);
            return cpActiveJournalLocations.get(0);
        } else {
            return lastLocation;
        }
    }

    /**
     * 
     */
    public Message getMessage(MessageId identity) throws IOException {

        ReferenceData data=null;
        
        synchronized (this) {
            // Is it still in flight???
        	data = messages.get(identity);
            if( data==null && cpAddedMessageIds!=null ) {
            	data = cpAddedMessageIds.get(identity);
            }
        }
        
        if( data==null ) {
        	data = referenceStore.getMessageReference(identity);
        }
        
        if( data==null ) {
        	return null;
        }
        
        Message answer = null;
        if (answer != null ) {
            return answer;
        }
        
    	Location location = new Location();
    	location.setDataFileId(data.getFileId());
    	location.setOffset(data.getOffset());
    	return (Message) peristenceAdapter.readCommand(location);
    }

    /**
     * Replays the referenceStore first as those messages are the oldest ones,
     * then messages are replayed from the transaction log and then the cache is
     * updated.
     * 
     * @param listener
     * @throws Exception 
     */
    public void recover(final MessageRecoveryListener listener) throws Exception {
        flush();
        referenceStore.recover(new RecoveryListenerAdapter(this, listener));
    }

    public void start() throws Exception {
        referenceStore.start();
    }

    public void stop() throws Exception {
        asyncWriteTask.shutdown();
        referenceStore.stop();
    }

    /**
     * @return Returns the longTermStore.
     */
    public ReferenceStore getReferenceStore() {
        return referenceStore;
    }

    /**
     * @see org.apache.activemq.store.MessageStore#removeAllMessages(ConnectionContext)
     */
    public void removeAllMessages(ConnectionContext context) throws IOException {
        flush();
        referenceStore.removeAllMessages(context);
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

    /**
     * @return
     * @throws IOException 
     * @see org.apache.activemq.store.MessageStore#getMessageCount()
     */
    public int getMessageCount() throws IOException{
        flush();
        return referenceStore.getMessageCount();
    }

	public void recoverNextMessages(int maxReturned,MessageRecoveryListener listener) throws Exception{
        flush();
        referenceStore.recoverNextMessages(maxReturned,new RecoveryListenerAdapter(this, listener));
        
    }

    
    public void resetBatching(){
        referenceStore.resetBatching();
        
    }

	public Location getMark() {
		return mark.get();
	}

}