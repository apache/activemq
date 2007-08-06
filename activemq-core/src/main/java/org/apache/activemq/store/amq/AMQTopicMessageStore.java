/**
 * 
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package org.apache.activemq.store.amq;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.HashMap;
import java.util.Iterator;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.command.JournalTopicAck;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.command.SubscriptionInfo;
import org.apache.activemq.kaha.impl.async.Location;
import org.apache.activemq.store.MessageRecoveryListener;
import org.apache.activemq.store.TopicMessageStore;
import org.apache.activemq.store.TopicReferenceStore;
import org.apache.activemq.transaction.Synchronization;
import org.apache.activemq.util.Callback;
import org.apache.activemq.util.SubscriptionKey;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * A MessageStore that uses a Journal to store it's messages.
 * 
 * @version $Revision: 1.13 $
 */
public class AMQTopicMessageStore extends AMQMessageStore implements TopicMessageStore{

    private static final Log log=LogFactory.getLog(AMQTopicMessageStore.class);
    private TopicReferenceStore topicReferenceStore;
    private HashMap<SubscriptionKey,MessageId> ackedLastAckLocations=new HashMap<SubscriptionKey,MessageId>();

    public AMQTopicMessageStore(AMQPersistenceAdapter adapter,TopicReferenceStore topicReferenceStore,
            ActiveMQTopic destinationName){
        super(adapter,topicReferenceStore,destinationName);
        this.topicReferenceStore=topicReferenceStore;
    }

    public void recoverSubscription(String clientId,String subscriptionName,MessageRecoveryListener listener)
            throws Exception{
        flush();
        topicReferenceStore.recoverSubscription(clientId,subscriptionName,new RecoveryListenerAdapter(this,listener));
    }

    public void recoverNextMessages(String clientId,String subscriptionName,int maxReturned,
            final MessageRecoveryListener listener) throws Exception{
        RecoveryListenerAdapter recoveryListener=new RecoveryListenerAdapter(this,listener);
        topicReferenceStore.recoverNextMessages(clientId,subscriptionName,maxReturned,recoveryListener);
        if(recoveryListener.size()==0){
            flush();
            topicReferenceStore.recoverNextMessages(clientId,subscriptionName,maxReturned,recoveryListener);
        }
    }

    public SubscriptionInfo lookupSubscription(String clientId,String subscriptionName) throws IOException{
        return topicReferenceStore.lookupSubscription(clientId,subscriptionName);
    }

    public void addSubsciption(SubscriptionInfo subscriptionInfo,boolean retroactive)
            throws IOException{
        topicReferenceStore.addSubsciption(subscriptionInfo,retroactive);
    }

    /**
     */
    public void acknowledge(ConnectionContext context,String clientId,String subscriptionName,final MessageId messageId)
            throws IOException{
        final boolean debug=log.isDebugEnabled();
        JournalTopicAck ack=new JournalTopicAck();
        ack.setDestination(destination);
        ack.setMessageId(messageId);
        ack.setMessageSequenceId(messageId.getBrokerSequenceId());
        ack.setSubscritionName(subscriptionName);
        ack.setClientId(clientId);
        ack.setTransactionId(context.getTransaction()!=null?context.getTransaction().getTransactionId():null);
        final Location location=peristenceAdapter.writeCommand(ack,false);
        final SubscriptionKey key=new SubscriptionKey(clientId,subscriptionName);
        if(!context.isInTransaction()){
            if(debug)
                log.debug("Journalled acknowledge for: "+messageId+", at: "+location);
            acknowledge(messageId,location,key);
        }else{
            if(debug)
                log.debug("Journalled transacted acknowledge for: "+messageId+", at: "+location);
            synchronized(this){
                inFlightTxLocations.add(location);
            }
            transactionStore.acknowledge(this,ack,location);
            context.getTransaction().addSynchronization(new Synchronization(){

                public void afterCommit() throws Exception{
                    if(debug)
                        log.debug("Transacted acknowledge commit for: "+messageId+", at: "+location);
                    synchronized(AMQTopicMessageStore.this){
                        inFlightTxLocations.remove(location);
                        acknowledge(messageId,location,key);
                    }
                }

                public void afterRollback() throws Exception{
                    if(debug)
                        log.debug("Transacted acknowledge rollback for: "+messageId+", at: "+location);
                    synchronized(AMQTopicMessageStore.this){
                        inFlightTxLocations.remove(location);
                    }
                }
            });
        }
    }

    public boolean replayAcknowledge(ConnectionContext context,String clientId,String subscritionName,
            MessageId messageId){
        try{
            SubscriptionInfo sub=topicReferenceStore.lookupSubscription(clientId,subscritionName);
            if(sub!=null){
                topicReferenceStore.acknowledge(context,clientId,subscritionName,messageId);
                return true;
            }
        }catch(Throwable e){
            log.debug("Could not replay acknowledge for message '"+messageId
                    +"'.  Message may have already been acknowledged. reason: "+e);
        }
        return false;
    }

    /**
     * @param messageId
     * @param location
     * @param key
     * @throws InterruptedIOException
     */
    protected void acknowledge(MessageId messageId,Location location,SubscriptionKey key) throws InterruptedIOException{
        synchronized(this){
            lastLocation=location;
            ackedLastAckLocations.put(key,messageId);
        }
        try{
            asyncWriteTask.wakeup();
        }catch(InterruptedException e){
            throw new InterruptedIOException();
        }
    }

    @Override protected Location doAsyncWrite() throws IOException{
        final HashMap<SubscriptionKey,MessageId> cpAckedLastAckLocations;
        // swap out the hash maps..
        synchronized(this){
            cpAckedLastAckLocations=this.ackedLastAckLocations;
            this.ackedLastAckLocations=new HashMap<SubscriptionKey,MessageId>();
        }
        Location location=super.doAsyncWrite();

        if (cpAckedLastAckLocations != null) {
            transactionTemplate.run(new Callback() {
                public void execute() throws Exception {
                    // Checkpoint the acknowledged messages.
                    Iterator<SubscriptionKey> iterator = cpAckedLastAckLocations.keySet().iterator();
                    while (iterator.hasNext()) {
                        SubscriptionKey subscriptionKey = iterator.next();
                        MessageId identity = cpAckedLastAckLocations.get(subscriptionKey);
                        topicReferenceStore.acknowledge(transactionTemplate.getContext(), subscriptionKey.clientId,
                                subscriptionKey.subscriptionName, identity);
                    }
                }
            });
        }
        return location;
    }

    /**
     * @return Returns the longTermStore.
     */
    public TopicReferenceStore getTopicReferenceStore(){
        return topicReferenceStore;
    }

    public void deleteSubscription(String clientId,String subscriptionName) throws IOException{
        topicReferenceStore.deleteSubscription(clientId,subscriptionName);
    }

    public SubscriptionInfo[] getAllSubscriptions() throws IOException{
        return topicReferenceStore.getAllSubscriptions();
    }

    public int getMessageCount(String clientId,String subscriberName) throws IOException{
        flush();
        return topicReferenceStore.getMessageCount(clientId,subscriberName);
    }

    public void resetBatching(String clientId,String subscriptionName){
        topicReferenceStore.resetBatching(clientId,subscriptionName);
    }
}