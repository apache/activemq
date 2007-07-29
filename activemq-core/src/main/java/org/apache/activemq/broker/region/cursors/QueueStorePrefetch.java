/**
 * 
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.activemq.broker.region.cursors;

import java.io.IOException;
import java.util.LinkedList;
import org.apache.activemq.broker.region.Destination;
import org.apache.activemq.broker.region.MessageReference;
import org.apache.activemq.broker.region.Queue;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.store.MessageRecoveryListener;
import org.apache.activemq.store.MessageStore;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * perist pending messages pending message (messages awaiting disptach to a
 * consumer) cursor
 * 
 * @version $Revision: 474985 $
 */
class QueueStorePrefetch extends AbstractPendingMessageCursor implements
        MessageRecoveryListener {

    static private final Log log=LogFactory.getLog(QueueStorePrefetch.class);
   
    private MessageStore store;
    private final LinkedList <Message>batchList=new LinkedList<Message>();
    private Destination regionDestination;
    private int size = 0;

    /**
     * @param topic
     * @param clientId
     * @param subscriberName
     * @throws IOException
     */
    public QueueStorePrefetch(Queue queue){
        this.regionDestination = queue;
        this.store=(MessageStore)queue.getMessageStore();
        
    }

    public void start() throws Exception{
    }

    public void stop() throws Exception{
        store.resetBatching();
        gc();
    }

    /**
     * @return true if there are no pending messages
     */
    public boolean isEmpty(){
        return size <= 0;
    }
    
    public boolean hasMessagesBufferedToDeliver() {
        return !batchList.isEmpty();
    }
    
    public synchronized int size(){
        try {
        size =  store.getMessageCount();
        }catch(IOException e) {
            log.error("Failed to get message count",e);
            throw new RuntimeException(e);
        }
        return size;
    }
    
    public synchronized void addMessageLast(MessageReference node) throws Exception{
        size++;
    }
    
    public void addMessageFirst(MessageReference node) throws Exception{
        size++;
    }
    
    public void remove(){
        size--;
    }

    public void remove(MessageReference node){
        size--;
    }


    public synchronized boolean hasNext(){
        if(batchList.isEmpty()){
            try{
                fillBatch();
            }catch(Exception e){
                log.error("Failed to fill batch",e);
                throw new RuntimeException(e);
            }
        }
        return !batchList.isEmpty();
    }

    public synchronized MessageReference next(){
        Message result = batchList.removeFirst();
        result.decrementReferenceCount();
        result.setRegionDestination(regionDestination);
        return result;
    }

    public void reset(){
    }

    // MessageRecoveryListener implementation
    public void finished(){
    }

    public boolean recoverMessage(Message message) throws Exception{
        message.setRegionDestination(regionDestination);
        message.incrementReferenceCount();
        batchList.addLast(message);
        return true;
    }

    public boolean recoverMessageReference(MessageId messageReference) throws Exception {
        Message msg=store.getMessage(messageReference);
        if(msg!=null){
            return recoverMessage(msg);
        }else{
            String err = "Failed to retrieve message for id: "+messageReference;
            log.error(err);
            throw new IOException(err);
        }
    }
    
    public void gc() {
        for (Message msg:batchList) {
            msg.decrementReferenceCount();
        }
        batchList.clear();
    }

    // implementation
    protected void fillBatch() throws Exception{
        store.recoverNextMessages(maxBatchSize,this);
    }
    
    public String toString() {
        return "QueueStorePrefetch" + System.identityHashCode(this) ;
    }
    
}
