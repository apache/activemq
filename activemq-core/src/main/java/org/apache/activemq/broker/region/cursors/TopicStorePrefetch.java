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

package org.apache.activemq.broker.region.cursors;

import java.io.IOException;
import java.util.LinkedList;
import org.apache.activemq.broker.region.Destination;
import org.apache.activemq.broker.region.MessageReference;
import org.apache.activemq.broker.region.Topic;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.store.MessageRecoveryListener;
import org.apache.activemq.store.TopicMessageStore;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * perist pendingCount messages pendingCount message (messages awaiting disptach to a consumer) cursor
 * 
 * @version $Revision$
 */
class TopicStorePrefetch extends AbstractPendingMessageCursor implements MessageRecoveryListener{

    static private final Log log=LogFactory.getLog(TopicStorePrefetch.class);
    private TopicMessageStore store;
    private final LinkedList<Message> batchList=new LinkedList<Message>();
    private String clientId;
    private String subscriberName;
    private Destination regionDestination;
    private MessageId firstMessageId;
    private MessageId lastMessageId;
    private int pendingCount;
    private boolean started;

    /**
     * @param topic
     * @param clientId
     * @param subscriberName
     */
    public TopicStorePrefetch(Topic topic,String clientId,String subscriberName){
        this.regionDestination=topic;
        this.store=(TopicMessageStore)topic.getMessageStore();
        this.clientId=clientId;
        this.subscriberName=subscriberName;
    }

    public synchronized void start(){
        if(!started){
            started=true;
            pendingCount=getStoreSize();
            try{
                fillBatch();
            }catch(Exception e){
                log.error("Failed to fill batch",e);
                throw new RuntimeException(e);
            }
        }
    }

    public synchronized void stop(){
        if(started){
            started=false;
            store.resetBatching(clientId,subscriberName);
            gc();
        }
    }

    /**
     * @return true if there are no pendingCount messages
     */
    public synchronized boolean isEmpty(){
        return pendingCount <= 0;
    }

    public synchronized int size(){
        return getPendingCount();
    }

    public synchronized void addMessageLast(MessageReference node) throws Exception{
        if(node!=null){
            if(isEmpty() && started){
                firstMessageId=node.getMessageId();
            }
            lastMessageId=node.getMessageId();
            node.decrementReferenceCount();
            pendingCount++;
        }
    }
    
    public synchronized void addMessageFirst(MessageReference node) throws Exception{
        if(node!=null){
            if(started){
                firstMessageId=node.getMessageId();
            }
            node.decrementReferenceCount();
            pendingCount++;
        }
    }
    
    public synchronized void remove(){
        pendingCount--;
    }
    
    public synchronized void remove(MessageReference node){
        pendingCount--;
    }
    
    public synchronized void clear(){
        pendingCount=0;
    }

    public synchronized boolean hasNext(){
        return !isEmpty();
    }

    public synchronized MessageReference next(){
        Message result=null;
        if(!isEmpty()){
            if(batchList.isEmpty()){
                try{
                    fillBatch();
                }catch(final Exception e){
                    log.error("Failed to fill batch",e);
                    throw new RuntimeException(e);
                }
                if(batchList.isEmpty()){
                    return null;
                }
            }
            if(!batchList.isEmpty()){
                result=batchList.removeFirst();
                if(lastMessageId!=null){
                    if(result.getMessageId().equals(lastMessageId)){
                        //pendingCount=0;
                    }
                }
                result.setRegionDestination(regionDestination);
            }
        }
        return result;
    }

    public void reset(){
    }

    // MessageRecoveryListener implementation
    public void finished(){
    }

    public synchronized void recoverMessage(Message message) throws Exception{
        message.setRegionDestination(regionDestination);
        // only increment if count is zero (could have been cached)
        if(message.getReferenceCount()==0){
            message.incrementReferenceCount();
        }
        batchList.addLast(message);
    }

    public void recoverMessageReference(MessageId messageReference) throws Exception{
        // shouldn't get called
        throw new RuntimeException("Not supported");
    }

    // implementation
    protected synchronized void fillBatch() throws Exception{
        if(!isEmpty()){
            store.recoverNextMessages(clientId,subscriberName,maxBatchSize,this);
            if(firstMessageId!=null){
                int pos=0;
                for(Message msg:batchList){
                    if(msg.getMessageId().equals(firstMessageId)){
                        firstMessageId=null;
                        break;
                    }
                    pos++;
                }
                if(pos>0){
                    for(int i=0;i<pos&&!batchList.isEmpty();i++){
                        batchList.removeFirst();
                    }
                    if(batchList.isEmpty()){
                        log.debug("Refilling batch - haven't got past first message = " + firstMessageId);
                        fillBatch();
                    }
                }
            }
        }
    }
    
    protected synchronized int getPendingCount(){
        if(pendingCount <= 0){
            pendingCount = getStoreSize();
        }
        return pendingCount;
    }
    
    protected synchronized int getStoreSize(){
        try{
            return store.getMessageCount(clientId,subscriberName);
        }catch(IOException e){
            log.error(this+" Failed to get the outstanding message count from the store",e);
            throw new RuntimeException(e);
        }
    }
    
    

    public synchronized void gc(){
        for(Message msg:batchList){
            msg.decrementReferenceCount();
        }
        batchList.clear();
    }

    public String toString(){
        return "TopicStorePrefetch"+System.identityHashCode(this)+"("+clientId+","+subscriberName+")";
    }
}
